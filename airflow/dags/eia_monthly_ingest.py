from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pipeline.core.settings import load_eia_settings, load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session
from pipeline.orchestration.bootstrap_runtime import (
    chain_depth_exhausted,
    next_bootstrap_conf,
    should_continue_bootstrap,
    should_trigger_matching_transform,
    trigger_dag_if_idle,
)
from pipeline.orchestration.ingest_runtime import finalize_ingest_state, plan_ingest_cadence, run_ingest_cadence

CADENCE_GROUP = "monthly"


def _plan_ingest(**context):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        return plan_ingest_cadence(session, settings.database, CADENCE_GROUP, conf)
    finally:
        close_session(session)


def _ingest_raw(**context):
    ti = context["ti"]
    plan_summary = ti.xcom_pull(task_ids="plan_ingest")
    settings = load_snowflake_settings()
    eia_settings = load_eia_settings()
    session = get_snowpark_session(settings)
    try:
        return run_ingest_cadence(session, eia_settings, settings.database, CADENCE_GROUP, plan_summary)
    finally:
        close_session(session)


def _finalize_ingest_state(**context):
    ti = context["ti"]
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        refreshed_plan = plan_ingest_cadence(session, settings.database, CADENCE_GROUP, conf)
        ingest_summary = ti.xcom_pull(task_ids="ingest_raw")
        return finalize_ingest_state(
            session,
            settings.database,
            CADENCE_GROUP,
            refreshed_plan,
            ingest_summary,
        )
    finally:
        close_session(session)


def _continue_bootstrap_if_needed(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    conf = (dag_run.conf or {}) if dag_run else {}
    plan_summary = ti.xcom_pull(task_ids="plan_ingest") or {}
    ingest_summary = ti.xcom_pull(task_ids="ingest_raw") or {}
    finalized_summary = ti.xcom_pull(task_ids="finalize_ingest_state") or {}
    progress_made = any(int(item.get("written", 0) or 0) > 0 for item in ingest_summary.values())
    results = {"progress_made": progress_made, "self_trigger": None, "transform_trigger": None}
    if chain_depth_exhausted(conf):
        results["reason"] = "chain_depth_exhausted"
        return results
    chained_conf = next_bootstrap_conf(
        conf,
        source_dag_id=f"eia_{CADENCE_GROUP}_ingest",
        source_dag_run_id=dag_run.run_id,
        trigger_matching_transform=plan_summary.get("trigger_matching_transform", True),
    )
    if should_trigger_matching_transform(plan_summary, ingest_summary, finalized_summary):
        results["transform_trigger"] = trigger_dag_if_idle(
            f"eia_{CADENCE_GROUP}_transform",
            conf=chained_conf,
            source_run_id=dag_run.run_id,
            exclude_run_id="",
        )
    if should_continue_bootstrap(finalized_summary, progress_made=progress_made):
        results["self_trigger"] = trigger_dag_if_idle(
            f"eia_{CADENCE_GROUP}_ingest",
            conf=chained_conf,
            source_run_id=dag_run.run_id,
            exclude_run_id=dag_run.run_id,
        )
    else:
        results["reason"] = "bootstrap_backlog_drained_or_no_progress"
    return results


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="eia_monthly_ingest",
    description="Fetch monthly EIA data into Snowflake RAW without running downstream transforms.",
    schedule_interval="0 6 1 * *",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["eia", "monthly", "ingest", "snowflake"],
    max_active_runs=1,
    params={
        "start_date": "",
        "end_date": "",
        "dataset_id": "",
        "force_rebuild": False,
        "bootstrap_mode": False,
        "bootstrap_chain_depth": 0,
        "bootstrap_batch_override": "",
        "bootstrap_priority": "",
        "trigger_matching_transform": True,
        "skip_repair": False,
        "bootstrap_chain_origin": "",
        "source_dag_run_id": "",
    },
) as dag:
    plan = PythonOperator(task_id="plan_ingest", python_callable=_plan_ingest)
    ingest = PythonOperator(task_id="ingest_raw", python_callable=_ingest_raw)
    finalize = PythonOperator(task_id="finalize_ingest_state", python_callable=_finalize_ingest_state)
    continue_bootstrap = PythonOperator(
        task_id="continue_bootstrap_if_needed",
        python_callable=_continue_bootstrap_if_needed,
    )

    plan >> ingest >> finalize >> continue_bootstrap
