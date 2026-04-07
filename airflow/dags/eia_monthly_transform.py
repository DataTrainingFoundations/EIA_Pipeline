from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pipeline.core.settings import load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session
from pipeline.orchestration.bootstrap_runtime import (
    chain_depth_exhausted,
    next_bootstrap_conf,
    should_continue_bootstrap,
    trigger_dag_if_idle,
)
from pipeline.orchestration.transform_runtime import (
    build_gold_for_cadence,
    build_silver_for_cadence,
    finalize_transform_state,
    plan_transform_cadence,
    refresh_cadence_dimensions,
)

CADENCE_GROUP = "monthly"


def _plan_transform(**context):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        return plan_transform_cadence(session, settings.database, CADENCE_GROUP, conf)
    finally:
        close_session(session)


def _build_silver_partitions(**context):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        refreshed_plan = plan_transform_cadence(session, settings.database, CADENCE_GROUP, conf)
        return build_silver_for_cadence(session, settings.database, CADENCE_GROUP, refreshed_plan)
    finally:
        close_session(session)


def _build_gold_partitions(**context):
    ti = context["ti"]
    silver_summary = ti.xcom_pull(task_ids="build_silver_partitions")
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        return build_gold_for_cadence(session, settings.database, CADENCE_GROUP, silver_summary)
    finally:
        close_session(session)


def _refresh_dimensions(**context):
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        return refresh_cadence_dimensions(session, settings.database, CADENCE_GROUP)
    finally:
        close_session(session)


def _finalize_transform_state(**context):
    ti = context["ti"]
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        refreshed_plan = plan_transform_cadence(session, settings.database, CADENCE_GROUP, conf)
        silver_summary = ti.xcom_pull(task_ids="build_silver_partitions")
        gold_summary = ti.xcom_pull(task_ids="build_gold_partitions")
        return finalize_transform_state(
            session,
            settings.database,
            CADENCE_GROUP,
            refreshed_plan,
            silver_summary,
            gold_summary,
        )
    finally:
        close_session(session)


def _continue_bootstrap_if_needed(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    conf = (dag_run.conf or {}) if dag_run else {}
    silver_summary = ti.xcom_pull(task_ids="build_silver_partitions") or {}
    gold_summary = ti.xcom_pull(task_ids="build_gold_partitions") or {}
    finalized_summary = ti.xcom_pull(task_ids="finalize_transform_state") or {}
    silver_results = silver_summary.get("dataset_results", {})
    progress_made = any(bool(items) for items in silver_results.values()) or bool(gold_summary.get("partitions", []))
    results = {"progress_made": progress_made, "self_trigger": None}
    if chain_depth_exhausted(conf):
        results["reason"] = "chain_depth_exhausted"
        return results
    if should_continue_bootstrap(finalized_summary, progress_made=progress_made):
        chained_conf = next_bootstrap_conf(
            conf,
            source_dag_id=f"eia_{CADENCE_GROUP}_transform",
            source_dag_run_id=dag_run.run_id,
        )
        results["self_trigger"] = trigger_dag_if_idle(
            f"eia_{CADENCE_GROUP}_transform",
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
    dag_id="eia_monthly_transform",
    description="Scan Snowflake RAW and publish pending monthly SILVER and GOLD partitions.",
    schedule_interval="30 6 1 * *",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["eia", "monthly", "transform", "snowflake"],
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
        "trigger_matching_transform": False,
        "skip_repair": False,
        "bootstrap_chain_origin": "",
        "source_dag_run_id": "",
    },
) as dag:
    plan = PythonOperator(task_id="plan_transform", python_callable=_plan_transform)
    silver = PythonOperator(task_id="build_silver_partitions", python_callable=_build_silver_partitions)
    gold = PythonOperator(task_id="build_gold_partitions", python_callable=_build_gold_partitions)
    refresh = PythonOperator(task_id="refresh_dimensions", python_callable=_refresh_dimensions)
    finalize = PythonOperator(task_id="finalize_transform_state", python_callable=_finalize_transform_state)
    continue_bootstrap = PythonOperator(
        task_id="continue_bootstrap_if_needed",
        python_callable=_continue_bootstrap_if_needed,
    )

    plan >> silver >> gold >> refresh >> finalize >> continue_bootstrap
