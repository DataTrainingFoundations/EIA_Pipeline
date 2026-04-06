from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pipeline.core.settings import load_eia_settings, load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session
from pipeline.orchestration.bronze_to_gold import (
    build_gold_for_cadence,
    build_silver_for_cadence,
    finalize_cadence_state,
    ingest_cadence_datasets,
    plan_cadence_partitions,
    refresh_cadence_dimensions,
)

CADENCE_GROUP = "monthly"


def _with_session(fn, **kwargs):
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        return fn(session, settings.database, **kwargs)
    finally:
        close_session(session)


def _plan_partitions(**context):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    return _with_session(plan_cadence_partitions, cadence_group=CADENCE_GROUP, conf=conf)


def _ingest_datasets(**context):
    ti = context["ti"]
    plan_summary = ti.xcom_pull(task_ids="plan_partitions")
    settings = load_snowflake_settings()
    eia_settings = load_eia_settings()
    session = get_snowpark_session(settings)
    try:
        return ingest_cadence_datasets(session, eia_settings, settings.database, CADENCE_GROUP, plan_summary)
    finally:
        close_session(session)


def _build_silver_partitions(**context):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        refreshed_plan = plan_cadence_partitions(session, settings.database, CADENCE_GROUP, conf)
        return build_silver_for_cadence(session, settings.database, CADENCE_GROUP, refreshed_plan)
    finally:
        close_session(session)


def _build_gold_partitions(**context):
    ti = context["ti"]
    silver_summary = ti.xcom_pull(task_ids="build_silver_partitions")
    return _with_session(build_gold_for_cadence, cadence_group=CADENCE_GROUP, silver_summary=silver_summary)


def _refresh_dimensions(**context):
    return _with_session(refresh_cadence_dimensions, cadence_group=CADENCE_GROUP)


def _finalize_state(**context):
    ti = context["ti"]
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        refreshed_plan = plan_cadence_partitions(session, settings.database, CADENCE_GROUP, conf)
        ingest_summary = ti.xcom_pull(task_ids="ingest_datasets")
        silver_summary = ti.xcom_pull(task_ids="build_silver_partitions")
        gold_summary = ti.xcom_pull(task_ids="build_gold_partitions")
        return finalize_cadence_state(
            session,
            settings.database,
            CADENCE_GROUP,
            refreshed_plan,
            ingest_summary,
            silver_summary,
            gold_summary,
        )
    finally:
        close_session(session)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="eia_monthly_bronze_to_gold",
    description="Automatically ingest RAW and publish monthly Snowflake SILVER and GOLD partitions.",
    schedule_interval="0 6 1 * *",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["eia", "monthly", "bronze-to-gold", "snowflake"],
    max_active_runs=1,
    params={"start_date": "", "end_date": "", "dataset_id": "", "force_rebuild": False},
) as dag:
    plan = PythonOperator(task_id="plan_partitions", python_callable=_plan_partitions)
    ingest = PythonOperator(task_id="ingest_datasets", python_callable=_ingest_datasets)
    silver = PythonOperator(task_id="build_silver_partitions", python_callable=_build_silver_partitions)
    gold = PythonOperator(task_id="build_gold_partitions", python_callable=_build_gold_partitions)
    refresh = PythonOperator(task_id="refresh_dimensions", python_callable=_refresh_dimensions)
    finalize = PythonOperator(task_id="finalize_state", python_callable=_finalize_state)

    plan >> ingest >> silver >> gold >> refresh >> finalize
