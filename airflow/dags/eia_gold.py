from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

from pipeline.core.registry import get_silver_table_name, iter_scheduled_transform_datasets
from pipeline.core.settings import load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session, table_has_rows_for_partition_date
from pipeline.core.windowing import resolve_processing_date
from pipeline.gold.transform import run_gold


def _all_silver_ready(**context) -> bool:
    target_date = resolve_processing_date(context["dag_run"].conf, context["ds"])
    settings = load_snowflake_settings(schema="SILVER")
    session = get_snowpark_session(settings)
    try:
        for dataset in iter_scheduled_transform_datasets():
            silver_table = f"{settings.database}.SILVER.{get_silver_table_name(dataset['id'])}"
            if not table_has_rows_for_partition_date(session, silver_table, target_date):
                return False
        return True
    finally:
        close_session(session)


def _run_gold(**context) -> None:
    target_date = resolve_processing_date(context["dag_run"].conf, context["ds"])
    settings = load_snowflake_settings(schema="GOLD")
    session = get_snowpark_session(settings)
    try:
        run_gold(session, target_date, settings.database)
    finally:
        close_session(session)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_gold",
    description="Sense Snowflake SILVER tables and build Snowflake GOLD tables.",
    schedule_interval="45 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "gold", "snowflake"],
    max_active_runs=1,
    params={"date": ""},
) as dag:
    sense = PythonSensor(
        task_id="sense_all_silver_tables",
        python_callable=_all_silver_ready,
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
    )
    build = PythonOperator(
        task_id="build_gold_tables",
        python_callable=_run_gold,
    )
    sense >> build
