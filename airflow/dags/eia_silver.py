from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

from pipeline.core.registry import get_dataset, iter_scheduled_transform_datasets, normalize_dataset_id
from pipeline.core.settings import load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session, rows_exist_for_date
from pipeline.core.windowing import resolve_processing_date


def _snowflake_rows_exist(dataset_id: str, **context) -> bool:
    target_date = resolve_processing_date(context["dag_run"].conf, context["ds"])
    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        return rows_exist_for_date(session, get_dataset(dataset_id)["snowflake_table"], target_date)
    finally:
        close_session(session)


def _run_silver(dataset_id: str, **context) -> None:
    target_date = resolve_processing_date(context["dag_run"].conf, context["ds"])
    dataset_arg = normalize_dataset_id(dataset_id)
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.silver", "--dataset", dataset_arg, "--date", target_date],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_silver",
    description="Sense Snowflake RAW rows and build Snowflake SILVER tables.",
    schedule_interval="*/30 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "silver", "snowflake"],
    max_active_runs=1,
    params={"date": ""},
) as dag:
    for dataset in iter_scheduled_transform_datasets():
        sense_task = PythonSensor(
            task_id=f"sense_raw__{dataset['id']}",
            python_callable=_snowflake_rows_exist,
            op_kwargs={"dataset_id": dataset["id"]},
            poke_interval=60,
            timeout=3600,
            mode="reschedule",
        )
        silver_task = PythonOperator(
            task_id=f"silver__{dataset['id']}",
            python_callable=_run_silver,
            op_kwargs={"dataset_id": dataset["id"]},
        )
        sense_task >> silver_task
