from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pipeline.core.registry import iter_datasets
from pipeline.core.windowing import resolve_ingest_window

_SNOWFLAKE_ENV_KEYS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


def _ingest_dataset(dataset_id: str, **context) -> None:
    conf = context["dag_run"].conf or {}
    start, end = resolve_ingest_window(
        start_date=conf.get("start_date", ""),
        end_date=conf.get("end_date", ""),
        rolling_hours=conf.get("rolling_hours", ""),
        default_rolling_hours=float(os.environ.get("ROLLING_HOURS", "2")),
    )
    env = {
        **os.environ,
        "TARGET_DATASET_ID": dataset_id,
        "BACKFILL_START_DATE": start[:10],
        "BACKFILL_END_DATE": end[:10],
        **{key: os.environ.get(key, "") for key in _SNOWFLAKE_ENV_KEYS},
    }
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.ingestion"],
        env=env,
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
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="eia_ingest",
    description="Fetch EIA API data directly into Snowflake RAW tables.",
    schedule_interval="15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "ingest", "snowflake"],
    max_active_runs=1,
    params={"start_date": "", "end_date": "", "rolling_hours": ""},
) as dag:
    for dataset in iter_datasets():
        PythonOperator(
            task_id=f"ingest__{dataset['id']}",
            python_callable=_ingest_dataset,
            op_kwargs={"dataset_id": dataset["id"]},
        )
