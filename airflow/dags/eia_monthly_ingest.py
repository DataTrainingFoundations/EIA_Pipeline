from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pipeline.core.registry import iter_monthly_ingest_datasets

_SNOWFLAKE_ENV_KEYS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


def _conf_str(conf: dict, key: str, default: str = "") -> str:
    value = conf.get(key, default)
    if value is None:
        return ""
    normalized = str(value).strip()
    if normalized.lower() == "none":
        return ""
    return normalized


def _ingest_dataset(dataset_id: str, **context) -> None:
    conf = context["dag_run"].conf or {}
    env = {
        **os.environ,
        "TARGET_DATASET_ID": dataset_id,
        "BACKFILL_START_DATE": _conf_str(conf, "start_date"),
        "BACKFILL_END_DATE": _conf_str(conf, "end_date"),
        "ROLLING_HOURS": "",
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
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_monthly_ingest",
    description="Fetch monthly EIA datasets directly into Snowflake RAW tables.",
    schedule_interval="0 6 1 * *",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["eia", "monthly", "ingest", "snowflake"],
    max_active_runs=1,
    params={"start_date": "", "end_date": ""},
) as dag:
    for dataset in iter_monthly_ingest_datasets():
        PythonOperator(
            task_id=f"ingest__{dataset['id']}",
            python_callable=_ingest_dataset,
            op_kwargs={"dataset_id": dataset["id"]},
        )
