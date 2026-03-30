"""
eia_silver.py
=============
DAG: eia_silver
---------------
Watches Snowflake RAW tables for new data and, when rows are present for the
target date, runs the silver clean/validate/deduplicate job per dataset.

HOW IT WORKS
------------
A PythonSensor per dataset queries the Snowflake raw table for rows where
_FETCHED_AT falls on the target date.  When rows are found the corresponding
silver job fires as a plain Python subprocess (no Spark):

    python silver_clean_transform.py --dataset <dataset_id> --date <date>

The script uses a Snowpark Session to read from the RAW table, clean and
deduplicate the records, then write results to a SILVER table:

    <SNOWFLAKE_DATABASE>.SILVER.SILVER_<DATASET>

TRIGGERING
----------
- Scheduled every 30 minutes so it catches data shortly after eia_ingest.
- Trigger manually with {"date": "YYYY-MM-DD"} to reprocess a specific date.

SENSOR BEHAVIOUR
----------------
- poke_interval: 60 s
- timeout:       3600 s (1 hour — alerts if ingest is broken)
- mode: "reschedule" (frees the worker slot between pokes)

ENVIRONMENT VARIABLES
---------------------
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

# ── Environment ────────────────────────────────────────────────────────────────
REGISTRY_PATH = Path("/opt/airflow/ingestion/src/dataset_registry.yml")
SILVER_SRC    = "/opt/airflow/ingestion/src"

_SNOWFLAKE_ENV_KEYS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_registry() -> list[dict]:
    with open(REGISTRY_PATH) as fh:
        return yaml.safe_load(fh).get("datasets", [])


def _resolve_date(context: dict) -> str:
    conf = context["dag_run"].conf or {}
    return conf.get("date") or context["ds"]


def _snowflake_session():
    """Open a lightweight Snowpark session for sensor queries."""
    from snowflake.snowpark import Session
    return Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "EIA"),
        "schema":    os.environ.get("SNOWFLAKE_SCHEMA",    "RAW"),
    }).create()


# ── Task callables ─────────────────────────────────────────────────────────────

def _snowflake_rows_exist(dataset_id: str, **context) -> bool:
    """
    Sensor: returns True when the Snowflake RAW table for this dataset
    contains rows fetched on the target date (_FETCHED_AT::DATE = target_date).
    """
    target_date = _resolve_date(context)
    registry    = _load_registry()
    dataset     = next((d for d in registry if d["id"] == dataset_id), None)

    if dataset is None:
        raise ValueError(f"Dataset '{dataset_id}' not found in registry.")

    table = dataset.get("snowflake_table")
    if not table:
        raise ValueError(f"Dataset '{dataset_id}' has no 'snowflake_table' configured.")

    session = _snowflake_session()
    try:
        # Check for rows fetched on the target date
        result = session.sql(
            f"SELECT COUNT(*) AS n FROM {table} "
            f"WHERE TRY_TO_DATE(_FETCHED_AT) = '{target_date}' "
            f"LIMIT 1"
        ).collect()
        count = result[0]["N"] if result else 0
        if count > 0:
            print(f"[sensor] {table}: {count:,} rows found for {target_date}")
            return True
        else:
            print(f"[sensor] {table}: no rows yet for {target_date}")
            return False
    except Exception as exc:
        # Table may not exist yet on first ever run
        print(f"[sensor] {table} query error (table may not exist yet): {exc}")
        return False
    finally:
        session.close()


def _run_silver(dataset_id: str, **context) -> None:
    """
    Run silver_clean_transform.py for one dataset as a plain Python subprocess.
    Snowpark opens its own session inside the script — no Spark, no MinIO.
    """
    target_date = _resolve_date(context)

    # Strip frequency suffix to match the --dataset choices in the script
    dataset_arg = dataset_id.replace("_hourly", "").replace("_monthly", "")

    env = {
        **os.environ,
        **{k: os.environ.get(k, "") for k in _SNOWFLAKE_ENV_KEYS},
    }

    print(f"[silver] Running {dataset_arg} for {target_date}")
    result = subprocess.run(
        ["python", "silver_clean_transform.py", "--dataset", dataset_arg, "--date", target_date],
        cwd=SILVER_SRC,
        env=env,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(
            f"Silver job failed for '{dataset_arg}' on {target_date}:\n{result.stderr}"
        )
    print(f"[silver] Done: {dataset_arg} -> Snowflake SILVER table")


# ── DAG ────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_silver",
    description=(
        "Sense new rows in Snowflake RAW tables and run silver "
        "clean/validate Snowpark jobs per dataset, writing to Snowflake SILVER tables."
    ),
    schedule_interval="*/30 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "silver", "snowflake", "sensor"],
    max_active_runs=1,
    params={
        "date": "",   # Override target date; leave blank to use today (ds)
    },
) as dag:

    datasets = _load_registry()

    for ds in datasets:
        ds_id = ds["id"]

        sense_task = PythonSensor(
            task_id=f"sense_snowflake__{ds_id}",
            python_callable=_snowflake_rows_exist,
            op_kwargs={"dataset_id": ds_id},
            poke_interval=60,
            timeout=3600,
            mode="reschedule",
            doc_md=(
                f"Wait for rows to appear in Snowflake table "
                f"`{ds.get('snowflake_table', '?')}` for the target date."
            ),
        )

        silver_task = PythonOperator(
            task_id=f"silver__{ds_id}",
            python_callable=_run_silver,
            op_kwargs={"dataset_id": ds_id},
            doc_md=(
                f"Run silver_clean_transform.py for `{ds_id}` via Snowpark: "
                f"read from `{ds.get('snowflake_table', '?')}`, clean and deduplicate, "
                f"write to `<SNOWFLAKE_DATABASE>.SILVER.SILVER_{ds_id.replace('_hourly','').upper()}`."
            ),
        )

        sense_task >> silver_task