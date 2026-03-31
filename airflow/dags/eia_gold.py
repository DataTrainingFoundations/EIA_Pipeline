"""
eia_gold.py
===========
DAG: eia_gold
-------------

TRIGGERING
----------
- Scheduled every hour at :45 (gives silver ~15 min after eia_ingest at :15
  and eia_silver pick-up at :30).
- Trigger manually with {"date": "YYYY-MM-DD"} to reprocess any partition.

SENSOR BEHAVIOUR
----------------
- poke_interval: 60 s
- timeout:       7200 s (2 hours — allows for slow silver jobs)
- mode: "reschedule"

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
POSTGRES_HOST     = os.environ.get("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = os.environ.get("POSTGRES_PORT",     "5432")
POSTGRES_DB       = os.environ.get("POSTGRES_DB",       "platform")
POSTGRES_USER     = os.environ.get("POSTGRES_USER",     "platform")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "platform")

REGISTRY_PATH  = Path("/opt/airflow/ingestion/src/dataset_registry.yml")
GOLD_SRC    = "/opt/airflow/ingestion/src"

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
        "schema":    "SILVER",
    }).create()


# ── Task callables ─────────────────────────────────────────────────────────────

def _all_silver_tables_ready(**context) -> bool:
    target_date = _resolve_date(context)
    datasets    = _load_registry()

    session = _snowflake_session()

    try:
        all_ready = True

        for ds in datasets:
            table = f"SILVER.SILVER_{ds['id'].replace('_hourly','').replace('_monthly','').upper()}"

            result = session.sql(f"""
                SELECT COUNT(*) AS n
                FROM {table}
                WHERE TO_DATE(period_ts) = '{target_date}'
                LIMIT 1
            """).collect()

            count = result[0]["N"] if result else 0

            if count > 0:
                print(f"[sensor] ✓ {table} ({count:,} rows)")
            else:
                print(f"[sensor] ✗ {table} empty for {target_date}")
                all_ready = False

        return all_ready

    finally:
        session.close()


def _run_gold(**context):
    target_date = _resolve_date(context)

    env = {
        **os.environ,
        "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", ""),
        "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
        "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE", ""),
        "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
        "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", ""),
    }

    print(f"[gold] Running gold job for {target_date}")

    result = subprocess.run(
        ["python", "gold_serving.py", "--date", target_date],
        cwd=GOLD_SRC,
        env=env,
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Gold job failed:\n{result.stderr}")

    print(f"[gold] Done for {target_date}")


# ── DAG ────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eia_gold",
    schedule_interval="45 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "gold", "snowflake"],
) as dag:

    sense_silver = PythonSensor(
        task_id="sense_snowflake_silver",
        python_callable=_all_silver_tables_ready,
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
    )

    run_gold = PythonOperator(
        task_id="gold_aggregations",
        python_callable=_run_gold,
    )

    sense_silver >> run_gold
