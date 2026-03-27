"""
eia_ingest.py
=============
DAG: eia_ingest
---------------
Universal EIA ingestion DAG.  Reads dataset_registry.yml at parse time and
creates one task per dataset entry.  Each task calls fetch_eia.py which
fetches from the EIA API and writes raw records directly to a Snowflake
table via a Snowpark Session.

Kafka and the MinIO bronze layer are no longer used.

PIPELINE POSITION
-----------------
    eia_ingest  →  (data lands in Snowflake RAW tables)
                →  eia_silver senses new rows and runs clean/transform
                →  eia_gold   builds serving tables + loads Postgres

DATE WINDOW RESOLUTION (priority order)
----------------------------------------
1. Manual trigger conf  — {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
2. rolling_hours param  — {"rolling_hours": "48"}
3. Default              — 2 hours (ROLLING_HOURS env var or hardcoded default)

SCHEDULE
--------
Every hour at :15 past.
Trigger manually with {start_date, end_date} for any backfill range.

ADDING NEW DATASETS
-------------------
Add an entry to dataset_registry.yml with a `snowflake_table` field —
no DAG changes needed.

ENVIRONMENT VARIABLES
---------------------
EIA_API_KEY
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Environment ────────────────────────────────────────────────────────────────
INGESTION_SRC = "/opt/airflow/ingestion/src"
REGISTRY_PATH = Path(f"{INGESTION_SRC}/dataset_registry.yml")

DEFAULT_ROLLING_HOURS = float(os.environ.get("ROLLING_HOURS", "2"))

# Snowflake vars forwarded to the subprocess environment
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


def _resolve_window(conf: dict) -> tuple[str, str]:
    """
    Return (start, end) EIA-format strings.
    Priority: manual conf  >  rolling_hours conf  >  default rolling hours.
    """
    if conf.get("start_date") and conf.get("end_date"):
        return f"{conf['start_date']}T00", f"{conf['end_date']}T23"

    rolling_hours = float(conf.get("rolling_hours", DEFAULT_ROLLING_HOURS))
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=rolling_hours)
    fmt   = "%Y-%m-%dT%H"
    return start.strftime(fmt), now.strftime(fmt)


# ── Task callable ──────────────────────────────────────────────────────────────

def _ingest_dataset(dataset_id: str, **context) -> None:
    """
    Resolve the date window for this run, then call fetch_eia.py for a
    single dataset.  The subprocess inherits all SNOWFLAKE_* env vars so
    publish_snowflake.py can open a Snowpark Session.
    """
    conf  = context["dag_run"].conf or {}
    start, end = _resolve_window(conf)

    context["ti"].xcom_push(
        key=f"{dataset_id}_window",
        value={"start": start, "end": end},
    )

    env = {
        **os.environ,
        "EIA_API_KEY":          os.environ.get("EIA_API_KEY", ""),
        "BACKFILL_START_DATE":  start[:10],   # YYYY-MM-DD
        "BACKFILL_END_DATE":    end[:10],
        "TARGET_DATASET_ID":    dataset_id,
        # Ensure all Snowflake vars are explicitly present
        **{k: os.environ.get(k, "") for k in _SNOWFLAKE_ENV_KEYS},
    }

    print(f"[ingest] {dataset_id}  window: {start} -> {end}")

    result = subprocess.run(
        ["python", "fetch_eia.py"],
        cwd=INGESTION_SRC,
        env=env,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(
            f"fetch_eia.py failed for dataset '{dataset_id}':\n{result.stderr}"
        )

    print(f"[ingest] {dataset_id} complete.")


# ── DAG ────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="eia_ingest",
    description=(
        "Universal EIA ingest: fetch EIA API -> write directly to Snowflake RAW tables. "
        "Runs hourly; trigger manually with {start_date, end_date} to backfill any range."
    ),
    schedule_interval="15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["eia", "ingest", "snowflake"],
    max_active_runs=1,
    params={
        "start_date":    "",   # "YYYY-MM-DD" — leave blank for rolling window
        "end_date":      "",   # "YYYY-MM-DD" — leave blank for rolling window
        "rolling_hours": "",   # e.g. "48"    — overrides the 2h default
    },
) as dag:

    datasets = _load_registry()

    for ds in datasets:
        ds_id = ds["id"]

        PythonOperator(
            task_id=f"ingest__{ds_id}",
            python_callable=_ingest_dataset,
            op_kwargs={"dataset_id": ds_id},
            doc_md=(
                f"Fetch `{ds_id}` from EIA API and write raw records to "
                f"Snowflake table `{ds.get('snowflake_table', '?')}` "
                f"via Snowpark Session."
            ),
        )
        # Tasks for different datasets run fully in parallel —
        # no dependency between them.