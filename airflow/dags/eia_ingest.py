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
                          Automatically split into chunk_days-sized chunks
                          (default 30 days) and ingested sequentially per
                          dataset so no single API call spans the full range.
2. rolling_hours param  — {"rolling_hours": "48"}
3. Default              — 2 hours (ROLLING_HOURS env var or hardcoded default)

BACKFILL EXAMPLE
----------------
Trigger manually with:
    {
        "start_date":  "2024-01-01",
        "end_date":    "2024-12-31",
        "chunk_days":  30
    }
Each dataset ingests Jan, then Feb, ..., then Dec sequentially.
Datasets still run in parallel with each other.

SCHEDULE
--------
Every hour at :15 past.

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
DEFAULT_CHUNK_DAYS    = 30

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


def _build_chunks(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
    """
    Split a date range into (chunk_start, chunk_end) pairs of at most
    chunk_days each.  Both bounds are inclusive YYYY-MM-DD strings.

    Example: 2024-01-01 → 2024-12-31 with chunk_days=30 produces 13 chunks:
        [("2024-01-01", "2024-01-30"),
         ("2024-01-31", "2024-02-29"),
         ...
         ("2024-12-02", "2024-12-31")]
    """
    from datetime import date as date_type

    start = date_type.fromisoformat(start_date)
    end   = date_type.fromisoformat(end_date)
    chunks: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        chunks.append((str(cursor), str(chunk_end)))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _rolling_window() -> tuple[str, str]:
    fmt   = "%Y-%m-%dT%H"
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=DEFAULT_ROLLING_HOURS)
    return start.strftime(fmt), now.strftime(fmt)


# ── Task callable ──────────────────────────────────────────────────────────────

def _ingest_dataset(dataset_id: str, **context) -> None:
    """
    Resolve the date window(s) for this run and call fetch_eia.py once per
    chunk.  Chunks are processed sequentially so each API call stays small
    and a failure only loses one chunk, not the whole range.

    For rolling / non-backfill runs there is always exactly one chunk.
    """
    conf       = context["dag_run"].conf or {}
    start_date = conf.get("start_date", "").strip()
    end_date   = conf.get("end_date",   "").strip()

    # ── Build list of (chunk_start, chunk_end) pairs ──────────────────────────
    if start_date and end_date:
        chunk_days = int(conf.get("chunk_days", DEFAULT_CHUNK_DAYS))
        chunks     = _build_chunks(start_date, end_date, chunk_days)
        print(
            f"[ingest] {dataset_id}: backfill {start_date} -> {end_date} "
            f"in {len(chunks)} chunk(s) of up to {chunk_days} days each."
        )
    else:
        # Rolling window — single implicit chunk
        rolling_hours = float(conf.get("rolling_hours", DEFAULT_ROLLING_HOURS))
        now    = datetime.now(timezone.utc)
        start  = now - timedelta(hours=rolling_hours)
        fmt    = "%Y-%m-%d"
        chunks = [(start.strftime(fmt), now.strftime(fmt))]
        print(f"[ingest] {dataset_id}: rolling {rolling_hours}h window.")

    base_env = {
        **os.environ,
        "EIA_API_KEY":       os.environ.get("EIA_API_KEY", ""),
        "TARGET_DATASET_ID": dataset_id,
        **{k: os.environ.get(k, "") for k in _SNOWFLAKE_ENV_KEYS},
    }

    # ── Ingest each chunk sequentially ────────────────────────────────────────
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        print(f"[ingest] {dataset_id} chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end}")

        env = {
            **base_env,
            "BACKFILL_START_DATE": chunk_start,
            "BACKFILL_END_DATE":   chunk_end,
        }

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
                f"fetch_eia.py failed for '{dataset_id}' "
                f"chunk {chunk_start} -> {chunk_end}:\n{result.stderr}"
            )

    print(f"[ingest] {dataset_id}: all {len(chunks)} chunk(s) complete.")


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
        "chunk_days":    "30", # days per backfill chunk; smaller = safer but more API calls
        "rolling_hours": "",   # e.g. "48" — only used when start_date/end_date are blank
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