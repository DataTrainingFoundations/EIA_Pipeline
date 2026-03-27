"""
fetch_eia.py
============
Fetches electricity data from the EIA Open Data API v2 and writes each
record directly to a Snowflake raw table via a Snowpark Session.

Kafka is no longer involved.  The raw records land in Snowflake immediately;
the silver Spark job reads from Snowflake instead of a MinIO bronze bucket.

DATE WINDOW MODES (checked in priority order)
---------------------------------------------
1. Backfill env vars  — BACKFILL_START_DATE + BACKFILL_END_DATE
                        e.g. BACKFILL_START_DATE=2024-01-01 BACKFILL_END_DATE=2024-01-31
2. Rolling hours      — ROLLING_HOURS env var (float hours to look back)
                        e.g. ROLLING_HOURS=48
3. Registry default   — rolling_days from dataset_registry.yml

DATASET SELECTION
-----------------
TARGET_DATASET_ID  — when set, only that one dataset is fetched.
                     The eia_ingest Airflow DAG sets this per task.
                     Unset → all datasets in the registry are fetched.

Required env vars:  EIA_API_KEY  +  all SNOWFLAKE_* vars (see publish_snowflake.py)
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

from publish_snowflake import close_session, get_session, write_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

EIA_BASE_URL  = "https://api.eia.gov/v2"
REGISTRY_PATH = Path(__file__).parent / "dataset_registry.yml"
MAX_RETRIES   = 3
RETRY_BACKOFF = 2  # seconds, exponential


# ── Registry ───────────────────────────────────────────────────────────────────

def load_registry() -> list[dict]:
    with open(REGISTRY_PATH) as fh:
        return yaml.safe_load(fh).get("datasets", [])


# ── Date window resolution ─────────────────────────────────────────────────────

def _fmt(dt: datetime) -> str:
    """EIA v2 hourly format: YYYY-MM-DDTHH"""
    return dt.strftime("%Y-%m-%dT%H")


def get_backfill_window() -> tuple[str, str] | None:
    start = os.environ.get("BACKFILL_START_DATE", "").strip()
    end   = os.environ.get("BACKFILL_END_DATE",   "").strip()
    if start and end:
        logger.info("Backfill mode: %sT00 -> %sT23", start, end)
        return f"{start}T00", f"{end}T23"
    return None


def get_rolling_window(hours: float) -> tuple[str, str]:
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    return _fmt(start), _fmt(now)


def resolve_date_window(dataset: dict) -> tuple[str, str]:
    """Return (start, end) using priority resolution for this dataset."""
    bw = get_backfill_window()
    if bw:
        return bw

    rolling_hours_env = os.environ.get("ROLLING_HOURS", "").strip()
    if rolling_hours_env:
        hours = float(rolling_hours_env)
        logger.info("ROLLING_HOURS override: %.2f h", hours)
        return get_rolling_window(hours)

    rolling_days = float(dataset.get("rolling_days", 7))
    logger.info("Registry default: %.1f days", rolling_days)
    return get_rolling_window(rolling_days * 24)


# ── EIA API ────────────────────────────────────────────────────────────────────

def fetch_page(
    api_key: str,
    route: str,
    params: dict[str, Any],
    start: str,
    end: str,
    offset: int = 0,
) -> list[dict]:
    """Fetch one page from the EIA v2 API.  Returns [] on unrecoverable failure."""
    url: str = f"{EIA_BASE_URL}/{route}/data/"
    query: dict[str, Any] = {
        "api_key": api_key,
        "offset":  offset,
        "start":   start,
        "end":     end,
    }

    for key, value in params.items():
        if key == "data":
            for col in value:
                query.setdefault("data[]", []).append(col)
        elif key == "facets":
            for facet_key, facet_vals in value.items():
                for v in facet_vals:
                    query.setdefault(f"facets[{facet_key}][]", []).append(v)
        elif key == "sort":
            for i, sort_item in enumerate(value):
                query[f"sort[{i}][column]"]    = sort_item["column"]
                query[f"sort[{i}][direction]"] = sort_item["direction"]
        else:
            query[key] = value

    logger.info("GET %s  offset=%d  window=%s->%s", url, offset, start, end)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=query, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            data    = payload.get("response", {}).get("data", [])
            total   = int(payload.get("response", {}).get("total", len(data)))
            logger.info("  -> %d records (total reported: %d)", len(data), total)
            return data
        except requests.RequestException as exc:
            logger.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)

    logger.error("All retries exhausted for route: %s", route)
    return []


def fetch_all_pages(api_key: str, dataset: dict) -> list[dict]:
    """Paginate through all results for one dataset within its date window."""
    route     = dataset["eia_route"]
    params    = dataset.get("params", {})
    page_size = int(params.get("length", 500))
    start, end = resolve_date_window(dataset)

    logger.info(
        "Dataset '%s'  table=%s  window: %s -> %s",
        dataset["id"], dataset.get("snowflake_table", "?"), start, end,
    )

    all_records: list[dict] = []
    offset = 0
    while True:
        records = fetch_page(api_key, route, params, start, end, offset)
        if not records:
            break
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for rec in records:
            rec["_dataset_id"] = dataset["id"]
            rec["_fetched_at"] = ts
        all_records.extend(records)
        if len(records) < page_size:
            break
        offset += page_size

    return all_records


# ── Main ───────────────────────────────────────────────────────────────────────

def run() -> None:
    api_key  = os.environ["EIA_API_KEY"]
    registry = load_registry()

    # ── Dataset selection ─────────────────────────────────────────────────────
    target_id = os.environ.get("TARGET_DATASET_ID", "").strip()
    if target_id:
        datasets = [d for d in registry if d["id"] == target_id]
        if not datasets:
            raise ValueError(
                f"TARGET_DATASET_ID='{target_id}' not found in registry. "
                f"Known: {[d['id'] for d in registry]}"
            )
        logger.info("Single-dataset mode: %s", target_id)
    else:
        # Manual / legacy: run all hourly datasets
        datasets = [d for d in registry if d.get("frequency", "hourly") == "hourly"]
        logger.info("All-hourly mode: %d datasets", len(datasets))

    # ── Open one shared Snowpark session for all datasets this run ────────────
    session = get_session()
    total_written = 0

    try:
        for dataset in datasets:
            snowflake_table = dataset.get("snowflake_table")
            if not snowflake_table:
                logger.warning(
                    "Dataset '%s' has no 'snowflake_table' in registry -- skipping.",
                    dataset["id"],
                )
                continue

            records = fetch_all_pages(api_key, dataset)
            if not records:
                logger.warning("No records fetched for '%s'.", dataset["id"])
                continue

            written = write_records(session, snowflake_table, records)
            total_written += written
            logger.info(
                "Dataset '%s': %d rows written -> %s",
                dataset["id"], written, snowflake_table,
            )
    finally:
        close_session(session)

    logger.info("Ingestion complete. Total rows written to Snowflake: %d", total_written)


if __name__ == "__main__":
    run()