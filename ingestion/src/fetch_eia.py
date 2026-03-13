"""
fetch_eia.py
------------
Fetches electricity generation and demand data from the EIA Open Data API v2
and publishes each record to the appropriate Kafka topic defined in
dataset_registry.yml.

Called by the Airflow DAGs via BashOperator or PythonOperator.

DATE WINDOW MODES (checked in this priority order):
    1. Backfill mode   — set BACKFILL_START_DATE + BACKFILL_END_DATE env vars
                         e.g. BACKFILL_START_DATE=2024-01-01 BACKFILL_END_DATE=2024-01-31
    2. Override mode   — set ROLLING_DAYS_OVERRIDE env var to a float
                         e.g. ROLLING_DAYS_OVERRIDE=0.083 for ~2 hours
    3. Registry mode   — uses rolling_days from dataset_registry.yml (default: 7 days)

Environment variables required:
    EIA_API_KEY            - Your EIA API key
    KAFKA_BROKER           - e.g. "kafka:9092"

Optional:
    BACKFILL_START_DATE    - ISO date string "YYYY-MM-DD"
    BACKFILL_END_DATE      - ISO date string "YYYY-MM-DD"
    ROLLING_DAYS_OVERRIDE  - float, overrides registry rolling_days for this run
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

from publish_kafka import close_producer, get_producer, publish_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

EIA_BASE_URL  = "https://api.eia.gov/v2"
REGISTRY_PATH = Path(__file__).parent / "dataset_registry.yml"
MAX_RETRIES   = 3
RETRY_BACKOFF = 2  # seconds


def load_registry() -> list[dict]:
    with open(REGISTRY_PATH) as f:
        registry = yaml.safe_load(f)
    return registry.get("datasets", [])


def get_date_window(rolling_days: float) -> tuple[str, str]:
    """Return (start, end) EIA-format datetime strings for a rolling window."""
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=rolling_days)
    return start.strftime("%Y-%m-%dT%H"), now.strftime("%Y-%m-%dT%H")


def get_backfill_window() -> tuple[str, str] | None:
    """
    Return (start, end) from BACKFILL_START_DATE / BACKFILL_END_DATE env vars,
    or None if not set.
    """
    start_env = os.environ.get("BACKFILL_START_DATE", "").strip()
    end_env   = os.environ.get("BACKFILL_END_DATE",   "").strip()
    if start_env and end_env:
        # EIA v2 hourly format — start at midnight, end at 23:00
        start_str = f"{start_env}T00"
        end_str   = f"{end_env}T23"
        logger.info("Backfill mode: %s → %s", start_str, end_str)
        return start_str, end_str
    return None


def resolve_date_window(rolling_days: float) -> tuple[str, str]:
    """
    Resolve the date window to use for this run, checking env var overrides
    before falling back to the registry rolling_days value.
    """
    # 1. Explicit backfill window
    backfill = get_backfill_window()
    if backfill:
        return backfill

    # 2. Rolling days override (used by hourly DAG: ROLLING_DAYS_OVERRIDE=0.083)
    override = os.environ.get("ROLLING_DAYS_OVERRIDE", "").strip()
    if override:
        days = float(override)
        logger.info("Rolling days override: %.4f days (~%d hours)", days, int(days * 24))
        return get_date_window(days)

    # 3. Registry default
    return get_date_window(rolling_days)


def fetch_dataset(
    api_key: str,
    route: str,
    params: dict[str, Any],
    start: str,
    end: str,
    offset: int = 0,
) -> list[dict]:
    """
    Fetch one page of data from the EIA v2 API within the given date window.
    Returns the list of data rows, or an empty list on failure.
    """
    url = f"{EIA_BASE_URL}/{route}/data/"
    query: dict[str, Any] = {
        "api_key": api_key,
        "offset": offset,
        "start": start,
        "end": end,
    }

    # Flatten nested param structure into EIA v2 query format
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

    logger.info("Requesting URL: %s", url)
    logger.info("Query params: %s", {k: v for k, v in query.items() if k != "api_key"})

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=query, timeout=30)
            if not resp.ok:
                logger.error("EIA API error %d: %s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
            payload = resp.json()
            data  = payload.get("response", {}).get("data", [])
            total = int(payload.get("response", {}).get("total", len(data)))
            logger.info(
                "Fetched %d records from %s (offset=%d, total=%d)",
                len(data), route, offset, total,
            )
            return data
        except requests.RequestException as exc:
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, route, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)

    logger.error("All retries exhausted for route: %s", route)
    return []



def get_monthly_window(rolling_months: int) -> tuple[str, str]:
    """Return (start, end) date strings for a monthly rolling window."""
    from datetime import date
    today = datetime.now(timezone.utc).date()
    end_month   = today.replace(day=1)
    # Go back rolling_months months
    year  = end_month.year - (rolling_months // 12)
    month = end_month.month - (rolling_months % 12)
    if month <= 0:
        month += 12
        year  -= 1
    start_month = date(year, month, 1)
    return str(start_month), str(today)


def fetch_all_pages(api_key: str, dataset: dict) -> list[dict]:
    """Paginate through all results for a dataset within its resolved date window."""
    route      = dataset["eia_route"]
    params     = dataset.get("params", {})
    page_size  = params.get("length", 500)
    frequency  = dataset.get("frequency", "hourly")

    if frequency == "monthly":
        rolling_months = dataset.get("rolling_months", 24)
        backfill = get_backfill_window()
        if backfill:
            start, end = backfill
        else:
            start, end = get_monthly_window(rolling_months)
    else:
        rolling_days = dataset.get("rolling_days", 7)
        start, end = resolve_date_window(rolling_days)

    logger.info(
        "Fetching dataset '%s' from %s to %s",
        dataset["id"], start, end,
    )

    all_records: list[dict] = []
    offset = 0

    while True:
        records = fetch_dataset(api_key, route, params, start, end, offset=offset)
        if not records:
            break
        for rec in records:
            rec["_dataset_id"] = dataset["id"]
            rec["_fetched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        all_records.extend(records)
        if len(records) < page_size:
            break
        offset += page_size

    return all_records


def run() -> None:
    api_key  = os.environ["EIA_API_KEY"]
    datasets = load_registry()
    producer = get_producer()

    # SALES_ONLY=true  → only monthly datasets (used by eia_sales_pipeline DAG)
    # SALES_ONLY unset → only hourly datasets  (used by eia_hourly_pipeline DAG)
    # (backfill DAGs set the appropriate env var for their dataset type)
    sales_only = os.environ.get("SALES_ONLY", "").lower() == "true"
    target_frequency = "monthly" if sales_only else "hourly"

    filtered = [d for d in datasets if d.get("frequency", "hourly") == target_frequency]
    logger.info(
        "Running in %s mode — %d datasets selected",
        target_frequency, len(filtered),
    )

    total_published = 0

    for dataset in filtered:
        dataset_id = dataset["id"]
        topic      = dataset["kafka_topic"]
        logger.info("Processing dataset: %s -> topic: %s", dataset_id, topic)

        records = fetch_all_pages(api_key, dataset)
        if not records:
            logger.warning("No records fetched for dataset: %s", dataset_id)
            continue

        published = publish_records(
            producer=producer,
            topic=topic,
            records=records,
            key_field="period",
        )
        total_published += published
        logger.info("Dataset %s: %d records published.", dataset_id, published)

    close_producer(producer)
    logger.info("Ingestion complete. Total records published: %d", total_published)


if __name__ == "__main__":
    run()