"""
fetch_eia.py
------------
Fetches electricity generation and demand data from the EIA Open Data API v2
and publishes each record to the appropriate Kafka topic defined in
dataset_registry.yml.

Called by the Airflow DAG via a BashOperator.

Environment variables required:
    EIA_API_KEY   - Your EIA API key
    KAFKA_BROKER  - e.g. "kafka:9092"
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

EIA_BASE_URL = "https://api.eia.gov/v2"
REGISTRY_PATH = Path(__file__).parent / "dataset_registry.yml"
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds


def load_registry() -> list[dict]:
    with open(REGISTRY_PATH) as f:
        registry = yaml.safe_load(f)
    return registry.get("datasets", [])


def get_date_window(rolling_days: int) -> tuple[str, str]:
    """Return (start, end) ISO date strings for the rolling window."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=rolling_days)
    # EIA v2 uses "YYYY-MM-DDTHH" format for hourly data
    start_str = start.strftime("%Y-%m-%dT%H")
    end_str = now.strftime("%Y-%m-%dT%H")
    return start_str, end_str


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
                query[f"sort[{i}][column]"] = sort_item["column"]
                query[f"sort[{i}][direction]"] = sort_item["direction"]
        else:
            query[key] = value

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=query, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("response", {}).get("data", [])
            total = int(payload.get("response", {}).get("total", len(data)))
            logger.info(
                "Fetched %d records from %s (offset=%d, total=%d)",
                len(data),
                route,
                offset,
                total,
            )
            return data
        except requests.RequestException as exc:
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, route, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)

    logger.error("All retries exhausted for route: %s", route)
    return []


def fetch_all_pages(api_key: str, dataset: dict) -> list[dict]:
    """Paginate through results for a dataset within its rolling date window."""
    route = dataset["eia_route"]
    params = dataset.get("params", {})
    page_size = params.get("length", 500)
    rolling_days = dataset.get("rolling_days", 7)
    start, end = get_date_window(rolling_days)

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
        # Attach dataset metadata to each record for traceability
        for rec in records:
            rec["_dataset_id"] = dataset["id"]
            rec["_fetched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        all_records.extend(records)
        if len(records) < page_size:
            break  # Last page
        offset += page_size

    return all_records


def run() -> None:
    api_key = os.environ["EIA_API_KEY"]
    datasets = load_registry()
    producer = get_producer()

    total_published = 0

    for dataset in datasets:
        dataset_id = dataset["id"]
        topic = dataset["kafka_topic"]
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