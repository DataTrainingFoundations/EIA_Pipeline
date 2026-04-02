from __future__ import annotations

import time

from pipeline.core.registry import get_dataset, iter_datasets
from pipeline.core.snowflake import write_raw_records
from pipeline.core.windowing import resolve_ingest_window
from pipeline.ingestion.client import fetch_all_pages


def select_datasets(target_dataset_id: str = "") -> list[dict]:
    if target_dataset_id:
        return [get_dataset(target_dataset_id)]
    return [dataset for dataset in iter_datasets() if dataset.get("frequency", "hourly") == "hourly"]


def enrich_records(records: list[dict], dataset_id: str) -> list[dict]:
    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    enriched = []
    for record in records:
        row = dict(record)
        row["_dataset_id"] = dataset_id
        row["_fetched_at"] = fetched_at
        enriched.append(row)
    return enriched


def ingest_dataset(
    session,
    eia_settings,
    dataset: dict,
    *,
    start_date: str = "",
    end_date: str = "",
    rolling_hours: str = "",
    default_rolling_hours: float = 2.0,
) -> int:
    start, end = resolve_ingest_window(
        start_date=start_date,
        end_date=end_date,
        rolling_hours=rolling_hours,
        default_rolling_hours=default_rolling_hours,
        rolling_days=float(dataset.get("rolling_days", 7)),
    )
    records = fetch_all_pages(eia_settings, dataset, start=start, end=end)
    return write_raw_records(session, dataset["snowflake_table"], enrich_records(records, dataset["id"]))


def run_ingestion(
    session,
    eia_settings,
    *,
    target_dataset_id: str = "",
    start_date: str = "",
    end_date: str = "",
    rolling_hours: str = "",
    default_rolling_hours: float = 2.0,
) -> int:
    total_written = 0
    for dataset in select_datasets(target_dataset_id):
        total_written += ingest_dataset(
            session,
            eia_settings,
            dataset,
            start_date=start_date,
            end_date=end_date,
            rolling_hours=rolling_hours,
            default_rolling_hours=default_rolling_hours,
        )
    return total_written
