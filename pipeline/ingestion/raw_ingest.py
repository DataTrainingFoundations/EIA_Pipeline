from __future__ import annotations

import time
from copy import deepcopy

from pipeline.core.registry import get_dataset, iter_ingest_datasets
from pipeline.core.snowflake import write_raw_records
from pipeline.core.windowing import resolve_ingest_windows
from pipeline.ingestion.client import fetch_all_pages


def select_datasets(target_dataset_id: str = "") -> list[dict]:
    if target_dataset_id:
        return [get_dataset(target_dataset_id)]
    return list(iter_ingest_datasets())


def enrich_records(records: list[dict], dataset_id: str) -> list[dict]:
    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    enriched = []
    for record in records:
        row = dict(record)
        row["_dataset_id"] = dataset_id
        row["_fetched_at"] = fetched_at
        enriched.append(row)
    return enriched


def _chunked_datasets(dataset: dict) -> list[dict]:
    facet_key = dataset.get("facet_chunk_key", "").strip()
    facet_chunk_size = int(dataset.get("facet_chunk_size", 0) or 0)
    if not facet_key or facet_chunk_size <= 0:
        return [dataset]

    params = dataset.get("params", {})
    facets = params.get("facets", {})
    facet_values = facets.get(facet_key, [])
    if not facet_values:
        return [dataset]

    chunked: list[dict] = []
    for index in range(0, len(facet_values), facet_chunk_size):
        chunk = deepcopy(dataset)
        chunk["params"]["facets"][facet_key] = facet_values[index : index + facet_chunk_size]
        chunked.append(chunk)
    return chunked


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
    total_written = 0
    windows = resolve_ingest_windows(
        dataset,
        start_date=start_date,
        end_date=end_date,
        rolling_hours=rolling_hours,
        default_rolling_hours=default_rolling_hours,
    )
    request_datasets = _chunked_datasets(dataset)
    for window_start, window_end in windows:
        for request_dataset in request_datasets:
            records = fetch_all_pages(eia_settings, request_dataset, start=window_start, end=window_end)
            total_written += write_raw_records(
                session,
                dataset["snowflake_table"],
                enrich_records(records, dataset["id"]),
                dataset.get("schema", {}),
            )
    return total_written


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
