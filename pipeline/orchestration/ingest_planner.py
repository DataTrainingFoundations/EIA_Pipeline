from __future__ import annotations

from dataclasses import dataclass

from pipeline.core.snowflake import current_partition_for_frequency, get_pipeline_state, raw_partitions_for_dataset
from pipeline.core.windowing import enumerate_partitions, month_anchor_date, shift_partition


@dataclass
class IngestDatasetPlan:
    dataset_id: str
    frequency: str
    bootstrap_ingest_complete: bool
    bootstrap_active: bool
    bootstrap_strategy: str
    bootstrap_batch_start: str
    bootstrap_batch_end: str
    remaining_partitions_estimate: int
    latest_target_partition: str
    has_more_bootstrap_work: bool
    ingest_start_date: str
    ingest_end_date: str
    current_partition: str


def _normalize_partition(value: str | None, frequency: str) -> str | None:
    if not value:
        return None
    return month_anchor_date(value) if frequency == "monthly" else value


def plan_ingest_windows(
    session,
    dataset: dict,
    database: str,
    *,
    override_start_date: str = "",
    override_end_date: str = "",
    bootstrap_mode: bool = False,
    bootstrap_batch_override: str = "",
    bootstrap_priority: str = "",
) -> IngestDatasetPlan:
    frequency = dataset["frequency"]
    dataset_id = dataset["id"]
    state = get_pipeline_state(session, database, dataset_id)
    current_partition = current_partition_for_frequency(frequency)
    steady_batch_size = int(dataset.get("max_partitions_per_run", 1) or 1)
    bootstrap_batch_size = int(
        str(bootstrap_batch_override or dataset.get("bootstrap_max_partitions_per_run", steady_batch_size) or steady_batch_size)
    )
    latest_target_partition = current_partition
    resolved_bootstrap_priority = str(bootstrap_priority or dataset.get("bootstrap_priority", "latest_first") or "latest_first")

    if override_start_date and override_end_date:
        ingest_start_date = _normalize_partition(override_start_date, frequency) or current_partition
        ingest_end_date = _normalize_partition(override_end_date, frequency) or current_partition
        bootstrap_active = False
        bootstrap_strategy = "manual_override"
        remaining_partitions_estimate = 0
        has_more_bootstrap_work = False
    elif dataset.get("bootstrap_enabled", True) and (bootstrap_mode or not bool(state.get("bootstrap_ingest_complete"))):
        bootstrap_active = True
        bootstrap_start = _normalize_partition(dataset.get("bootstrap_start_date"), frequency) or current_partition
        existing = {
            item["partition_date"]
            for item in raw_partitions_for_dataset(
                session,
                dataset["snowflake_table"],
                frequency=frequency,
                start_date=bootstrap_start,
                end_date=latest_target_partition,
            )
            if item["partition_date"]
        }
        expected = enumerate_partitions(bootstrap_start, latest_target_partition, frequency=frequency)
        missing = [partition for partition in expected if partition not in existing]
        remaining_partitions_estimate = len(missing)
        if missing:
            if resolved_bootstrap_priority == "latest_first":
                latest_missing = missing[-1]
                selected = list(reversed(missing))[:bootstrap_batch_size]
                bootstrap_strategy = "latest_first"
                ingest_end_date = latest_missing
                ingest_start_date = shift_partition(latest_missing, frequency, -(len(selected) - 1))
            else:
                selected = missing[:bootstrap_batch_size]
                bootstrap_strategy = "oldest_first"
                ingest_start_date = selected[0]
                ingest_end_date = shift_partition(selected[0], frequency, len(selected) - 1)
            has_more_bootstrap_work = len(missing) > len(selected)
        else:
            ingest_start_date = latest_target_partition
            ingest_end_date = latest_target_partition
            bootstrap_strategy = resolved_bootstrap_priority
            has_more_bootstrap_work = False
    else:
        bootstrap_active = False
        lookback = int(dataset.get("repair_lookback_partitions", 1) or 1)
        ingest_end_date = current_partition
        ingest_start_date = shift_partition(ingest_end_date, frequency, -(lookback - 1))
        bootstrap_strategy = "repair_window"
        remaining_partitions_estimate = 0
        has_more_bootstrap_work = False

    return IngestDatasetPlan(
        dataset_id=dataset_id,
        frequency=frequency,
        bootstrap_ingest_complete=bool(state.get("bootstrap_ingest_complete")),
        bootstrap_active=bootstrap_active,
        bootstrap_strategy=bootstrap_strategy,
        bootstrap_batch_start=ingest_start_date,
        bootstrap_batch_end=ingest_end_date,
        remaining_partitions_estimate=remaining_partitions_estimate,
        latest_target_partition=latest_target_partition,
        has_more_bootstrap_work=has_more_bootstrap_work,
        ingest_start_date=ingest_start_date,
        ingest_end_date=ingest_end_date,
        current_partition=current_partition,
    )
