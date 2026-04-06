from __future__ import annotations

from dataclasses import dataclass

from pipeline.core.registry import get_gold_table_names, get_silver_table_name, normalize_dataset_id
from pipeline.core.snowflake import (
    current_partition_for_frequency,
    get_pipeline_state,
    pipeline_partitions_for_table,
    raw_partitions_for_dataset,
)
from pipeline.core.windowing import month_anchor_date, shift_partition


@dataclass
class DatasetPlan:
    dataset_id: str
    frequency: str
    bootstrap_complete: bool
    bootstrap_active: bool
    ingest_start_date: str
    ingest_end_date: str
    pending_partitions: list[str]
    stale_partitions: list[str]
    raw_latest_partition: str | None
    current_partition: str


def _normalize_partition(value: str | None, frequency: str) -> str | None:
    if not value:
        return None
    return month_anchor_date(value) if frequency == "monthly" else value


def _max_partition(partitions: list[str]) -> str | None:
    return max(partitions) if partitions else None


def _table_freshness(
    session,
    table_name: str,
    *,
    frequency: str,
    processed_column: str,
    start_date: str,
    end_date: str,
) -> dict[str, dict]:
    return pipeline_partitions_for_table(
        session,
        table_name,
        frequency=frequency,
        processed_column=processed_column,
        start_date=start_date,
        end_date=end_date,
    )


def _gold_freshness(
    session,
    database: str,
    *,
    dataset_id: str,
    frequency: str,
    cadence_group: str,
    start_date: str,
    end_date: str,
) -> dict[str, dict]:
    gold_tables = get_gold_table_names()
    if cadence_group == "hourly":
        target_name = "fact_generation_hourly" if normalize_dataset_id(dataset_id) == "electricity_generation" else "fact_demand_hourly"
        target_table = f"{database}.GOLD.{gold_tables[target_name]}"
    else:
        target_table = f"{database}.GOLD.{gold_tables['gold_electricity_operational_sales']}"
    return _table_freshness(
        session,
        target_table,
        frequency=frequency,
        processed_column="gold_processed_at",
        start_date=start_date,
        end_date=end_date,
    )


def plan_dataset_partitions(
    session,
    dataset: dict,
    database: str,
    *,
    force_rebuild: bool = False,
    override_start_date: str = "",
    override_end_date: str = "",
) -> DatasetPlan:
    dataset_id = dataset["id"]
    frequency = dataset["frequency"]
    current_partition = current_partition_for_frequency(frequency)
    state = get_pipeline_state(session, database, dataset_id)
    max_partitions_per_run = int(dataset.get("max_partitions_per_run", 1) or 1)

    if override_start_date and override_end_date:
        ingest_start_date = _normalize_partition(override_start_date, frequency) or current_partition
        ingest_end_date = _normalize_partition(override_end_date, frequency) or current_partition
        bootstrap_active = False
    elif dataset.get("bootstrap_enabled", True) and not bool(state.get("bootstrap_complete")):
        bootstrap_active = True
        ingest_start_date = (
            shift_partition(str(state["last_raw_partition"]), frequency, 1)
            if state.get("last_raw_partition")
            else _normalize_partition(dataset.get("bootstrap_start_date"), frequency) or current_partition
        )
        ingest_end_date = min(
            current_partition,
            shift_partition(ingest_start_date, frequency, max_partitions_per_run - 1),
        )
    else:
        bootstrap_active = False
        lookback = int(dataset.get("repair_lookback_partitions", 1) or 1)
        ingest_end_date = current_partition
        ingest_start_date = shift_partition(ingest_end_date, frequency, -(lookback - 1))

    if override_start_date and override_end_date:
        scan_start_date = ingest_start_date
        scan_end_date = ingest_end_date
    elif bootstrap_active:
        scan_start_date = _normalize_partition(dataset.get("bootstrap_start_date"), frequency) or ingest_start_date
        scan_end_date = current_partition
    else:
        scan_start_date = ingest_start_date
        scan_end_date = ingest_end_date

    raw_partitions = raw_partitions_for_dataset(
        session,
        dataset["snowflake_table"],
        frequency=frequency,
        start_date=scan_start_date,
        end_date=scan_end_date,
    )
    raw_map = {item["partition_date"]: item for item in raw_partitions if item["partition_date"]}
    silver_map = _table_freshness(
        session,
        f"{database}.SILVER.{get_silver_table_name(dataset_id)}",
        frequency=frequency,
        processed_column="silver_processed_at",
        start_date=scan_start_date,
        end_date=scan_end_date,
    )
    gold_map = _gold_freshness(
        session,
        database,
        dataset_id=dataset_id,
        frequency=frequency,
        cadence_group=dataset.get("cadence_group", frequency),
        start_date=scan_start_date,
        end_date=scan_end_date,
    )

    pending_missing: list[str] = []
    stale_partitions: list[str] = []
    ordered = sorted(raw_map)
    for partition_date in ordered:
        silver_entry = silver_map.get(partition_date)
        gold_entry = gold_map.get(partition_date)
        raw_entry = raw_map[partition_date]
        if force_rebuild or silver_entry is None or gold_entry is None:
            pending_missing.append(partition_date)
            continue
        raw_processed = raw_entry["processed_at"] or ""
        silver_processed = silver_entry["processed_at"] or ""
        gold_processed = gold_entry["processed_at"] or ""
        if raw_processed > silver_processed or silver_processed > gold_processed:
            stale_partitions.append(partition_date)

    backlog = pending_missing if pending_missing else sorted(stale_partitions, reverse=not bootstrap_active)
    pending_partitions = backlog[:max_partitions_per_run]
    raw_latest_partition = _max_partition(ordered)
    return DatasetPlan(
        dataset_id=dataset_id,
        frequency=frequency,
        bootstrap_complete=bool(state.get("bootstrap_complete")),
        bootstrap_active=bootstrap_active,
        ingest_start_date=ingest_start_date,
        ingest_end_date=ingest_end_date,
        pending_partitions=pending_partitions,
        stale_partitions=stale_partitions,
        raw_latest_partition=raw_latest_partition,
        current_partition=current_partition,
    )
