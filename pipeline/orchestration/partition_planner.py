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
class TransformDatasetPlan:
    dataset_id: str
    frequency: str
    bootstrap_transform_complete: bool
    bootstrap_active: bool
    bootstrap_priority: str
    scan_start_date: str
    scan_end_date: str
    planned_partitions: list[str]
    pending_partitions: list[str]
    stale_partitions: list[str]
    remaining_pending_count: int
    remaining_stale_count: int
    raw_latest_partition: str | None
    current_partition: str
    has_more_bootstrap_work: bool


def _normalize_partition(value: str | None, frequency: str) -> str | None:
    if not value:
        return None
    return month_anchor_date(value) if frequency == "monthly" else value


def _max_partition(partitions: list[str]) -> str | None:
    return max(partitions) if partitions else None


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
        target_name = (
            "fact_generation_hourly"
            if normalize_dataset_id(dataset_id) == "electricity_generation"
            else "fact_demand_hourly"
        )
        target_table = f"{database}.GOLD.{gold_tables[target_name]}"
    else:
        target_table = f"{database}.GOLD.{gold_tables['gold_electricity_operational_sales']}"
    return pipeline_partitions_for_table(
        session,
        target_table,
        frequency=frequency,
        processed_column="gold_processed_at",
        start_date=start_date,
        end_date=end_date,
    )


def plan_transform_partitions(
    session,
    dataset: dict,
    database: str,
    *,
    force_rebuild: bool = False,
    override_start_date: str = "",
    override_end_date: str = "",
    bootstrap_mode: bool = False,
    bootstrap_batch_override: str = "",
    bootstrap_priority: str = "",
    skip_repair: bool = False,
) -> TransformDatasetPlan:
    dataset_id = dataset["id"]
    frequency = dataset["frequency"]
    state = get_pipeline_state(session, database, dataset_id)
    current_partition = current_partition_for_frequency(frequency)
    steady_batch_size = int(dataset.get("max_partitions_per_run", 1) or 1)
    bootstrap_batch_size = int(
        str(bootstrap_batch_override or dataset.get("bootstrap_max_partitions_per_run", steady_batch_size) or steady_batch_size)
    )
    resolved_bootstrap_priority = str(bootstrap_priority or dataset.get("bootstrap_priority", "latest_first") or "latest_first")

    if override_start_date and override_end_date:
        scan_start_date = _normalize_partition(override_start_date, frequency) or current_partition
        scan_end_date = _normalize_partition(override_end_date, frequency) or current_partition
        bootstrap_active = False
    elif bootstrap_mode or not bool(state.get("bootstrap_transform_complete")):
        bootstrap_active = True
        scan_start_date = _normalize_partition(dataset.get("bootstrap_start_date"), frequency) or current_partition
        scan_end_date = current_partition
    else:
        bootstrap_active = False
        lookback = int(dataset.get("repair_lookback_partitions", 1) or 1)
        scan_end_date = current_partition
        scan_start_date = shift_partition(scan_end_date, frequency, -(lookback - 1))

    raw_partitions = raw_partitions_for_dataset(
        session,
        dataset["snowflake_table"],
        frequency=frequency,
        start_date=scan_start_date,
        end_date=scan_end_date,
    )
    raw_map = {item["partition_date"]: item for item in raw_partitions if item["partition_date"]}
    silver_map = pipeline_partitions_for_table(
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

    if bootstrap_active:
        pending_missing = sorted(pending_missing, reverse=resolved_bootstrap_priority == "latest_first")
        planned_partitions = pending_missing[:bootstrap_batch_size]
        pending_partitions = planned_partitions
        remaining_pending_count = max(len(pending_missing) - len(planned_partitions), 0)
        remaining_stale_count = 0
        stale_partitions = []
        has_more_bootstrap_work = bool(remaining_pending_count)
    else:
        backlog = pending_missing if pending_missing else sorted(stale_partitions, reverse=False)
        planned_partitions = backlog[:steady_batch_size]
        pending_partitions = planned_partitions
        remaining_pending_count = max(len(pending_missing) - len([p for p in planned_partitions if p in pending_missing]), 0)
        remaining_stale_count = max(len(stale_partitions) - len([p for p in planned_partitions if p in stale_partitions]), 0)
        has_more_bootstrap_work = False
    return TransformDatasetPlan(
        dataset_id=dataset_id,
        frequency=frequency,
        bootstrap_transform_complete=bool(state.get("bootstrap_transform_complete")),
        bootstrap_active=bootstrap_active,
        bootstrap_priority=resolved_bootstrap_priority,
        scan_start_date=scan_start_date,
        scan_end_date=scan_end_date,
        planned_partitions=planned_partitions,
        pending_partitions=pending_partitions,
        stale_partitions=stale_partitions,
        remaining_pending_count=remaining_pending_count,
        remaining_stale_count=remaining_stale_count,
        raw_latest_partition=_max_partition(ordered),
        current_partition=current_partition,
        has_more_bootstrap_work=has_more_bootstrap_work,
    )
