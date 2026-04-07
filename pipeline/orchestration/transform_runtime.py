from __future__ import annotations

import logging
from datetime import datetime, timezone

from pipeline.core.registry import iter_scheduled_transform_cadence_datasets, normalize_dataset_id
from pipeline.core.snowflake import get_pipeline_state, update_transform_state
from pipeline.core.windowing import month_anchor_date
from pipeline.gold.transform import refresh_gold_dimensions, run_gold_partitions
from pipeline.orchestration.partition_planner import plan_transform_partitions
from pipeline.silver.transform import run_silver_partitions

logger = logging.getLogger(__name__)


def _normalize_conf(conf: dict | None) -> dict[str, str]:
    conf = conf or {}
    normalized: dict[str, str] = {}
    for key in (
        "start_date",
        "end_date",
        "dataset_id",
        "bootstrap_batch_override",
        "bootstrap_priority",
        "bootstrap_chain_origin",
        "source_dag_run_id",
    ):
        value = str(conf.get(key, "") or "").strip()
        normalized[key] = "" if value.lower() == "none" else value
    for key in ("force_rebuild", "bootstrap_mode", "skip_repair", "trigger_matching_transform"):
        value = str(conf.get(key, "") or "").strip().lower()
        normalized[key] = "true" if value in {"1", "true", "yes"} else ""
    depth_value = str(conf.get("bootstrap_chain_depth", "") or "").strip()
    normalized["bootstrap_chain_depth"] = depth_value if depth_value else "0"
    return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def plan_transform_cadence(session, database: str, cadence_group: str, conf: dict | None = None) -> dict:
    normalized_conf = _normalize_conf(conf)
    selected_dataset = normalized_conf["dataset_id"]
    plans: dict[str, dict] = {}
    for dataset in iter_scheduled_transform_cadence_datasets(cadence_group):
        if selected_dataset and dataset["id"] != selected_dataset:
            continue
        plan = plan_transform_partitions(
            session,
            dataset,
            database,
            force_rebuild=bool(normalized_conf["force_rebuild"]),
            override_start_date=normalized_conf["start_date"],
            override_end_date=normalized_conf["end_date"],
            bootstrap_mode=bool(normalized_conf["bootstrap_mode"]),
            bootstrap_batch_override=normalized_conf["bootstrap_batch_override"],
            bootstrap_priority=normalized_conf["bootstrap_priority"],
            skip_repair=bool(normalized_conf["skip_repair"]),
        )
        plans[dataset["id"]] = {
            "dataset_id": plan.dataset_id,
            "frequency": plan.frequency,
            "bootstrap_transform_complete": plan.bootstrap_transform_complete,
            "bootstrap_active": plan.bootstrap_active,
            "bootstrap_priority": plan.bootstrap_priority,
            "scan_start_date": plan.scan_start_date,
            "scan_end_date": plan.scan_end_date,
            "planned_partitions": plan.planned_partitions,
            "pending_partitions": plan.pending_partitions,
            "stale_partitions": plan.stale_partitions,
            "remaining_pending_count": plan.remaining_pending_count,
            "remaining_stale_count": plan.remaining_stale_count,
            "raw_latest_partition": plan.raw_latest_partition,
            "current_partition": plan.current_partition,
            "has_more_bootstrap_work": plan.has_more_bootstrap_work,
        }
        logger.info(
            "Planned transform dataset=%s cadence=%s bootstrap_active=%s priority=%s partitions=%s remaining_pending=%s remaining_stale=%s",
            dataset["id"],
            cadence_group,
            plan.bootstrap_active,
            plan.bootstrap_priority,
            plan.planned_partitions,
            plan.remaining_pending_count,
            plan.remaining_stale_count,
        )
    return {
        "cadence_group": cadence_group,
        "dataset_plans": plans,
        "force_rebuild": bool(normalized_conf["force_rebuild"]),
        "selected_dataset": selected_dataset,
        "override_start_date": normalized_conf["start_date"],
        "override_end_date": normalized_conf["end_date"],
        "bootstrap_mode": bool(normalized_conf["bootstrap_mode"]),
        "bootstrap_chain_depth": int(normalized_conf["bootstrap_chain_depth"] or "0"),
        "bootstrap_batch_override": normalized_conf["bootstrap_batch_override"],
        "bootstrap_priority": normalized_conf["bootstrap_priority"],
        "skip_repair": bool(normalized_conf["skip_repair"]),
        "trigger_matching_transform": bool(normalized_conf["trigger_matching_transform"]),
        "bootstrap_chain_origin": normalized_conf["bootstrap_chain_origin"],
        "source_dag_run_id": normalized_conf["source_dag_run_id"],
    }


def build_silver_for_cadence(session, database: str, cadence_group: str, plan_summary: dict) -> dict:
    datasets = {dataset["id"]: dataset for dataset in iter_scheduled_transform_cadence_datasets(cadence_group)}
    silver_results: dict[str, list[dict]] = {}
    gold_partitions = set()
    for dataset_id, plan in plan_summary["dataset_plans"].items():
        partitions = plan["pending_partitions"]
        if not partitions:
            silver_results[dataset_id] = []
            continue
        logger.info(
            "Running silver cadence=%s dataset=%s partitions=%s",
            cadence_group,
            dataset_id,
            partitions,
        )
        results = run_silver_partitions(
            session,
            dataset=normalize_dataset_id(datasets[dataset_id]["id"]),
            target_dates=partitions,
            database=database,
        )
        silver_results[dataset_id] = results
        for item in results:
            partition_date = item["partition_date"]
            gold_partitions.add(month_anchor_date(partition_date) if plan["frequency"] == "monthly" else partition_date)
        logger.info(
            "Finished silver cadence=%s dataset=%s results=%s",
            cadence_group,
            dataset_id,
            results,
        )
    return {
        "dataset_results": silver_results,
        "gold_partitions": sorted(gold_partitions),
    }


def build_gold_for_cadence(session, database: str, cadence_group: str, silver_summary: dict) -> dict:
    partitions = silver_summary.get("gold_partitions", [])
    if not partitions:
        return {"scope": cadence_group, "partitions": [], "results": {}}
    logger.info("Running gold cadence=%s partitions=%s", cadence_group, partitions)
    results = run_gold_partitions(
        session,
        database=database,
        target_dates=partitions,
        scope=cadence_group,
    )
    logger.info("Finished gold cadence=%s results=%s", cadence_group, results)
    return {"scope": cadence_group, "partitions": partitions, "results": results}


def refresh_cadence_dimensions(session, database: str, cadence_group: str) -> dict:
    results = refresh_gold_dimensions(session, database=database, scope=cadence_group)
    logger.info("Refreshed dimensions cadence=%s results=%s", cadence_group, results)
    return results


def finalize_transform_state(
    session,
    database: str,
    cadence_group: str,
    plan_summary: dict,
    silver_summary: dict,
    gold_summary: dict,
    *,
    error_message: str | None = None,
) -> dict:
    finalized: dict[str, dict] = {}
    started_ts = _utc_now()
    success_ts = None if error_message else _utc_now()
    gold_partitions = gold_summary.get("partitions", [])
    silver_results = silver_summary.get("dataset_results", {})
    for dataset in iter_scheduled_transform_cadence_datasets(cadence_group):
        dataset_id = dataset["id"]
        if dataset_id not in plan_summary["dataset_plans"]:
            continue
        plan = plan_summary["dataset_plans"][dataset_id]
        prior_state = get_pipeline_state(session, database, dataset_id)
        silver_partitions = [item["partition_date"] for item in silver_results.get(dataset_id, [])]
        last_silver_partition = max(silver_partitions) if silver_partitions else prior_state.get("last_silver_partition")
        last_gold_partition = max(gold_partitions) if gold_partitions else prior_state.get("last_gold_partition")
        bootstrap_transform_complete = bool(prior_state.get("bootstrap_transform_complete"))
        if not error_message:
            bootstrap_transform_complete = not plan["has_more_bootstrap_work"] and not plan["remaining_pending_count"]
        update_transform_state(
            session,
            database,
            dataset_id,
            frequency=dataset["frequency"],
            bootstrap_transform_complete=bootstrap_transform_complete,
            last_silver_partition=str(last_silver_partition) if last_silver_partition else None,
            last_gold_partition=str(last_gold_partition) if last_gold_partition else None,
            last_transform_started_at=started_ts,
            last_transform_succeeded_at=success_ts,
            last_transform_error_message=error_message,
        )
        finalized[dataset_id] = {
            "bootstrap_transform_complete": bootstrap_transform_complete,
            "last_silver_partition": last_silver_partition,
            "last_gold_partition": last_gold_partition,
            "has_more_bootstrap_work": plan["has_more_bootstrap_work"],
        }
        logger.info(
            "Finalized transform state dataset=%s cadence=%s bootstrap_complete=%s last_silver_partition=%s last_gold_partition=%s has_more_bootstrap_work=%s",
            dataset_id,
            cadence_group,
            bootstrap_transform_complete,
            last_silver_partition,
            last_gold_partition,
            plan["has_more_bootstrap_work"],
        )
    return finalized
