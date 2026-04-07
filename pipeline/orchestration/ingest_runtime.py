from __future__ import annotations

import logging
from datetime import datetime, timezone

from pipeline.core.registry import iter_scheduled_ingest_cadence_datasets
from pipeline.core.snowflake import get_pipeline_state, update_ingest_state
from pipeline.ingestion.raw_ingest import ingest_dataset
from pipeline.orchestration.ingest_planner import plan_ingest_windows

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
    for key in ("bootstrap_mode",):
        value = str(conf.get(key, "") or "").strip().lower()
        normalized[key] = "true" if value in {"1", "true", "yes"} else ""
    trigger_value = str(conf.get("trigger_matching_transform", "true") or "").strip().lower()
    normalized["trigger_matching_transform"] = "true" if trigger_value in {"1", "true", "yes"} else ""
    depth_value = str(conf.get("bootstrap_chain_depth", "") or "").strip()
    normalized["bootstrap_chain_depth"] = depth_value if depth_value else "0"
    return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def plan_ingest_cadence(session, database: str, cadence_group: str, conf: dict | None = None) -> dict:
    normalized_conf = _normalize_conf(conf)
    selected_dataset = normalized_conf["dataset_id"]
    plans: dict[str, dict] = {}
    for dataset in iter_scheduled_ingest_cadence_datasets(cadence_group):
        if selected_dataset and dataset["id"] != selected_dataset:
            continue
        plan = plan_ingest_windows(
            session,
            dataset,
            database,
            override_start_date=normalized_conf["start_date"],
            override_end_date=normalized_conf["end_date"],
            bootstrap_mode=bool(normalized_conf["bootstrap_mode"]),
            bootstrap_batch_override=normalized_conf["bootstrap_batch_override"],
            bootstrap_priority=normalized_conf["bootstrap_priority"],
        )
        plans[dataset["id"]] = {
            "dataset_id": plan.dataset_id,
            "frequency": plan.frequency,
            "bootstrap_ingest_complete": plan.bootstrap_ingest_complete,
            "bootstrap_active": plan.bootstrap_active,
            "bootstrap_strategy": plan.bootstrap_strategy,
            "bootstrap_batch_start": plan.bootstrap_batch_start,
            "bootstrap_batch_end": plan.bootstrap_batch_end,
            "remaining_partitions_estimate": plan.remaining_partitions_estimate,
            "latest_target_partition": plan.latest_target_partition,
            "has_more_bootstrap_work": plan.has_more_bootstrap_work,
            "ingest_start_date": plan.ingest_start_date,
            "ingest_end_date": plan.ingest_end_date,
            "current_partition": plan.current_partition,
        }
        logger.info(
            "Planned ingest dataset=%s cadence=%s bootstrap_active=%s strategy=%s start=%s end=%s remaining_estimate=%s",
            dataset["id"],
            cadence_group,
            plan.bootstrap_active,
            plan.bootstrap_strategy,
            plan.ingest_start_date,
            plan.ingest_end_date,
            plan.remaining_partitions_estimate,
        )
    return {
        "cadence_group": cadence_group,
        "dataset_plans": plans,
        "selected_dataset": selected_dataset,
        "override_start_date": normalized_conf["start_date"],
        "override_end_date": normalized_conf["end_date"],
        "bootstrap_mode": bool(normalized_conf["bootstrap_mode"]),
        "bootstrap_chain_depth": int(normalized_conf["bootstrap_chain_depth"] or "0"),
        "bootstrap_batch_override": normalized_conf["bootstrap_batch_override"],
        "bootstrap_priority": normalized_conf["bootstrap_priority"],
        "trigger_matching_transform": bool(normalized_conf["trigger_matching_transform"]),
        "bootstrap_chain_origin": normalized_conf["bootstrap_chain_origin"],
        "source_dag_run_id": normalized_conf["source_dag_run_id"],
    }


def run_ingest_cadence(session, eia_settings, database: str, cadence_group: str, plan_summary: dict) -> dict:
    ingest_results: dict[str, dict] = {}
    datasets = {dataset["id"]: dataset for dataset in iter_scheduled_ingest_cadence_datasets(cadence_group)}
    for dataset_id, plan in plan_summary["dataset_plans"].items():
        dataset = datasets[dataset_id]
        logger.info(
            "Running ingest dataset=%s cadence=%s start=%s end=%s bootstrap_active=%s",
            dataset_id,
            cadence_group,
            plan["ingest_start_date"],
            plan["ingest_end_date"],
            plan["bootstrap_active"],
        )
        written = ingest_dataset(
            session,
            eia_settings,
            dataset,
            start_date=plan["ingest_start_date"],
            end_date=plan["ingest_end_date"],
        )
        ingest_results[dataset_id] = {
            "written": written,
            "start_date": plan["ingest_start_date"],
            "end_date": plan["ingest_end_date"],
        }
        logger.info(
            "Finished ingest dataset=%s cadence=%s written=%s start=%s end=%s",
            dataset_id,
            cadence_group,
            written,
            plan["ingest_start_date"],
            plan["ingest_end_date"],
        )
    return ingest_results


def finalize_ingest_state(
    session,
    database: str,
    cadence_group: str,
    plan_summary: dict,
    ingest_summary: dict,
    *,
    error_message: str | None = None,
) -> dict:
    finalized: dict[str, dict] = {}
    started_ts = _utc_now()
    success_ts = None if error_message else _utc_now()
    for dataset in iter_scheduled_ingest_cadence_datasets(cadence_group):
        dataset_id = dataset["id"]
        if dataset_id not in plan_summary["dataset_plans"]:
            continue
        plan = plan_summary["dataset_plans"][dataset_id]
        prior_state = get_pipeline_state(session, database, dataset_id)
        last_raw_partition = ingest_summary.get(dataset_id, {}).get("end_date") or prior_state.get("last_raw_partition")
        bootstrap_ingest_complete = bool(prior_state.get("bootstrap_ingest_complete"))
        if not error_message:
            bootstrap_ingest_complete = not plan["has_more_bootstrap_work"]
        update_ingest_state(
            session,
            database,
            dataset_id,
            frequency=dataset["frequency"],
            bootstrap_ingest_complete=bootstrap_ingest_complete,
            last_raw_partition=str(last_raw_partition) if last_raw_partition else None,
            last_ingest_started_at=started_ts,
            last_ingest_succeeded_at=success_ts,
            last_ingest_error_message=error_message,
        )
        finalized[dataset_id] = {
            "bootstrap_ingest_complete": bootstrap_ingest_complete,
            "last_raw_partition": last_raw_partition,
            "has_more_bootstrap_work": plan["has_more_bootstrap_work"],
        }
        logger.info(
            "Finalized ingest state dataset=%s cadence=%s bootstrap_complete=%s last_raw_partition=%s has_more_bootstrap_work=%s",
            dataset_id,
            cadence_group,
            bootstrap_ingest_complete,
            last_raw_partition,
            plan["has_more_bootstrap_work"],
        )
    return finalized
