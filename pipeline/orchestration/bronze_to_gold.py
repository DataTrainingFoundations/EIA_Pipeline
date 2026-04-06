from __future__ import annotations

from datetime import datetime, timezone

from pipeline.core.registry import iter_scheduled_cadence_datasets, normalize_dataset_id
from pipeline.core.snowflake import get_pipeline_state, upsert_pipeline_state
from pipeline.core.windowing import month_anchor_date
from pipeline.gold.transform import refresh_gold_dimensions, run_gold_partitions
from pipeline.ingestion.raw_ingest import ingest_dataset
from pipeline.orchestration.partition_planner import plan_dataset_partitions
from pipeline.silver.transform import run_silver_partitions


def _normalize_conf(conf: dict | None) -> dict[str, str]:
    conf = conf or {}
    normalized: dict[str, str] = {}
    for key in ("start_date", "end_date", "dataset_id"):
        value = str(conf.get(key, "") or "").strip()
        normalized[key] = "" if value.lower() == "none" else value
    force_value = str(conf.get("force_rebuild", "") or "").strip().lower()
    normalized["force_rebuild"] = "true" if force_value in {"1", "true", "yes"} else ""
    return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def plan_cadence_partitions(session, database: str, cadence_group: str, conf: dict | None = None) -> dict:
    normalized_conf = _normalize_conf(conf)
    selected_dataset = normalized_conf["dataset_id"]
    plans: dict[str, dict] = {}
    for dataset in iter_scheduled_cadence_datasets(cadence_group):
        if selected_dataset and dataset["id"] != selected_dataset:
            continue
        plan = plan_dataset_partitions(
            session,
            dataset,
            database,
            force_rebuild=bool(normalized_conf["force_rebuild"]),
            override_start_date=normalized_conf["start_date"],
            override_end_date=normalized_conf["end_date"],
        )
        plans[dataset["id"]] = {
            "dataset_id": plan.dataset_id,
            "frequency": plan.frequency,
            "bootstrap_active": plan.bootstrap_active,
            "bootstrap_complete": plan.bootstrap_complete,
            "ingest_start_date": plan.ingest_start_date,
            "ingest_end_date": plan.ingest_end_date,
            "pending_partitions": plan.pending_partitions,
            "stale_partitions": plan.stale_partitions,
            "raw_latest_partition": plan.raw_latest_partition,
            "current_partition": plan.current_partition,
        }
    return {
        "cadence_group": cadence_group,
        "dataset_plans": plans,
        "force_rebuild": bool(normalized_conf["force_rebuild"]),
        "selected_dataset": selected_dataset,
        "override_start_date": normalized_conf["start_date"],
        "override_end_date": normalized_conf["end_date"],
    }


def ingest_cadence_datasets(session, eia_settings, database: str, cadence_group: str, plan_summary: dict) -> dict:
    ingest_results: dict[str, dict] = {}
    datasets = {dataset["id"]: dataset for dataset in iter_scheduled_cadence_datasets(cadence_group)}
    for dataset_id, plan in plan_summary["dataset_plans"].items():
        dataset = datasets[dataset_id]
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
    return ingest_results


def build_silver_for_cadence(session, database: str, cadence_group: str, plan_summary: dict) -> dict:
    datasets = {dataset["id"]: dataset for dataset in iter_scheduled_cadence_datasets(cadence_group)}
    silver_results: dict[str, list[dict]] = {}
    gold_partitions = set()
    for dataset_id, plan in plan_summary["dataset_plans"].items():
        partitions = plan["pending_partitions"]
        if not partitions:
            silver_results[dataset_id] = []
            continue
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
    return {
        "dataset_results": silver_results,
        "gold_partitions": sorted(gold_partitions),
    }


def build_gold_for_cadence(session, database: str, cadence_group: str, silver_summary: dict) -> dict:
    partitions = silver_summary.get("gold_partitions", [])
    if not partitions:
        return {"scope": cadence_group, "partitions": [], "results": {}}
    results = run_gold_partitions(
        session,
        database=database,
        target_dates=partitions,
        scope=cadence_group,
    )
    return {"scope": cadence_group, "partitions": partitions, "results": results}


def refresh_cadence_dimensions(session, database: str, cadence_group: str) -> dict:
    return refresh_gold_dimensions(session, database=database, scope=cadence_group)


def finalize_cadence_state(
    session,
    database: str,
    cadence_group: str,
    plan_summary: dict,
    ingest_summary: dict,
    silver_summary: dict,
    gold_summary: dict,
    *,
    error_message: str | None = None,
) -> dict:
    finalized: dict[str, dict] = {}
    success_ts = None if error_message else _utc_now()
    started_ts = _utc_now()
    gold_partitions = gold_summary.get("partitions", [])
    silver_results = silver_summary.get("dataset_results", {})
    for dataset in iter_scheduled_cadence_datasets(cadence_group):
        dataset_id = dataset["id"]
        if dataset_id not in plan_summary["dataset_plans"]:
            continue
        plan = plan_summary["dataset_plans"][dataset_id]
        prior_state = get_pipeline_state(session, database, dataset_id)
        silver_partitions = [item["partition_date"] for item in silver_results.get(dataset_id, [])]
        last_raw_partition = plan["raw_latest_partition"] or (
            ingest_summary.get(dataset_id, {}).get("end_date") or prior_state.get("last_raw_partition")
        )
        last_silver_partition = max(silver_partitions) if silver_partitions else prior_state.get("last_silver_partition")
        last_gold_partition = max(gold_partitions) if gold_partitions else prior_state.get("last_gold_partition")
        bootstrap_complete = bool(prior_state.get("bootstrap_complete"))
        if not error_message:
            current_partition = plan["current_partition"]
            reached_current = last_raw_partition is not None and str(last_raw_partition) >= current_partition
            bootstrap_complete = reached_current and not plan["pending_partitions"]
        upsert_pipeline_state(
            session,
            database,
            dataset_id,
            frequency=dataset["frequency"],
            bootstrap_complete=bootstrap_complete,
            last_raw_partition=str(last_raw_partition) if last_raw_partition else None,
            last_silver_partition=str(last_silver_partition) if last_silver_partition else None,
            last_gold_partition=str(last_gold_partition) if last_gold_partition else None,
            last_run_started_at=started_ts,
            last_run_succeeded_at=success_ts,
            last_error_message=error_message,
        )
        finalized[dataset_id] = {
            "bootstrap_complete": bootstrap_complete,
            "last_raw_partition": last_raw_partition,
            "last_silver_partition": last_silver_partition,
            "last_gold_partition": last_gold_partition,
        }
    return finalized


def run_bronze_to_gold(
    session,
    eia_settings,
    *,
    database: str,
    cadence_group: str,
    conf: dict | None = None,
) -> dict:
    plan_summary = plan_cadence_partitions(session, database, cadence_group, conf)
    ingest_summary = ingest_cadence_datasets(session, eia_settings, database, cadence_group, plan_summary)
    plan_summary = plan_cadence_partitions(session, database, cadence_group, conf)
    silver_summary = build_silver_for_cadence(session, database, cadence_group, plan_summary)
    gold_summary = build_gold_for_cadence(session, database, cadence_group, silver_summary)
    dimension_summary = refresh_cadence_dimensions(session, database, cadence_group)
    state_summary = finalize_cadence_state(
        session,
        database,
        cadence_group,
        plan_summary,
        ingest_summary,
        silver_summary,
        gold_summary,
    )
    return {
        "plan": plan_summary,
        "ingest": ingest_summary,
        "silver": silver_summary,
        "gold": gold_summary,
        "dimensions": dimension_summary,
        "state": state_summary,
    }
