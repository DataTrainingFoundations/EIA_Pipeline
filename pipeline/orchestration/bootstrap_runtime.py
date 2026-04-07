from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


BOOTSTRAP_CHAIN_DEPTH_LIMIT = 10


def should_continue_bootstrap(finalized_summary: dict[str, dict] | None, *, progress_made: bool) -> bool:
    if not progress_made or not finalized_summary:
        return False
    return any(bool(item.get("has_more_bootstrap_work")) for item in finalized_summary.values())


def should_trigger_matching_transform(
    plan_summary: dict[str, Any] | None,
    ingest_summary: dict[str, dict] | None,
    finalized_summary: dict[str, dict] | None,
) -> bool:
    if not plan_summary or not ingest_summary or not finalized_summary:
        return False
    if not plan_summary.get("trigger_matching_transform"):
        return False
    if not any(bool(item.get("bootstrap_active")) for item in plan_summary.get("dataset_plans", {}).values()):
        return False
    return any(int(result.get("written", 0) or 0) > 0 for result in ingest_summary.values())


def next_bootstrap_conf(
    current_conf: dict[str, Any] | None,
    *,
    source_dag_id: str,
    source_dag_run_id: str,
    trigger_matching_transform: bool | None = None,
) -> dict[str, Any]:
    conf = dict(current_conf or {})
    depth = int(str(conf.get("bootstrap_chain_depth", "0") or "0"))
    conf["bootstrap_mode"] = True
    conf["bootstrap_chain_depth"] = depth + 1
    conf["bootstrap_chain_origin"] = conf.get("bootstrap_chain_origin") or source_dag_id
    conf["source_dag_run_id"] = source_dag_run_id
    if trigger_matching_transform is not None:
        conf["trigger_matching_transform"] = bool(trigger_matching_transform)
    return conf


def chain_depth_exhausted(current_conf: dict[str, Any] | None) -> bool:
    conf = current_conf or {}
    depth = int(str(conf.get("bootstrap_chain_depth", "0") or "0"))
    return depth >= BOOTSTRAP_CHAIN_DEPTH_LIMIT


def build_trigger_run_id(dag_id: str, *, source_run_id: str, depth: int) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{dag_id}__bootstrap__d{depth}__{timestamp}__{source_run_id.replace(':', '_')}"


def has_active_dag_run(dag_id: str, *, exclude_run_id: str = "") -> bool:
    from airflow.models import DagRun
    from airflow.settings import Session

    session = Session()
    try:
        active_states = ("queued", "running")
        query = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.state.in_(active_states))
        if exclude_run_id:
            query = query.filter(DagRun.run_id != exclude_run_id)
        return query.limit(1).first() is not None
    finally:
        session.close()


def trigger_dag_if_idle(
    dag_id: str,
    *,
    conf: dict[str, Any],
    source_run_id: str,
    exclude_run_id: str = "",
) -> dict[str, Any]:
    from airflow.api.common.trigger_dag import trigger_dag

    depth = int(str(conf.get("bootstrap_chain_depth", "0") or "0"))
    if has_active_dag_run(dag_id, exclude_run_id=exclude_run_id):
        return {"triggered": False, "reason": "active_run_exists", "dag_id": dag_id}
    run_id = build_trigger_run_id(dag_id, source_run_id=source_run_id, depth=depth)
    trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=conf,
        replace_microseconds=False,
    )
    return {"triggered": True, "dag_id": dag_id, "run_id": run_id, "depth": depth}
