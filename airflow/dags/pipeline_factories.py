"""Compatibility wrapper for Airflow DAG factory imports.

The dataset and business-serving DAG builders now live in separate modules so
their responsibilities are easier to follow. This file keeps the original
import surface and central DAG registration entrypoint.
"""

from __future__ import annotations

from airflow import DAG

from pipeline_dataset_dags import (
    build_backfill_dag,
    build_bronze_repair_dag,
    build_bronze_verification_dag,
    build_incremental_dag,
)
from pipeline_serving_dags import build_grid_operations_dag, build_resource_planning_dag
from pipeline_support import load_dataset_registry


def register_all_dags() -> dict[str, DAG]:
    """Build and return every dynamically-registered DAG in this repository."""

    datasets = load_dataset_registry()
    dags: dict[str, DAG] = {}
    for dataset_id, dataset in datasets.items():
        dags[f"{dataset_id}_incremental"] = build_incremental_dag(dataset_id, dataset)
        dags[f"{dataset_id}_backfill"] = build_backfill_dag(dataset_id, dataset)
        dags[f"{dataset_id}_bronze_hourly_verification"] = build_bronze_verification_dag(dataset_id, dataset)
        dags[f"{dataset_id}_bronze_hourly_repair"] = build_bronze_repair_dag(dataset_id, dataset)
    dags["platinum_grid_operations_hourly"] = build_grid_operations_dag()
    dags["platinum_resource_planning_daily"] = build_resource_planning_dag()
    return dags
