import pytest

pytest.importorskip("airflow")

import pipeline_factories


def test_register_all_dags_returns_expected_keys(monkeypatch) -> None:  # noqa: ANN001
    dataset = {"topic": "eia_electricity_region_data"}
    monkeypatch.setattr(pipeline_factories, "load_dataset_registry", lambda: {"electricity_region_data": dataset})
    monkeypatch.setattr(pipeline_factories, "build_incremental_dag", lambda dataset_id, _dataset: {"dag_id": f"{dataset_id}_incremental"})
    monkeypatch.setattr(pipeline_factories, "build_backfill_dag", lambda dataset_id, _dataset: {"dag_id": f"{dataset_id}_backfill"})
    monkeypatch.setattr(pipeline_factories, "build_bronze_verification_dag", lambda dataset_id, _dataset: {"dag_id": f"{dataset_id}_bronze_hourly_verification"})
    monkeypatch.setattr(pipeline_factories, "build_bronze_repair_dag", lambda dataset_id, _dataset: {"dag_id": f"{dataset_id}_bronze_hourly_repair"})
    monkeypatch.setattr(pipeline_factories, "build_grid_operations_dag", lambda: {"dag_id": "platinum_grid_operations_hourly"})
    monkeypatch.setattr(pipeline_factories, "build_resource_planning_dag", lambda: {"dag_id": "platinum_resource_planning_daily"})

    dags = pipeline_factories.register_all_dags()

    assert sorted(dags) == [
        "electricity_region_data_backfill",
        "electricity_region_data_bronze_hourly_repair",
        "electricity_region_data_bronze_hourly_verification",
        "electricity_region_data_incremental",
        "platinum_grid_operations_hourly",
        "platinum_resource_planning_daily",
    ]
