from __future__ import annotations

from pipeline.orchestration import ingest_planner, partition_planner


def test_ingest_planner_treats_incomplete_historical_hourly_partition_as_missing(monkeypatch) -> None:
    dataset = {
        "id": "electricity_generation_hourly",
        "frequency": "hourly",
        "snowflake_table": "ELECTRICITY_GENERATION_RAW",
        "bootstrap_enabled": True,
        "bootstrap_start_date": "2026-04-01",
        "bootstrap_max_partitions_per_run": 10,
        "max_partitions_per_run": 7,
        "bootstrap_priority": "latest_first",
    }

    monkeypatch.setattr(
        ingest_planner,
        "get_pipeline_state",
        lambda session, database, dataset_id: {"bootstrap_ingest_complete": False},
    )
    monkeypatch.setattr(ingest_planner, "current_partition_for_frequency", lambda frequency: "2026-04-08")
    monkeypatch.setattr(
        ingest_planner,
        "raw_partitions_for_dataset",
        lambda session, table_name, **kwargs: [
            {"partition_date": "2026-04-01", "is_complete": False},
            {"partition_date": "2026-04-02", "is_complete": True},
            {"partition_date": "2026-04-03", "is_complete": True},
        ],
    )

    plan = ingest_planner.plan_ingest_windows(None, dataset, "EIA_PIPELINE", bootstrap_mode=True)

    assert plan.bootstrap_active is True
    assert plan.ingest_start_date == "2026-04-01"
    assert "2026-04-01" <= plan.ingest_end_date


def test_transform_planner_excludes_incomplete_historical_hourly_raw_partition(monkeypatch) -> None:
    dataset = {
        "id": "electricity_generation_hourly",
        "frequency": "hourly",
        "snowflake_table": "ELECTRICITY_GENERATION_RAW",
        "bootstrap_start_date": "2026-04-01",
        "bootstrap_max_partitions_per_run": 10,
        "max_partitions_per_run": 7,
        "bootstrap_priority": "latest_first",
        "cadence_group": "hourly",
    }

    monkeypatch.setattr(
        partition_planner,
        "get_pipeline_state",
        lambda session, database, dataset_id: {"bootstrap_transform_complete": False},
    )
    monkeypatch.setattr(partition_planner, "current_partition_for_frequency", lambda frequency: "2026-04-08")
    monkeypatch.setattr(
        partition_planner,
        "raw_partitions_for_dataset",
        lambda session, table_name, **kwargs: [
            {"partition_date": "2026-04-01", "processed_at": "2026-04-08T13:58:11Z", "is_complete": False},
            {"partition_date": "2026-04-02", "processed_at": "2026-04-08T15:15:32Z", "is_complete": True},
        ],
    )
    monkeypatch.setattr(partition_planner, "pipeline_partitions_for_table", lambda *args, **kwargs: {})
    monkeypatch.setattr(partition_planner, "_gold_freshness", lambda *args, **kwargs: {})

    plan = partition_planner.plan_transform_partitions(None, dataset, "EIA_PIPELINE", bootstrap_mode=True)

    assert "2026-04-01" not in plan.planned_partitions
    assert plan.planned_partitions == ["2026-04-02"]


def test_transform_planner_monthly_gold_freshness_targets_fact_sales_monthly(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def _capture_table(session, table_name, **kwargs):
        captured["table_name"] = table_name
        return {}

    monkeypatch.setattr(
        partition_planner,
        "pipeline_partitions_for_table",
        _capture_table,
    )

    result = partition_planner._gold_freshness(
        None,
        "EIA_PIPELINE",
        dataset_id="electricity_retail_sales_monthly",
        frequency="monthly",
        cadence_group="monthly",
        start_date="2026-04-01",
        end_date="2026-04-01",
    )

    assert result == {}
    assert captured["table_name"] == "EIA_PIPELINE.GOLD.FACT_SALES_MONTHLY"
