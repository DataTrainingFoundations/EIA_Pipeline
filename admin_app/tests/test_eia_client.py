from __future__ import annotations

from datetime import datetime, timezone

from config import AppConfig
from eia_client import build_expected_stage_keys
from models import ComparisonRequest
from registry import DatasetRegistryEntry


def _config() -> AppConfig:
    return AppConfig(
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="platform",
        postgres_user="platform",
        postgres_password="platform",
        minio_endpoint="minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
        eia_api_key="test",
        app_timezone="UTC",
        api_cache_ttl_seconds=300,
        query_timeout_seconds=60,
        default_days=7,
        airflow_log_dir=__import__("pathlib").Path("."),
        registry_path=__import__("pathlib").Path("ingestion/src/dataset_registry.yml"),
    )


def test_build_expected_region_gold_and_platinum_keys(monkeypatch) -> None:  # noqa: ANN001
    dataset = DatasetRegistryEntry(
        dataset_id="electricity_region_data",
        route="electricity/rto/region-data",
        topic="topic",
        frequency="hourly",
        data_columns=("value",),
        default_facets={"type": ["D", "DF"]},
        bronze_output_path="s3a://bronze/electricity_region_data",
        silver_output_path="s3a://silver/region_data",
        gold_output_path="s3a://gold/facts/region_demand_forecast_hourly",
    )
    rows = [
        {"period": "2026-03-09T00", "respondent": "PJM", "respondent-name": "PJM", "type": "D", "value": "100"},
        {"period": "2026-03-09T00", "respondent": "PJM", "respondent-name": "PJM", "type": "DF", "value": "95"},
        {"period": "2026-03-09T01", "respondent": "PJM", "respondent-name": "PJM", "type": "D", "value": "105"},
    ]
    monkeypatch.setattr("eia_client.fetch_dataset_rows", lambda *args, **kwargs: rows)

    gold_request = ComparisonRequest(
        dataset_id="electricity_region_data",
        stage="gold",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )
    gold_df = build_expected_stage_keys(_config(), gold_request, dataset)
    assert len(gold_df) == 2

    platinum_request = ComparisonRequest(
        dataset_id="platinum.region_demand_daily",
        stage="platinum",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )
    platinum_df = build_expected_stage_keys(_config(), platinum_request, dataset)
    assert len(platinum_df) == 1
    assert platinum_df.iloc[0]["respondent"] == "PJM"


def test_build_expected_fuel_gold_keys(monkeypatch) -> None:  # noqa: ANN001
    dataset = DatasetRegistryEntry(
        dataset_id="electricity_fuel_type_data",
        route="electricity/rto/fuel-type-data",
        topic="topic",
        frequency="hourly",
        data_columns=("value",),
        default_facets={},
        bronze_output_path="s3a://bronze/electricity_fuel_type_data",
        silver_output_path="s3a://silver/fuel_type_data",
        gold_output_path="s3a://gold/facts/fuel_generation_hourly",
    )
    rows = [
        {"period": "2026-03-09T00", "respondent": "PJM", "respondent-name": "PJM", "fueltype": "NG", "value": "40"},
        {"period": "2026-03-09T00", "respondent": "PJM", "respondent-name": "PJM", "fueltype": "SUN", "value": "20"},
    ]
    monkeypatch.setattr("eia_client.fetch_dataset_rows", lambda *args, **kwargs: rows)

    request = ComparisonRequest(
        dataset_id="electricity_fuel_type_data",
        stage="gold",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )
    gold_df = build_expected_stage_keys(_config(), request, dataset)
    assert len(gold_df) == 2
    assert set(gold_df["dimension_value"]) == {"NG", "SUN"}
