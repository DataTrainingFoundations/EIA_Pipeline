from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import AppConfig
from models import ComparisonRequest
from parquet_store import ParquetStore, _sql_array, _standardize
from registry import DatasetRegistryEntry


def _config(tmp_path: Path) -> AppConfig:
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
        airflow_log_dir=tmp_path,
        registry_path=tmp_path / "dataset_registry.yml",
    )


def test_sql_array_quotes_and_escapes_values() -> None:
    assert _sql_array(["PJM", "O'Brien"]) == "['PJM','O''Brien']"


def test_standardize_drops_invalid_rows_and_formats_grain_keys() -> None:
    df = pd.DataFrame(
        [
            {"period_start_utc": "2026-03-10T00:00:00Z", "respondent": "PJM", "dimension_value": "D"},
            {"period_start_utc": "bad", "respondent": "PJM", "dimension_value": "D"},
            {"period_start_utc": "2026-03-10T01:00:00Z", "respondent": None, "dimension_value": "D"},
        ]
    )

    standardized = _standardize(df)

    assert standardized["grain_key"].tolist() == ["2026-03-10T00:00:00Z|PJM|D"]


def test_standardize_returns_empty_schema_for_empty_frame() -> None:
    standardized = _standardize(pd.DataFrame())

    assert standardized.columns.tolist() == ["grain_key", "period_start_utc", "respondent", "dimension_value"]


def test_date_patterns_cover_multiple_days_and_same_day_fallback(tmp_path: Path) -> None:
    store = ParquetStore(_config(tmp_path))
    multi_day = store._date_patterns(
        "s3://silver/region_data",
        ComparisonRequest(
            dataset_id="electricity_region_data",
            stage="silver",
            start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 11, tzinfo=timezone.utc),
        ),
    )
    same_day = store._date_patterns(
        "s3://silver/region_data",
        ComparisonRequest(
            dataset_id="electricity_region_data",
            stage="silver",
            start_utc=datetime(2026, 3, 9, 0, 0, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
        ),
    )

    assert multi_day == [
        "s3://silver/region_data/event_date=2026-03-09/*.parquet",
        "s3://silver/region_data/event_date=2026-03-10/*.parquet",
    ]
    assert same_day == ["s3://silver/region_data/event_date=2026-03-09/*.parquet"]


def test_bronze_patterns_cover_inclusive_final_day(tmp_path: Path) -> None:
    store = ParquetStore(_config(tmp_path))
    request = ComparisonRequest(
        dataset_id="electricity_region_data",
        stage="bronze",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 11, tzinfo=timezone.utc),
    )

    patterns = store._bronze_patterns("s3://bronze/events", "electricity_region_data", request)

    assert len(patterns) == 2
    assert patterns[0].endswith("/event_day=9/event_hour=*/*.parquet")
    assert patterns[1].endswith("/event_day=10/event_hour=*/*.parquet")


def test_list_files_deduplicates_and_sorts_local_matches(tmp_path: Path) -> None:
    first = tmp_path / "a.parquet"
    second = tmp_path / "b.parquet"
    first.touch()
    second.touch()

    store = ParquetStore(_config(tmp_path))
    matches = store._list_files([str(tmp_path / "*.parquet"), str(first)])

    assert matches == [str(first), str(second)]


def test_list_files_uses_mocked_s3_filesystem(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    store = ParquetStore(_config(tmp_path))

    class FakeS3:
        def glob(self, pattern: str) -> list[str]:
            assert pattern == "bucket/path/*.parquet"
            return ["bucket/path/b.parquet", "bucket/path/a.parquet", "bucket/path/a.parquet"]

    monkeypatch.setattr(store, "_s3_filesystem", lambda: FakeS3())

    matches = store._list_files(["s3://bucket/path/*.parquet"])

    assert matches == ["s3://bucket/path/a.parquet", "s3://bucket/path/b.parquet"]


def test_query_stage_keys_from_local_bronze_and_silver_files(tmp_path: Path) -> None:
    bronze_file = tmp_path / "bronze.parquet"
    silver_file = tmp_path / "silver.parquet"

    bronze_df = pd.DataFrame(
        [
            {
                "event_ts": pd.Timestamp("2026-03-09T00:00:00Z"),
                "dataset_partition": "electricity_region_data",
                "raw_json": json.dumps({"payload": {"respondent": "PJM", "type": "D"}}, separators=(",", ":")),
            }
        ]
    )
    bronze_df.to_parquet(bronze_file, index=False)

    silver_df = pd.DataFrame(
        [{"period": pd.Timestamp("2026-03-09T00:00:00Z"), "respondent": "PJM", "type": "D"}]
    )
    silver_df.to_parquet(silver_file, index=False)

    store = ParquetStore(_config(tmp_path))
    bronze_request = ComparisonRequest(
        dataset_id="electricity_region_data",
        stage="bronze",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )
    silver_request = ComparisonRequest(
        dataset_id="electricity_region_data",
        stage="silver",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )

    bronze_keys = store.query_stage_keys_from_files(bronze_request, [str(bronze_file)])
    silver_keys = store.query_stage_keys_from_files(silver_request, [str(silver_file)])

    assert bronze_keys.iloc[0]["dimension_value"] == "D"
    assert silver_keys.iloc[0]["respondent"] == "PJM"


def test_query_stage_keys_from_local_power_files(tmp_path: Path) -> None:
    bronze_file = tmp_path / "power_bronze.parquet"
    silver_file = tmp_path / "power_silver.parquet"
    gold_file = tmp_path / "power_gold.parquet"

    bronze_df = pd.DataFrame(
        [
            {
                "event_ts": pd.Timestamp("2026-01-01T00:00:00Z"),
                "dataset_partition": "electricity_power_operational_data",
                "raw_json": json.dumps(
                    {"payload": {"period": "2026-01", "location": "US", "sectorid": "1", "fueltypeid": "COL"}},
                    separators=(",", ":"),
                ),
            }
        ]
    )
    bronze_df.to_parquet(bronze_file, index=False)

    stage_df = pd.DataFrame(
        [{"period": pd.Timestamp("2026-01-01T00:00:00Z"), "location": "US", "sector_id": "1", "fueltype_id": "COL"}]
    )
    stage_df.to_parquet(silver_file, index=False)
    stage_df.to_parquet(gold_file, index=False)

    store = ParquetStore(_config(tmp_path))
    bronze_request = ComparisonRequest(
        dataset_id="electricity_power_operational_data",
        stage="bronze",
        start_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_utc=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )
    silver_request = ComparisonRequest(
        dataset_id="electricity_power_operational_data",
        stage="silver",
        start_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_utc=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )
    gold_request = ComparisonRequest(
        dataset_id="electricity_power_operational_data",
        stage="gold",
        start_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_utc=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )

    bronze_keys = store.query_stage_keys_from_files(bronze_request, [str(bronze_file)])
    silver_keys = store.query_stage_keys_from_files(silver_request, [str(silver_file)])
    gold_keys = store.query_stage_keys_from_files(gold_request, [str(gold_file)])

    assert bronze_keys.iloc[0]["grain_key"] == "2026-01-01T00:00:00Z|US|1|COL"
    assert silver_keys.iloc[0]["dimension_value"] == "1|COL"
    assert gold_keys.iloc[0]["respondent"] == "US"


def test_fetch_stage_keys_dispatches_by_stage_and_returns_empty_for_unsupported_stage(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    store = ParquetStore(_config(tmp_path))
    dataset = DatasetRegistryEntry(
        dataset_id="electricity_region_data",
        route="electricity/rto/region-data",
        topic="topic",
        frequency="hourly",
        data_columns=("value",),
        default_facets={},
        bronze_output_path="s3a://bronze/events",
        silver_output_path="s3a://silver/region_data",
        gold_output_path="s3a://gold/facts/region_demand_forecast_hourly",
    )
    request = ComparisonRequest(
        dataset_id="electricity_region_data",
        stage="gold",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(store, "_list_files", lambda patterns: captured.setdefault("patterns", patterns) or [])
    monkeypatch.setattr(store, "query_stage_keys_from_files", lambda _request, _files: pd.DataFrame())

    result = store.fetch_stage_keys(request, dataset)
    unsupported = store.fetch_stage_keys(
        ComparisonRequest(
            dataset_id="electricity_region_data",
            stage="platinum",
            start_utc=request.start_utc,
            end_utc=request.end_utc,
        ),
        dataset,
    )

    assert captured["patterns"] == ["s3://gold/facts/region_demand_forecast_hourly/event_date=2026-03-09/*.parquet"]
    assert result.empty is True
    assert unsupported.columns.tolist() == ["grain_key", "period_start_utc", "respondent", "dimension_value"]
