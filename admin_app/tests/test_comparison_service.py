from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from comparison_service import run_comparison
from config import AppConfig
from models import ComparisonRequest


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


def test_run_comparison_reports_missing_and_extra(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    expected_df = pd.DataFrame(
        [
            {"grain_key": "k1", "period_start_utc": pd.Timestamp("2026-03-09T00:00:00Z"), "respondent": "PJM", "dimension_value": "D"},
            {"grain_key": "k2", "period_start_utc": pd.Timestamp("2026-03-09T01:00:00Z"), "respondent": "PJM", "dimension_value": "D"},
        ]
    )
    actual_df = pd.DataFrame(
        [
            {"grain_key": "k1", "period_start_utc": pd.Timestamp("2026-03-09T00:00:00Z"), "respondent": "PJM", "dimension_value": "D"},
            {"grain_key": "k3", "period_start_utc": pd.Timestamp("2026-03-09T02:00:00Z"), "respondent": "PJM", "dimension_value": "D"},
        ]
    )

    monkeypatch.setattr(
        "comparison_service.load_registry",
        lambda *args, **kwargs: {"electricity_region_data": object()},
    )
    monkeypatch.setattr("comparison_service.build_expected_stage_keys", lambda *args, **kwargs: expected_df)

    class FakeParquetStore:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def fetch_stage_keys(self, *_args, **_kwargs) -> pd.DataFrame:
            return actual_df

    class FakeWarehouseStore:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    monkeypatch.setattr("comparison_service.ParquetStore", FakeParquetStore)
    monkeypatch.setattr("comparison_service.WarehouseStore", FakeWarehouseStore)

    result = run_comparison(
        _config(tmp_path),
        ComparisonRequest(
            dataset_id="electricity_region_data",
            stage="bronze",
            start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
        ),
    )
    assert result.summary.missing_count == 1
    assert result.summary.extra_count == 1
    assert result.summary.status == "mismatch"


def test_run_comparison_excludes_end_boundary_timestamp(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    expected_df = pd.DataFrame(
        [
            {"grain_key": "k1", "period_start_utc": pd.Timestamp("2026-03-09T23:00:00Z"), "respondent": "PJM", "dimension_value": "D"},
            {"grain_key": "k2", "period_start_utc": pd.Timestamp("2026-03-10T00:00:00Z"), "respondent": "PJM", "dimension_value": "D"},
        ]
    )

    monkeypatch.setattr(
        "comparison_service.load_registry",
        lambda *args, **kwargs: {"electricity_region_data": object()},
    )
    monkeypatch.setattr("comparison_service.build_expected_stage_keys", lambda *args, **kwargs: expected_df)

    class FakeParquetStore:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def fetch_stage_keys(self, *_args, **_kwargs) -> pd.DataFrame:
            return pd.DataFrame(columns=["grain_key", "period_start_utc", "respondent", "dimension_value"])

    class FakeWarehouseStore:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    monkeypatch.setattr("comparison_service.ParquetStore", FakeParquetStore)
    monkeypatch.setattr("comparison_service.WarehouseStore", FakeWarehouseStore)

    result = run_comparison(
        _config(tmp_path),
        ComparisonRequest(
            dataset_id="electricity_region_data",
            stage="bronze",
            start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
        ),
    )

    assert result.summary.expected_count == 1
    assert result.expected_df["grain_key"].tolist() == ["k1"]
