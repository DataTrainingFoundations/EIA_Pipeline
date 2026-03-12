from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import AppConfig
from models import ComparisonRequest
from parquet_store import ParquetStore


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


def test_query_stage_keys_from_local_bronze_and_silver_files(tmp_path: Path) -> None:
    bronze_file = tmp_path / "bronze.parquet"
    silver_file = tmp_path / "silver.parquet"

    bronze_df = pd.DataFrame(
        [
            {
                "event_ts": pd.Timestamp("2026-03-09T00:00:00Z"),
                "dataset_partition": "electricity_region_data",
                "raw_json": json.dumps(
                    {"payload": {"respondent": "PJM", "type": "D"}},
                    separators=(",", ":"),
                ),
            }
        ]
    )
    bronze_df.to_parquet(bronze_file, index=False)

    silver_df = pd.DataFrame(
        [
            {
                "period": pd.Timestamp("2026-03-09T00:00:00Z"),
                "respondent": "PJM",
                "type": "D",
            }
        ]
    )
    silver_df.to_parquet(silver_file, index=False)

    store = ParquetStore(_config(tmp_path))
    bronze_request = ComparisonRequest(
        dataset_id="electricity_region_data",
        stage="bronze",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )
    bronze_keys = store.query_stage_keys_from_files(bronze_request, [str(bronze_file)])
    assert len(bronze_keys) == 1
    assert bronze_keys.iloc[0]["dimension_value"] == "D"

    silver_request = ComparisonRequest(
        dataset_id="electricity_region_data",
        stage="silver",
        start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
    )
    silver_keys = store.query_stage_keys_from_files(silver_request, [str(silver_file)])
    assert len(silver_keys) == 1
    assert silver_keys.iloc[0]["respondent"] == "PJM"
