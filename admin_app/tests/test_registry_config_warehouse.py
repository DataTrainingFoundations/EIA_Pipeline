from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import config
import registry
from config import AppConfig
from models import ComparisonRequest
from warehouse_store import WarehouseStore


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: ANN001
        return cls(2026, 3, 11, 15, 42, tzinfo=tz or timezone.utc)


class FakeCursor:
    def __init__(self, rows=None, description=None) -> None:  # noqa: ANN001
        self.rows = rows or []
        self.description = description
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query, params=None) -> None:  # noqa: ANN001
        self.executed.append((str(query), tuple(params or ())))

    def fetchall(self):  # noqa: ANN201
        return self.rows

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        return None


def _app_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="platform",
        postgres_user="platform",
        postgres_password="platform",
        minio_endpoint="minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
        eia_api_key="secret",
        app_timezone="UTC",
        api_cache_ttl_seconds=300,
        query_timeout_seconds=60,
        default_days=7,
        airflow_log_dir=tmp_path,
        registry_path=tmp_path / "dataset_registry.yml",
    )


def test_load_config_and_default_window_respect_environment(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    monkeypatch.setenv("POSTGRES_HOST", "warehouse")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio.internal:9000")
    monkeypatch.setenv("ADMIN_APP_DEFAULT_DAYS", "3")
    monkeypatch.setenv("ADMIN_APP_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(config, "datetime", FixedDatetime)

    app_config = config.load_config()
    start_utc, end_utc = app_config.default_window()

    assert app_config.postgres_host == "warehouse"
    assert app_config.postgres_port == 6543
    assert app_config.minio_endpoint_with_scheme == "http://minio.internal:9000"
    assert start_utc.isoformat() == "2026-03-08T15:00:00+00:00"
    assert end_utc.isoformat() == "2026-03-11T15:00:00+00:00"


def test_load_registry_fills_missing_gold_path_and_lists_stage_options(tmp_path: Path) -> None:
    registry_path = tmp_path / "dataset_registry.yml"
    registry_path.write_text(
        """
datasets:
  - id: electricity_region_data
    route: electricity/rto/region-data
    topic: eia_electricity_region_data
    bronze_output_path: s3a://bronze/region
    silver_output_path: s3a://silver/region
    gold_output_path: s3a://gold/region
  - id: electricity_fuel_type_data
    route: electricity/rto/fuel-type-data
    topic: eia_electricity_fuel_type_data
    bronze_output_path: s3a://bronze/fuel
    silver_output_path: s3a://silver/fuel
""".strip(),
        encoding="utf-8",
    )
    registry.load_registry.cache_clear()

    loaded = registry.load_registry(str(registry_path))

    assert loaded["electricity_fuel_type_data"].gold_output_path == registry.FUEL_GOLD_PATH
    assert registry.dataset_options_for_stage("raw") == registry.RAW_DATASET_IDS
    assert registry.dataset_options_for_stage("platinum") == list(registry.PLATINUM_TARGETS)


def test_warehouse_read_sql_and_platform_counts(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    store = WarehouseStore(_app_config(tmp_path))
    cursor = FakeCursor(rows=[("platinum.region_demand_daily", 12)], description=[("target_name",), ("row_count",)])
    monkeypatch.setattr(store, "_connection", lambda: FakeConnection(cursor))

    df = store.read_sql("select * from counts where id = %s", [9])
    captured: dict[str, str] = {}

    def fake_read_sql(query, params=None):  # noqa: ANN001, ARG001
        captured["query"] = query
        return pd.DataFrame()

    monkeypatch.setattr(store, "read_sql", fake_read_sql)
    store.get_platform_counts()

    assert df.to_dict("records") == [{"target_name": "platinum.region_demand_daily", "row_count": 12}]
    assert cursor.executed[0][1] == (9,)
    assert "platinum.grid_operations_hourly" in captured["query"]


def test_fetch_platinum_keys_builds_grain_keys_and_filters_respondent(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    store = WarehouseStore(_app_config(tmp_path))
    request = ComparisonRequest(
        dataset_id="platinum.grid_operations_hourly",
        stage="platinum",
        start_utc=datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
        end_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
        respondent_filter="PJM",
    )
    captured: dict[str, object] = {}

    def fake_read_sql(query, params=None):  # noqa: ANN001
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame(
            [
                {
                    "period_start_utc": "2026-03-10T00:00:00+00:00",
                    "respondent": "PJM",
                    "dimension_value": "",
                }
            ]
        )

    monkeypatch.setattr(store, "read_sql", fake_read_sql)

    result = store.fetch_platinum_keys(request)
    empty = store.fetch_platinum_keys(
        ComparisonRequest(
            dataset_id="unsupported.table",
            stage="platinum",
            start_utc=request.start_utc,
            end_utc=request.end_utc,
        )
    )

    assert "platinum.grid_operations_hourly" in captured["query"]
    assert captured["params"] == [request.start_utc, request.end_utc, "PJM"]
    assert result.iloc[0]["grain_key"] == "2026-03-10T00:00:00Z|PJM|"
    assert empty.empty is True
