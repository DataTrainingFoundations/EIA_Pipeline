from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from config import AppConfig
from eia_client import (
    _build_fuel_gold,
    _build_fuel_silver,
    _build_power_gold,
    _build_power_silver,
    _build_region_gold,
    _build_region_silver,
    _parse_period,
    build_expected_stage_keys,
    fetch_dataset_rows,
)
from models import ComparisonRequest
from registry import DatasetRegistryEntry


def _config(api_key: str = "test") -> AppConfig:
    return AppConfig(
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="platform",
        postgres_user="platform",
        postgres_password="platform",
        minio_endpoint="minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
        eia_api_key=api_key,
        app_timezone="UTC",
        api_cache_ttl_seconds=300,
        query_timeout_seconds=60,
        default_days=7,
        airflow_log_dir=__import__("pathlib").Path("."),
        registry_path=__import__("pathlib").Path("ingestion/src/dataset_registry.yml"),
    )


REGION_DATASET = DatasetRegistryEntry(
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

FUEL_DATASET = DatasetRegistryEntry(
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

POWER_DATASET = DatasetRegistryEntry(
    dataset_id="electricity_power_operational_data",
    route="electricity/electric-power-operational-data",
    topic="topic",
    frequency="monthly",
    data_columns=("ash-content", "consumption-for-eg", "generation", "heat-content"),
    default_facets={"location": ["US"]},
    bronze_output_path="s3a://bronze/electricity_power_operational_data",
    silver_output_path="s3a://silver/electric_power_operational_data",
    gold_output_path="s3a://gold/facts/electric_power_operations_monthly",
)


def test_fetch_dataset_rows_requires_api_key() -> None:
    with pytest.raises(RuntimeError, match="EIA_API_KEY"):
        fetch_dataset_rows(
            _config(api_key=""),
            REGION_DATASET,
            ComparisonRequest(
                dataset_id="electricity_region_data",
                stage="bronze",
                start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
                end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
            ),
        )


def test_fetch_dataset_rows_stops_on_total_and_applies_params(monkeypatch) -> None:  # noqa: ANN001
    calls: list[dict] = []

    class FakeResponse:
        def __init__(self, payload: dict) -> None:
            self.payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"response": self.payload}

    class FakeSession:
        def __init__(self) -> None:
            self.responses = [
                FakeResponse({"data": [{"period": "2026-03-09T00"}], "total": 2}),
                FakeResponse({"data": [{"period": "2026-03-09T01"}], "total": 2}),
            ]

        def get(self, _url: str, *, params: dict, timeout: int):  # noqa: ANN001
            calls.append({"params": params, "timeout": timeout})
            return self.responses.pop(0)

    monkeypatch.setattr("eia_client.requests.Session", FakeSession)

    rows = fetch_dataset_rows(
        _config(),
        REGION_DATASET,
        ComparisonRequest(
            dataset_id="electricity_region_data",
            stage="bronze",
            start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
            respondent_filter="PJM",
        ),
        page_size=1,
    )

    assert len(rows) == 2
    assert calls[0]["params"]["facets[type][]"] == ["D", "DF"]
    assert calls[0]["params"]["facets[respondent][]"] == ["PJM"]
    assert calls[0]["params"]["data[0]"] == "value"


def test_fetch_dataset_rows_stops_on_short_page_without_total(monkeypatch) -> None:  # noqa: ANN001
    class FakeResponse:
        def __init__(self, rows: list[dict]) -> None:
            self.rows = rows

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"response": {"data": self.rows}}

    class FakeSession:
        def get(self, _url: str, *, params: dict, timeout: int):  # noqa: ANN001, ARG002
            return FakeResponse([{"period": "2026-03-09T00"}])

    monkeypatch.setattr("eia_client.requests.Session", FakeSession)

    rows = fetch_dataset_rows(
        _config(),
        REGION_DATASET,
        ComparisonRequest(
            dataset_id="electricity_region_data",
            stage="bronze",
            start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
        ),
        page_size=5,
    )

    assert rows == [{"period": "2026-03-09T00"}]


def test_parse_period_handles_non_strings_and_z_suffix() -> None:
    assert pd.isna(_parse_period(123))
    assert _parse_period("2026-03-09T00:00:00Z") == pd.Timestamp("2026-03-09T00:00:00Z")
    assert pd.isna(_parse_period("not-a-date"))


def test_region_silver_and_gold_filter_and_latest_logic() -> None:
    df = pd.DataFrame(
        [
            {"event_id": "evt-1", "period_start_utc": "2026-03-09T00:00:00Z", "respondent": "PJM", "respondent_name": "PJM", "type": "D", "value": 100.0},
            {"event_id": "evt-1", "period_start_utc": "2026-03-09T00:00:00Z", "respondent": "PJM", "respondent_name": "PJM", "type": "D", "value": 100.0},
            {"event_id": "evt-2", "period_start_utc": "2026-03-09T00:00:00Z", "respondent": "PJM", "respondent_name": None, "type": "DF", "value": 90.0},
            {"event_id": "evt-3", "period_start_utc": "2026-03-09T00:00:00Z", "respondent": "PJM", "respondent_name": "PJM Updated", "type": "DF", "value": 95.0},
        ]
    )

    silver_df = _build_region_silver(df)
    gold_df = _build_region_gold(df)

    assert silver_df["event_id"].tolist() == ["evt-1", "evt-2", "evt-3"]
    assert gold_df.iloc[0]["respondent_name"] == "PJM"
    assert gold_df.iloc[0]["day_ahead_forecast_mwh"] == 95.0


def test_fuel_silver_and_gold_filter_and_clip_negative_values() -> None:
    df = pd.DataFrame(
        [
            {"event_id": "evt-1", "period_start_utc": "2026-03-09T00:00:00Z", "respondent": "PJM", "respondent_name": "PJM", "fueltype": "NG", "value": -10.0},
            {"event_id": "evt-2", "period_start_utc": "2026-03-09T00:00:00Z", "respondent": "PJM", "respondent_name": "PJM", "fueltype": None, "value": 12.0},
        ]
    )

    silver_df = _build_fuel_silver(df)
    gold_df = _build_fuel_gold(df)

    assert silver_df["event_id"].tolist() == ["evt-1"]
    assert gold_df.iloc[0]["generation_mwh"] == 0.0


def test_power_silver_and_gold_filter_and_clip_negative_values() -> None:
    df = pd.DataFrame(
        [
            {
                "event_id": "evt-1",
                "period_start_utc": "2026-01-01T00:00:00Z",
                "location": "US",
                "location_name": "U.S. Total",
                "sector_id": "1",
                "sector_name": "Electric Utility",
                "fueltype_id": "COL",
                "fueltype_name": "Coal",
                "ash_content_pct": -1.0,
                "consumption_for_eg_thousand_units": -2.0,
                "generation_thousand_mwh": -3.0,
                "heat_content_btu_per_unit": -4.0,
            },
            {
                "event_id": "evt-2",
                "period_start_utc": "2026-01-01T00:00:00Z",
                "location": "US",
                "location_name": None,
                "sector_id": "1",
                "sector_name": "Electric Utility",
                "fueltype_id": "NG",
                "fueltype_name": "Natural Gas",
                "ash_content_pct": 1.0,
                "consumption_for_eg_thousand_units": 2.0,
                "generation_thousand_mwh": 3.0,
                "heat_content_btu_per_unit": 4.0,
            },
        ]
    )

    silver_df = _build_power_silver(df)
    gold_df = _build_power_gold(df)

    assert silver_df["event_id"].tolist() == ["evt-1"]
    assert gold_df.iloc[0]["generation_mwh"] == 0.0
    assert gold_df.iloc[0]["heat_content_btu_per_unit"] == 0.0


def test_build_expected_region_gold_and_platinum_keys(monkeypatch) -> None:  # noqa: ANN001
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
    gold_df = build_expected_stage_keys(_config(), gold_request, REGION_DATASET)
    assert len(gold_df) == 2

    for dataset_id in [
        "platinum.region_demand_daily",
        "platinum.grid_operations_hourly",
        "platinum.resource_planning_daily",
    ]:
        platinum_request = ComparisonRequest(
            dataset_id=dataset_id,
            stage="platinum",
            start_utc=datetime(2026, 3, 9, tzinfo=timezone.utc),
            end_utc=datetime(2026, 3, 10, tzinfo=timezone.utc),
        )
        platinum_df = build_expected_stage_keys(_config(), platinum_request, REGION_DATASET)
        assert platinum_df.iloc[0]["respondent"] == "PJM"


def test_build_expected_fuel_gold_keys(monkeypatch) -> None:  # noqa: ANN001
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
    gold_df = build_expected_stage_keys(_config(), request, FUEL_DATASET)
    assert set(gold_df["dimension_value"]) == {"NG", "SUN"}


def test_build_expected_power_gold_and_platinum_keys(monkeypatch) -> None:  # noqa: ANN001
    rows = [
        {
            "period": "2026-01",
            "location": "US",
            "stateDescription": "U.S. Total",
            "sectorid": "1",
            "sectorDescription": "Electric Utility",
            "fueltypeid": "COL",
            "fuelTypeDescription": "Coal",
            "ash-content": "7.5",
            "consumption-for-eg": "12.3",
            "generation": "45.6",
            "heat-content": "20.1",
        },
        {
            "period": "2026-01",
            "location": "US",
            "stateDescription": "U.S. Total",
            "sectorid": "1",
            "sectorDescription": "Electric Utility",
            "fueltypeid": "NG",
            "fuelTypeDescription": "Natural Gas",
            "ash-content": None,
            "consumption-for-eg": "8.1",
            "generation": "22.2",
            "heat-content": "1.02",
        },
    ]
    monkeypatch.setattr("eia_client.fetch_dataset_rows", lambda *args, **kwargs: rows)

    gold_request = ComparisonRequest(
        dataset_id="electricity_power_operational_data",
        stage="gold",
        start_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_utc=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )
    gold_df = build_expected_stage_keys(_config(), gold_request, POWER_DATASET)
    assert set(gold_df["dimension_value"]) == {"1|COL", "1|NG"}

    platinum_request = ComparisonRequest(
        dataset_id="platinum.electric_power_operations_monthly",
        stage="platinum",
        start_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_utc=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )
    platinum_df = build_expected_stage_keys(_config(), platinum_request, POWER_DATASET)
    assert set(platinum_df["respondent"]) == {"US"}
