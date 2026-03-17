from pathlib import Path

import pytest

from src.eia_api import build_eia_query_params, fetch_dataset_rows, resolve_api_window_bounds
from src.event_factory import build_event, build_event_id
from src.registry import load_dataset_registry, validate_dataset_registry


class FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self, responses: list[dict]):
        self.responses = responses
        self.calls: list[dict] = []

    def get(self, url, params, timeout):  # noqa: ANN001
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        payload = self.responses[len(self.calls) - 1]
        return FakeResponse(payload)


def test_load_dataset_registry_from_repo_root_relative_path() -> None:
    registry = load_dataset_registry(Path("src/dataset_registry.yml"))
    assert "electricity_region_data" in registry
    assert registry["electricity_region_data"]["topic"] == "eia_electricity_region_data"
    assert registry["electricity_region_data"]["backfill"]["step"] == "day"
    assert registry["electricity_region_data"]["default_facets"]["type"] == ["D", "DF"]


def test_validate_dataset_registry_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError, match="missing required fields"):
        validate_dataset_registry(
            {
                "electricity_region_data": {
                    "id": "electricity_region_data",
                    "route": "electricity/rto/region-data",
                }
            }
        )


def test_build_query_params_applies_facets() -> None:
    params = build_eia_query_params(
        api_key="k",
        start="2026-03-01T00",
        end="2026-03-01T03",
        offset=0,
        length=5000,
        dataset_config={"default_facets": {"type": ["D"]}, "data_columns": ["value"]},
        respondent="PJM",
    )
    assert params["facets[type][]"] == ["D"]
    assert params["facets[respondent][]"] == ["PJM"]
    assert params["data[0]"] == "value"
    assert params["sort[0][column]"] == "period"


def test_resolve_api_window_bounds_converts_exclusive_hourly_end() -> None:
    assert resolve_api_window_bounds("2026-03-15T14", "2026-03-15T15") == ("2026-03-15T14", "2026-03-15T14")


def test_resolve_api_window_bounds_extends_current_day_end_for_region_forecast() -> None:
    assert resolve_api_window_bounds(
        "2026-03-17T03",
        "2026-03-17T04",
        hourly_window_mode="current_day_end",
    ) == ("2026-03-17T03", "2026-03-17T23")


def test_resolve_api_window_bounds_converts_monthly_window() -> None:
    assert resolve_api_window_bounds("2026-02-01T00:00:00+00:00", "2026-03-01T00:00:00+00:00", "monthly") == (
        "2026-01-31",
        "2026-02-01",
    )


def test_resolve_api_window_bounds_rejects_empty_or_negative_window() -> None:
    with pytest.raises(ValueError, match="end must be greater than start"):
        resolve_api_window_bounds("2026-03-15T14", "2026-03-15T14")


def test_fetch_dataset_rows_paginates_until_total() -> None:
    session = FakeSession(
        responses=[
            {"response": {"total": "3", "data": [{"period": "2026-01-01T00"}, {"period": "2026-01-01T01"}]}},
            {"response": {"total": "3", "data": [{"period": "2026-01-01T02"}]}},
        ]
    )
    rows = fetch_dataset_rows(
        api_key="k",
        dataset_config={
            "route": "electricity/rto/region-data",
            "default_facets": {"type": ["D"]},
            "data_columns": ["value"],
        },
        start="2026-01-01T00",
        end="2026-01-01T02",
        page_size=2,
        max_pages=10,
        session=session,
    )
    assert len(rows) == 3
    assert len(session.calls) == 2
    assert session.calls[1]["params"]["offset"] == 2
    assert session.calls[0]["params"]["data[0]"] == "value"
    assert session.calls[0]["params"]["end"] == "2026-01-01T01"


def test_fetch_dataset_rows_stops_when_page_is_short_without_total() -> None:
    session = FakeSession(
        responses=[
            {"response": {"data": [{"period": "2026-01-01T00"}]}},
            {"response": {"data": [{"period": "2026-01-01T01"}]}},
        ]
    )
    rows = fetch_dataset_rows(
        api_key="k",
        dataset_config={"route": "electricity/rto/region-data"},
        start="2026-01-01T00",
        end="2026-01-01T02",
        page_size=2,
        max_pages=10,
        session=session,
    )
    assert rows == [{"period": "2026-01-01T00"}]
    assert len(session.calls) == 1


def test_build_event_envelope_uses_deterministic_event_id() -> None:
    row = {"period": "2026-03-01T05", "respondent": "PJM", "value": "100"}
    first_event = build_event(
        "electricity_region_data",
        "electricity/rto/region-data",
        row,
        ingestion_run_id="fetch_1",
        source_window_start="2026-03-01T00",
        source_window_end="2026-03-01T23",
    )
    second_event = build_event("electricity_region_data", "electricity/rto/region-data", row)
    assert first_event["dataset"] == "electricity_region_data"
    assert first_event["metadata"]["route"] == "electricity/rto/region-data"
    assert first_event["metadata"]["ingestion_run_id"] == "fetch_1"
    assert first_event["metadata"]["source_window_start"] == "2026-03-01T00"
    assert first_event["metadata"]["source_window_end"] == "2026-03-01T23"
    assert first_event["payload"]["value"] == "100"
    assert first_event["event_id"] == second_event["event_id"]
    assert first_event["event_id"] == build_event_id("electricity_region_data", "electricity/rto/region-data", row)


def test_build_event_rejects_invalid_period() -> None:
    with pytest.raises(ValueError):
        build_event("electricity_region_data", "electricity/rto/region-data", {"period": "not-a-date"})


def test_build_event_accepts_monthly_period() -> None:
    event = build_event(
        "electricity_power_operational_data",
        "electricity/electric-power-operational-data",
        {"period": "2026-02", "generation": "100"},
    )
    assert event["event_timestamp"] == "2026-02-01T00:00:00+00:00"
