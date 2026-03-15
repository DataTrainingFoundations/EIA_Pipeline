from __future__ import annotations

import argparse
import sys

import pytest

from src import fetch_eia


def test_parse_args_and_select_datasets(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fetch_eia.py",
            "--dataset",
            "electricity_region_data",
            "--start",
            "2026-03-10T00",
            "--end",
            "2026-03-10T02",
            "--respondent",
            "PJM",
            "--dry-run",
        ],
    )

    args = fetch_eia.parse_args()
    selected = fetch_eia._select_datasets(
        {
            "electricity_region_data": {"id": "electricity_region_data"},
            "electricity_fuel_type_data": {"id": "electricity_fuel_type_data"},
        },
        None,
    )

    assert args.dataset == "electricity_region_data"
    assert args.respondent == "PJM"
    assert args.dry_run is True
    assert "[start, end)" in fetch_eia.parse_args.__doc__
    assert [dataset["id"] for dataset in selected] == ["electricity_region_data", "electricity_fuel_type_data"]

    with pytest.raises(ValueError, match="Unknown dataset"):
        fetch_eia._select_datasets({"electricity_region_data": {"id": "electricity_region_data"}}, "missing")


def test_main_dry_run_builds_events_without_publishing(monkeypatch) -> None:  # noqa: ANN001
    args = argparse.Namespace(
        dataset="electricity_region_data",
        start="2026-03-10T00",
        end="2026-03-10T02",
        page_size=100,
        max_pages=3,
        respondent="PJM",
        dry_run=True,
    )
    built_events: list[dict[str, str]] = []
    published: list[dict[str, object]] = []

    monkeypatch.setattr(fetch_eia, "parse_args", lambda: args)
    monkeypatch.setenv("EIA_API_KEY", "test-key")
    monkeypatch.setattr(
        fetch_eia,
        "load_dataset_registry",
        lambda _path: {
            "electricity_region_data": {
                "id": "electricity_region_data",
                "route": "electricity/rto/region-data",
                "topic": "eia_region",
                "frequency": "hourly",
            }
        },
    )
    monkeypatch.setattr(
        fetch_eia,
        "fetch_dataset_rows",
        lambda **kwargs: [{"period": "2026-03-10T00", "respondent": kwargs["respondent"]}],
    )

    def fake_build_event(**kwargs):  # noqa: ANN001
        built_events.append(kwargs)
        return {"event_id": "evt-1", "payload": kwargs["row"]}

    monkeypatch.setattr(fetch_eia, "build_event", fake_build_event)
    monkeypatch.setattr(fetch_eia, "publish_events", lambda **kwargs: published.append(kwargs))

    fetch_eia.main()

    assert built_events[0]["dataset_id"] == "electricity_region_data"
    assert built_events[0]["source_window_start"] == "2026-03-10T00"
    assert built_events[0]["source_window_end"] == "2026-03-10T02"
    assert published == []


def test_main_publishes_events_and_surfaces_validation_failures(monkeypatch) -> None:  # noqa: ANN001
    args = argparse.Namespace(
        dataset="electricity_region_data",
        start="2026-03-10T00",
        end="2026-03-10T02",
        page_size=100,
        max_pages=3,
        respondent=None,
        dry_run=False,
    )
    published: list[dict[str, object]] = []
    dataset = {
        "id": "electricity_region_data",
        "route": "electricity/rto/region-data",
        "topic": "eia_region",
        "frequency": "hourly",
    }

    monkeypatch.setattr(fetch_eia, "parse_args", lambda: args)
    monkeypatch.setenv("EIA_API_KEY", "test-key")
    monkeypatch.setattr(fetch_eia, "load_dataset_registry", lambda _path: {"electricity_region_data": dataset})
    monkeypatch.setattr(fetch_eia, "fetch_dataset_rows", lambda **kwargs: [{"period": "2026-03-10T00"}])
    monkeypatch.setattr(fetch_eia, "build_event", lambda **kwargs: {"event_id": "evt-1", "payload": kwargs["row"]})
    monkeypatch.setattr(fetch_eia, "publish_events", lambda **kwargs: published.append(kwargs) or 1)

    fetch_eia.main()

    assert published[0]["topic"] == "eia_region"
    assert published[0]["events"] == [{"event_id": "evt-1", "payload": {"period": "2026-03-10T00"}}]

    monkeypatch.setattr(fetch_eia, "build_event", lambda **kwargs: (_ for _ in ()).throw(ValueError("bad row")))  # noqa: ARG005
    with pytest.raises(RuntimeError, match="Validation failed"):
        fetch_eia.main()


def test_main_requires_api_key(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        fetch_eia,
        "parse_args",
        lambda: argparse.Namespace(
            dataset=None,
            start="2026-03-10T00",
            end="2026-03-10T02",
            page_size=100,
            max_pages=3,
            respondent=None,
            dry_run=False,
        ),
    )
    monkeypatch.delenv("EIA_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="EIA_API_KEY is required"):
        fetch_eia.main()
