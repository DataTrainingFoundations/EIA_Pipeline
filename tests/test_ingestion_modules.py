from __future__ import annotations

from pipeline.core.settings import EiaSettings
from pipeline.ingestion import client, raw_ingest


def test_append_query_value_and_fetch_page(monkeypatch) -> None:
    query = {}
    client._append_query_value(query, "data[]", ["value", "other"])
    assert query == {"data[]": ["value", "other"]}

    captured = {}

    class Response:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"response": {"data": [{"x": 1}]}}

    monkeypatch.setattr(
        client.requests,
        "get",
        lambda url, params, timeout: captured.update({"url": url, "params": params, "timeout": timeout}) or Response(),
    )

    settings = EiaSettings(api_key="abc", max_retries=1)
    page = client.fetch_page(
        settings,
        "electricity/rto/region-data",
        {
            "data": ["value"],
            "facets": {"respondent": ["MISO"]},
            "sort": [{"column": "period", "direction": "desc"}],
            "length": 500,
        },
        "2026-04-09T00",
        "2026-04-09T23",
        500,
    )

    assert page == [{"x": 1}]
    assert captured["params"]["data[]"] == ["value"]
    assert captured["params"]["facets[respondent][]"] == ["MISO"]
    assert captured["params"]["sort[0][column]"] == "period"


def test_fetch_page_retries_and_fetch_all_pages(monkeypatch) -> None:
    attempts = {"count": 0}

    def flaky_get(*args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise client.requests.RequestException("retry")

        class Response:
            @staticmethod
            def raise_for_status():
                return None

            @staticmethod
            def json():
                return {"response": {"data": [{"page": attempts["count"]}]}}

        return Response()

    monkeypatch.setattr(client.requests, "get", flaky_get)
    monkeypatch.setattr(client.time, "sleep", lambda *_args, **_kwargs: None)
    settings = EiaSettings(api_key="abc", max_retries=2, retry_backoff_seconds=1)
    assert client.fetch_page(settings, "route", {}, "s", "e") == [{"page": 2}]

    pages = [[{"id": 1}, {"id": 2}], [{"id": 3}], []]
    monkeypatch.setattr(client, "fetch_page", lambda *args, **kwargs: pages.pop(0))
    dataset = {"id": "ds", "eia_route": "route", "params": {"length": 2}}
    assert client.fetch_all_pages(settings, dataset, start="s", end="e") == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_raw_ingest_dataset_selection_chunking_and_run(monkeypatch) -> None:
    dataset = {
        "id": "electricity_generation_hourly",
        "frequency": "hourly",
        "snowflake_table": "RAW.TABLE",
        "params": {"facets": {"respondent": ["A", "B", "C"]}},
        "facet_chunk_key": "respondent",
        "facet_chunk_size": 2,
    }
    monkeypatch.setattr(raw_ingest, "iter_ingest_datasets", lambda: [dataset])
    monkeypatch.setattr(raw_ingest, "get_dataset", lambda dataset_id: dataset)
    assert raw_ingest.select_datasets() == [dataset]
    assert raw_ingest.select_datasets("electricity_generation_hourly") == [dataset]

    enriched = raw_ingest.enrich_records([{"x": 1}], "ds")
    assert enriched[0]["_dataset_id"] == "ds"
    assert "_fetched_at" in enriched[0]

    chunked = raw_ingest._chunked_datasets(dataset)
    assert len(chunked) == 2
    assert chunked[0]["params"]["facets"]["respondent"] == ["A", "B"]

    monkeypatch.setattr(raw_ingest, "resolve_ingest_windows", lambda *args, **kwargs: [("2026-04-09T00", "2026-04-09T23")])
    monkeypatch.setattr(raw_ingest, "fetch_all_pages", lambda *args, **kwargs: [{"x": 1}, {"x": 2}])
    monkeypatch.setattr(raw_ingest, "write_raw_records", lambda *args, **kwargs: len(args[2]))
    total = raw_ingest.ingest_dataset(None, object(), dataset, start_date="2026-04-09", end_date="2026-04-09")
    assert total == 4

    monkeypatch.setattr(raw_ingest, "ingest_dataset", lambda *args, **kwargs: 3)
    assert raw_ingest.run_ingestion(None, object(), target_dataset_id="electricity_generation_hourly") == 3
