from __future__ import annotations

import time
from typing import Any

import requests

from pipeline.core.settings import EiaSettings


def _append_query_value(query: dict[str, Any], key: str, value: Any) -> None:
    if isinstance(value, (list, tuple)):
        for item in value:
            _append_query_value(query, key, item)
        return
    query.setdefault(key, []).append(value)


def fetch_page(
    settings: EiaSettings,
    route: str,
    params: dict[str, Any],
    start: str,
    end: str,
    offset: int = 0,
) -> list[dict]:
    query: dict[str, Any] = {
        "api_key": settings.api_key,
        "offset": offset,
        "start": start,
        "end": end,
    }
    for key, value in params.items():
        if key == "data":
            for item in value:
                _append_query_value(query, "data[]", item)
        elif key == "facets":
            for facet_key, facet_values in value.items():
                for facet_value in facet_values:
                    _append_query_value(query, f"facets[{facet_key}][]", facet_value)
        elif key == "sort":
            for index, sort_item in enumerate(value):
                query[f"sort[{index}][column]"] = sort_item["column"]
                query[f"sort[{index}][direction]"] = sort_item["direction"]
        else:
            query[key] = value

    url = f"{settings.base_url}/{route}/data/"
    for attempt in range(1, settings.max_retries + 1):
        try:
            response = requests.get(url, params=query, timeout=30)
            response.raise_for_status()
            payload = response.json()
            return payload.get("response", {}).get("data", [])
        except requests.RequestException:
            if attempt == settings.max_retries:
                raise
            time.sleep(settings.retry_backoff_seconds**attempt)
    return []


def fetch_all_pages(settings: EiaSettings, dataset: dict, *, start: str, end: str) -> list[dict]:
    params = dataset.get("params", {})
    page_size = int(params.get("length", 500))
    offset = 0
    records: list[dict] = []
    while True:
        page = fetch_page(settings, dataset["eia_route"], params, start, end, offset)
        if not page:
            break
        records.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return records
