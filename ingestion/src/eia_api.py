"""EIA API request helpers for the ingestion CLI.

This module is responsible for building query parameters and paging through the
EIA API. `fetch_eia.py` calls these helpers and then turns the returned rows
into Kafka events.
"""

from __future__ import annotations

from typing import Any

import requests

EIA_API_BASE_URL = "https://api.eia.gov/v2"


def _apply_facets(params: dict[str, Any], facets: dict[str, list[str]] | None) -> None:
    """Add configured EIA facet filters to the request parameter dictionary."""

    if not facets:
        return
    for facet_name, facet_values in facets.items():
        params[f"facets[{facet_name}][]"] = facet_values


def build_eia_query_params(
    api_key: str,
    start: str,
    end: str,
    offset: int,
    length: int,
    default_facets: dict[str, list[str]] | None = None,
    data_columns: list[str] | None = None,
    respondent: str | None = None,
) -> dict[str, Any]:
    """Build a single paginated EIA API request payload.

    Args:
        api_key: API key used to call EIA v2.
        start: Inclusive start of the source window.
        end: Inclusive end of the source window.
        offset: Row offset for pagination.
        length: Maximum rows to request for this page.
        default_facets: Dataset-specific filters from the registry.
        data_columns: Value columns to request from EIA.
        respondent: Optional single-respondent filter for debugging or replay.

    Returns:
        A request parameter dictionary ready to pass to `requests`.
    """

    params: dict[str, Any] = {
        "api_key": api_key,
        "frequency": "hourly",
        "start": start,
        "end": end,
        "offset": offset,
        "length": length,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    _apply_facets(params, default_facets)
    for index, column in enumerate(data_columns or ["value"]):
        params[f"data[{index}]"] = column
    if respondent:
        params["facets[respondent][]"] = [respondent]
    return params


def fetch_dataset_rows(
    api_key: str,
    dataset_config: dict[str, Any],
    start: str,
    end: str,
    *,
    page_size: int = 5000,
    max_pages: int = 500,
    timeout_seconds: int = 30,
    respondent: str | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch every available row for one dataset and source window.

    Args:
        api_key: EIA API key.
        dataset_config: One dataset entry from `dataset_registry.yml`.
        start: Inclusive source-window start.
        end: Inclusive source-window end.
        page_size: Max rows per API page.
        max_pages: Hard stop to prevent unbounded paging.
        timeout_seconds: Per-request timeout.
        respondent: Optional respondent filter.
        session: Optional injected requests session for tests.

    Returns:
        A list of raw EIA API rows for the requested dataset window.

    Raises:
        requests.HTTPError: If EIA returns a non-success response.
    """

    route = dataset_config["route"]
    query_url = f"{EIA_API_BASE_URL}/{route}/data/"
    offset = 0
    all_rows: list[dict[str, Any]] = []
    page_count = 0
    current_total: int | None = None
    session = session or requests.Session()

    while page_count < max_pages:
        params = build_eia_query_params(
            api_key=api_key,
            start=start,
            end=end,
            offset=offset,
            length=page_size,
            default_facets=dataset_config.get("default_facets"),
            data_columns=dataset_config.get("data_columns"),
            respondent=respondent,
        )
        response = session.get(query_url, params=params, timeout=timeout_seconds)
        response.raise_for_status()
        body = response.json().get("response", {})
        rows = body.get("data", [])
        total = body.get("total")
        if total is not None:
            current_total = int(total)
        if not rows:
            break

        all_rows.extend(rows)
        offset += len(rows)
        page_count += 1

        if current_total is not None and offset >= current_total:
            break
        if len(rows) < page_size and current_total is None:
            break

    return all_rows
