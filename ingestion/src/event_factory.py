"""Kafka event construction helpers for ingestion.

This module converts raw EIA rows into the event envelope that Bronze expects.
`fetch_eia.py` uses these helpers immediately after API pagination.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def _parse_event_timestamp(period_value: Any) -> str:
    """Normalize an EIA period value into an ISO-8601 UTC timestamp string.

    Args:
        period_value: Raw `period` value from the EIA payload.

    Returns:
        A UTC ISO-8601 timestamp string.

    Raises:
        ValueError: If the period cannot be parsed.
    """

    if not isinstance(period_value, str):
        raise ValueError(f"Invalid period value: {period_value!r}")

    candidate_formats = (
        None,
        "%Y-%m-%dT%H",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
    )
    normalized_value = period_value.replace("Z", "+00:00")
    for candidate_format in candidate_formats:
        try:
            if candidate_format is None:
                parsed = datetime.fromisoformat(normalized_value)
            else:
                parsed = datetime.strptime(period_value, candidate_format)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue

    raise ValueError(f"Invalid period value: {period_value!r}")


def build_event_id(dataset_id: str, route: str, row: dict[str, Any]) -> str:
    """Build a deterministic event id from dataset metadata and row payload."""

    payload = json.dumps(
        {"dataset": dataset_id, "route": route, "row": row},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_event(
    dataset_id: str,
    route: str,
    row: dict[str, Any],
    frequency: str = "hourly",
    *,
    ingestion_run_id: str | None = None,
    source_window_start: str | None = None,
    source_window_end: str | None = None,
) -> dict[str, Any]:
    """Build the Bronze-ready Kafka event envelope for one EIA row.

    Args:
        dataset_id: Registry dataset id.
        route: EIA route used to fetch the row.
        row: Raw EIA row payload.
        frequency: Source frequency recorded in event metadata.
        ingestion_run_id: Unique identifier for the current CLI run.
        source_window_start: Requested source-window start.
        source_window_end: Requested source-window end.

    Returns:
        A dictionary matching the Bronze Kafka event contract.

    Raises:
        ValueError: If the row contains an invalid period value.
    """

    return {
        "event_id": build_event_id(dataset_id, route, row),
        "dataset": dataset_id,
        "source": "eia_api_v2",
        "event_timestamp": _parse_event_timestamp(row.get("period")),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "frequency": frequency,
            "route": route,
            "respondent": row.get("respondent"),
            "ingestion_run_id": ingestion_run_id,
            "source_window_start": source_window_start,
            "source_window_end": source_window_end,
        },
        "payload": row,
    }
