"""Dataset registry helpers for ingestion.

This module loads the dataset configuration that defines which EIA endpoints
the ingestion CLI can call. `fetch_eia.py` uses these helpers to discover
datasets, Kafka topics, and backfill settings before any API requests are made.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

SUPPORTED_BACKFILL_STEPS = {"hour", "day", "month", "year"}


def load_dataset_registry(path: Path) -> dict[str, dict[str, Any]]:
    """Load and validate the ingestion dataset registry.

    Args:
        path: Absolute or relative path to `dataset_registry.yml`.

    Returns:
        A dictionary keyed by dataset id so callers can quickly look up config.

    Side effects:
        Reads YAML from disk.

    Raises:
        ValueError: If a dataset is missing required fields or uses an
            unsupported backfill step.
    """

    if not path.is_absolute():
        candidate_paths = [
            Path.cwd() / path,
            Path(__file__).resolve().parent / path.name,
            Path(__file__).resolve().parent.parent / path,
        ]
        for candidate_path in candidate_paths:
            if candidate_path.exists():
                path = candidate_path
                break

    with path.open("r", encoding="utf-8") as registry_file:
        payload = yaml.safe_load(registry_file) or {}

    datasets = payload.get("datasets", [])
    registry = {dataset["id"]: dataset for dataset in datasets}
    validate_dataset_registry(registry)
    return registry


def validate_dataset_registry(registry: dict[str, dict[str, Any]]) -> None:
    """Validate the minimum fields and backfill settings for each dataset.

    Args:
        registry: Dataset config keyed by dataset id.

    Raises:
        ValueError: If any dataset is incomplete or has invalid backfill config.
    """

    required_fields = {"id", "route", "topic", "frequency"}
    for dataset_id, dataset in registry.items():
        missing_fields = sorted(required_fields - dataset.keys())
        if missing_fields:
            missing_fields_text = ", ".join(missing_fields)
            raise ValueError(f"Dataset '{dataset_id}' is missing required fields: {missing_fields_text}")

        backfill_config = dataset.get("backfill") or {}
        step = backfill_config.get("step")
        if step and step not in SUPPORTED_BACKFILL_STEPS:
            supported_steps = ", ".join(sorted(SUPPORTED_BACKFILL_STEPS))
            raise ValueError(
                f"Dataset '{dataset_id}' uses unsupported backfill step '{step}'. Supported: {supported_steps}"
            )
