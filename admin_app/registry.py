from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


RAW_DATASET_IDS = [
    "electricity_region_data",
    "electricity_fuel_type_data",
    "electricity_power_operational_data",
]
PLATINUM_TARGETS = {
    "platinum.region_demand_daily": "Region Demand Daily",
    "platinum.grid_operations_hourly": "Grid Operations Hourly",
    "platinum.resource_planning_daily": "Resource Planning Daily",
    "platinum.electric_power_operations_monthly": "Electric Power Operations Monthly",
}
FUEL_GOLD_PATH = "s3a://gold/facts/fuel_generation_hourly"


@dataclass(frozen=True)
class DatasetRegistryEntry:
    dataset_id: str
    route: str
    topic: str
    frequency: str
    data_columns: tuple[str, ...]
    default_facets: dict[str, list[str]]
    bronze_output_path: str
    silver_output_path: str
    gold_output_path: str | None


def _coerce_entry(payload: dict[str, Any]) -> DatasetRegistryEntry:
    return DatasetRegistryEntry(
        dataset_id=payload["id"],
        route=payload["route"],
        topic=payload["topic"],
        frequency=payload.get("frequency", "hourly"),
        data_columns=tuple(payload.get("data_columns", ["value"])),
        default_facets=payload.get("default_facets") or {},
        bronze_output_path=payload["bronze_output_path"],
        silver_output_path=payload["silver_output_path"],
        gold_output_path=payload.get("gold_output_path"),
    )


@lru_cache(maxsize=1)
def load_registry(registry_path: str) -> dict[str, DatasetRegistryEntry]:
    path = Path(registry_path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    registry = {
        entry["id"]: _coerce_entry(entry)
        for entry in payload.get("datasets", [])
    }
    if "electricity_fuel_type_data" in registry and registry["electricity_fuel_type_data"].gold_output_path is None:
        entry = registry["electricity_fuel_type_data"]
        registry["electricity_fuel_type_data"] = DatasetRegistryEntry(
            dataset_id=entry.dataset_id,
            route=entry.route,
            topic=entry.topic,
            frequency=entry.frequency,
            data_columns=entry.data_columns,
            default_facets=entry.default_facets,
            bronze_output_path=entry.bronze_output_path,
            silver_output_path=entry.silver_output_path,
            gold_output_path=FUEL_GOLD_PATH,
        )
    return registry


def dataset_options_for_stage(stage: str) -> list[str]:
    if stage == "platinum":
        return list(PLATINUM_TARGETS)
    return RAW_DATASET_IDS
