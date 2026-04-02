from __future__ import annotations

from pathlib import Path

import yaml

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "config" / "dataset_registry.yml"

GOLD_TABLES = {
    "dim_balancing_authority": "DIM_BALANCING_AUTHORITY",
    "dim_fuel_type": "DIM_FUEL_TYPE",
    "fact_generation_hourly": "FACT_GENERATION_HOURLY",
    "fact_demand_hourly": "FACT_DEMAND_HOURLY",
    "agg_daily_generation": "AGG_DAILY_GENERATION",
    "agg_daily_demand_peak": "AGG_DAILY_DEMAND_PEAK",
}


def load_registry() -> list[dict]:
    with REGISTRY_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle).get("datasets", [])


def normalize_dataset_id(dataset_id: str) -> str:
    return dataset_id.removesuffix("_hourly").removesuffix("_monthly")


def iter_datasets() -> list[dict]:
    return load_registry()


def get_dataset(dataset_id: str) -> dict:
    for dataset in load_registry():
        if dataset["id"] == dataset_id:
            return dataset
    raise ValueError(f"Unknown dataset_id '{dataset_id}'")


def get_raw_table_name(dataset_id: str) -> str:
    return get_dataset(dataset_id)["snowflake_table"]


def get_silver_table_name(dataset_id: str) -> str:
    return f"SILVER_{normalize_dataset_id(dataset_id).upper()}"


def get_gold_table_names() -> dict[str, str]:
    return dict(GOLD_TABLES)
