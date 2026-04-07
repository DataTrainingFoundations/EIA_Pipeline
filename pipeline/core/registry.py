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
    "gold_electricity_operational_sales": "GOLD_ELECTRICITY_OPERATIONAL_SALES",
}


def load_registry() -> list[dict]:
    with REGISTRY_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle).get("datasets", [])


def normalize_dataset_id(dataset_id: str) -> str:
    return dataset_id.removesuffix("_hourly").removesuffix("_monthly")


def iter_datasets() -> list[dict]:
    return load_registry()


def iter_ingest_datasets() -> list[dict]:
    return load_registry()


def iter_transform_datasets() -> list[dict]:
    return [dataset for dataset in load_registry() if dataset.get("transform_enabled", False)]


def iter_cadence_datasets(cadence_group: str) -> list[dict]:
    return [dataset for dataset in load_registry() if dataset.get("cadence_group", dataset.get("frequency")) == cadence_group]


def iter_scheduled_ingest_datasets() -> list[dict]:
    return [dataset for dataset in load_registry() if dataset.get("scheduled_ingest", dataset.get("frequency") == "hourly")]


def iter_scheduled_transform_datasets() -> list[dict]:
    return [
        dataset
        for dataset in load_registry()
        if dataset.get("transform_enabled", False)
        and dataset.get("scheduled_transform", dataset.get("frequency") == "hourly")
    ]


def iter_scheduled_cadence_datasets(cadence_group: str) -> list[dict]:
    return [
        dataset
        for dataset in iter_cadence_datasets(cadence_group)
        if dataset.get("scheduled_ingest", dataset.get("frequency") == "hourly")
        and dataset.get("scheduled_transform", dataset.get("transform_enabled", False))
    ]


def iter_scheduled_ingest_cadence_datasets(cadence_group: str) -> list[dict]:
    return [
        dataset
        for dataset in iter_cadence_datasets(cadence_group)
        if dataset.get("scheduled_ingest", dataset.get("frequency") == "hourly")
    ]


def iter_scheduled_transform_cadence_datasets(cadence_group: str) -> list[dict]:
    return [
        dataset
        for dataset in iter_cadence_datasets(cadence_group)
        if dataset.get("transform_enabled", False)
        and dataset.get("scheduled_transform", dataset.get("frequency") == "hourly")
    ]


def iter_monthly_ingest_datasets() -> list[dict]:
    return [dataset for dataset in load_registry() if dataset.get("frequency") == "monthly"]


def iter_monthly_transform_datasets() -> list[dict]:
    return [
        dataset
        for dataset in load_registry()
        if dataset.get("frequency") == "monthly" and dataset.get("transform_enabled", False)
    ]


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
