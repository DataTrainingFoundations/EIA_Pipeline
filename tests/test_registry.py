from __future__ import annotations

from pipeline.core.registry import get_gold_table_names, iter_transform_datasets


def test_transform_registry_contains_expected_dataset_ids() -> None:
    dataset_ids = {dataset["id"] for dataset in iter_transform_datasets()}

    assert dataset_ids == {
        "electricity_generation_hourly",
        "electricity_demand_hourly",
        "electricity_retail_sales_monthly",
        "electricity_power_operational_data_monthly",
    }


def test_gold_table_names_remain_stable() -> None:
    assert get_gold_table_names() == {
        "dim_balancing_authority": "DIM_BALANCING_AUTHORITY",
        "dim_fuel_type": "DIM_FUEL_TYPE",
        "fact_generation_hourly": "FACT_GENERATION_HOURLY",
        "fact_demand_hourly": "FACT_DEMAND_HOURLY",
        "agg_daily_generation": "AGG_DAILY_GENERATION",
        "agg_daily_demand_peak": "AGG_DAILY_DEMAND_PEAK",
        "fact_sales_monthly": "FACT_SALES_MONTHLY",
    }
