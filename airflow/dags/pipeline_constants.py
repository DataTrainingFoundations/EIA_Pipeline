from __future__ import annotations

import os

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
SPARK_PACKAGES_WITH_POSTGRES = f"{SPARK_PACKAGES},org.postgresql:postgresql:42.7.3"

BACKFILL_SCHEDULE = os.getenv("AIRFLOW_BACKFILL_SCHEDULE", "5 * * * *")
GLOBAL_BACKFILL_POOL = os.getenv(
    "AIRFLOW_GLOBAL_BACKFILL_POOL", "global_backfill_worker"
)
GRID_OPERATIONS_SCHEDULE = os.getenv(
    "AIRFLOW_PLATINUM_GRID_OPERATIONS_SCHEDULE",
    os.getenv(
        "AIRFLOW_GRID_OPERATIONS_SCHEDULE",
        os.getenv("AIRFLOW_GRID_OPS_SCHEDULE", "20 * * * *"),
    ),
)
RESOURCE_PLANNING_DAILY_SCHEDULE = os.getenv(
    "AIRFLOW_PLATINUM_RESOURCE_PLANNING_SCHEDULE",
    os.getenv(
        "AIRFLOW_RESOURCE_PLANNING_DAILY_SCHEDULE",
        os.getenv("AIRFLOW_RESOURCE_PLANNING_SCHEDULE", "15,45 * * * *"),
    ),
)
BRONZE_VERIFICATION_SCHEDULE = os.getenv(
    "AIRFLOW_BRONZE_VERIFICATION_SCHEDULE", "40 * * * *"
)
BRONZE_REPAIR_SCHEDULE = os.getenv("AIRFLOW_BRONZE_REPAIR_SCHEDULE", "50 * * * *")

GOLD_REGION_FACT_HOURLY_PATH = "s3a://gold/facts/region_demand_forecast_hourly"
GOLD_FUEL_FACT_HOURLY_PATH = "s3a://gold/facts/fuel_generation_hourly"
GOLD_RESPONDENT_DIM_PATH = "s3a://gold/dimensions/respondent"
GOLD_FUEL_TYPE_DIM_PATH = "s3a://gold/dimensions/fuel_type"

REGION_DAILY_COLUMNS = [
    "date",
    "respondent",
    "daily_demand_mwh",
    "avg_hourly_demand_mwh",
    "peak_hourly_demand_mwh",
    "loaded_at",
    "source_window_start",
    "source_window_end",
    "updated_at",
]

GRID_OPERATIONS_COLUMNS = [
    "period",
    "respondent",
    "respondent_name",
    "actual_demand_mwh",
    "day_ahead_forecast_mwh",
    "forecast_error_mwh",
    "forecast_error_pct",
    "forecast_error_zscore",
    "demand_ramp_mwh",
    "demand_ramp_zscore",
    "demand_zscore",
    "total_generation_mwh",
    "renewable_generation_mwh",
    "fossil_generation_mwh",
    "gas_generation_mwh",
    "renewable_share_pct",
    "fossil_share_pct",
    "gas_share_pct",
    "generation_gap_mwh",
    "coverage_ratio",
    "alert_count",
    "updated_at",
]

GRID_OPERATIONS_ALERT_COLUMNS = [
    "period",
    "respondent",
    "respondent_name",
    "alert_type",
    "severity",
    "metric_value",
    "threshold_value",
    "message",
    "updated_at",
]

RESOURCE_PLANNING_COLUMNS = [
    "date",
    "respondent",
    "respondent_name",
    "daily_demand_mwh",
    "peak_hourly_demand_mwh",
    "avg_abs_forecast_error_pct",
    "renewable_share_pct",
    "fossil_share_pct",
    "gas_share_pct",
    "carbon_intensity_kg_per_mwh",
    "fuel_diversity_index",
    "peak_hour_gas_share_pct",
    "clean_coverage_ratio",
    "weekend_flag",
    "updated_at",
]

POWER_OPERATIONS_MONTHLY_COLUMNS = [
    "period",
    "location",
    "location_name",
    "sector_id",
    "sector_name",
    "fueltype_id",
    "fueltype_name",
    "generation_mwh",
    "generation_share_pct",
    "consumption_for_eg_thousand_units",
    "ash_content_pct",
    "heat_content_btu_per_unit",
    "cost_usd",
    "fuel_heat_input_mmbtu",
    "heat_rate_btu_per_kwh",
    "loaded_at",
    "source_window_start",
    "source_window_end",
    "updated_at",
]

BRONZE_HOURLY_COVERAGE_COLUMNS = [
    "dataset_id",
    "hour_start_utc",
    "observed_row_count",
    "expected_row_count",
    "row_count_delta",
    "status",
    "oldest_hour_utc",
    "newest_hour_utc",
    "verification_boundary_hour_utc",
    "verified_at",
]
