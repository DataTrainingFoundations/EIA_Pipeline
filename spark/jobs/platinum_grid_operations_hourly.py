"""Grid operations serving-table builder.

This job reads curated Gold demand and fuel datasets, derives operational
signals and alert conditions, and writes stage tables for the Airflow merge
steps that populate the platinum grid operations marts.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_spark_app_config
from common.io import has_partitioned_parquet_input, read_partitioned_parquet
from common.logging_utils import configure_logging, log_job_complete, log_job_start
from common.quality import assert_no_nulls, assert_non_negative, assert_unique_keys, assert_value_bounds
from common.spark_session import build_spark_session
from common.windowing import filter_time_window

RENEWABLE_FUELS = ["SUN", "WND", "WAT", "GEO"]
FOSSIL_FUELS = ["COL", "NG", "OIL", "OTH"]
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the grid operations stage-builder job."""

    parser = argparse.ArgumentParser(description="Build grid operations hourly serving tables.")
    parser.add_argument("--silver-base-path", default="s3a://silver")
    parser.add_argument("--region-fact-path", default="s3a://gold/facts/region_demand_forecast_hourly")
    parser.add_argument("--fuel-fact-path", default="s3a://gold/facts/fuel_generation_hourly")
    parser.add_argument("--ops-stage-table", default="platinum.grid_operations_hourly_stage")
    parser.add_argument("--alerts-stage-table", default="platinum.grid_operations_alert_hourly_stage")
    parser.add_argument("--start")
    parser.add_argument("--end")
    return parser.parse_args()


def build_grid_operations_status(region_df: DataFrame, fuel_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Build the hourly status and alert stage tables for grid operations."""

    fuel_agg_df = (
        fuel_df.groupBy("period", "respondent")
        .agg(
            F.max("respondent_name").alias("fuel_respondent_name"),
            F.sum("generation_mwh").alias("total_generation_mwh"),
            F.sum(F.when(F.col("fueltype").isin(RENEWABLE_FUELS), F.col("generation_mwh")).otherwise(F.lit(0.0))).alias("renewable_generation_mwh"),
            F.sum(F.when(F.col("fueltype").isin(FOSSIL_FUELS), F.col("generation_mwh")).otherwise(F.lit(0.0))).alias("fossil_generation_mwh"),
            F.sum(F.when(F.col("fueltype") == "NG", F.col("generation_mwh")).otherwise(F.lit(0.0))).alias("gas_generation_mwh"),
        )
    )

    base_df = (
        region_df.join(fuel_agg_df, ["period", "respondent"], "left")
        .withColumn(
            "respondent_name",
            F.coalesce(F.col("respondent_name"), F.col("fuel_respondent_name"), F.col("respondent")),
        )
        .drop("fuel_respondent_name")
    )

    order_window = Window.partitionBy("respondent").orderBy("period")
    stats_window = Window.partitionBy("respondent")

    status_df = (
        base_df.withColumn("demand_ramp_mwh", F.col("actual_demand_mwh") - F.lag("actual_demand_mwh").over(order_window))
        .withColumn(
            "generation_gap_mwh",
            F.when(F.col("total_generation_mwh").isNotNull(), F.col("total_generation_mwh") - F.col("actual_demand_mwh")),
        )
        .withColumn(
            "coverage_ratio",
            F.when((F.col("total_generation_mwh").isNotNull()) & (F.col("actual_demand_mwh") > 0), F.col("total_generation_mwh") / F.col("actual_demand_mwh")),
        )
        .withColumn(
            "renewable_share_pct",
            F.when(F.col("total_generation_mwh") > 0, (F.col("renewable_generation_mwh") / F.col("total_generation_mwh")) * 100.0),
        )
        .withColumn(
            "fossil_share_pct",
            F.when(F.col("total_generation_mwh") > 0, (F.col("fossil_generation_mwh") / F.col("total_generation_mwh")) * 100.0),
        )
        .withColumn(
            "gas_share_pct",
            F.when(F.col("total_generation_mwh") > 0, (F.col("gas_generation_mwh") / F.col("total_generation_mwh")) * 100.0),
        )
        .withColumn("forecast_error_avg", F.avg("forecast_error_mwh").over(stats_window))
        .withColumn("forecast_error_std", F.coalesce(F.stddev_pop("forecast_error_mwh").over(stats_window), F.lit(0.0)))
        .withColumn("demand_ramp_avg", F.avg("demand_ramp_mwh").over(stats_window))
        .withColumn("demand_ramp_std", F.coalesce(F.stddev_pop("demand_ramp_mwh").over(stats_window), F.lit(0.0)))
        .withColumn("demand_avg", F.avg("actual_demand_mwh").over(stats_window))
        .withColumn("demand_std", F.coalesce(F.stddev_pop("actual_demand_mwh").over(stats_window), F.lit(0.0)))
        .withColumn(
            "forecast_error_zscore",
            F.when(F.col("forecast_error_std") > 0, (F.col("forecast_error_mwh") - F.col("forecast_error_avg")) / F.col("forecast_error_std")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "demand_ramp_zscore",
            F.when(F.col("demand_ramp_std") > 0, (F.col("demand_ramp_mwh") - F.col("demand_ramp_avg")) / F.col("demand_ramp_std")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "demand_zscore",
            F.when(F.col("demand_std") > 0, (F.col("actual_demand_mwh") - F.col("demand_avg")) / F.col("demand_std")).otherwise(F.lit(0.0)),
        )
        .drop("forecast_error_avg", "forecast_error_std", "demand_ramp_avg", "demand_ramp_std", "demand_avg", "demand_std", "event_date", "loaded_at")
        .withColumn(
            "alert_count",
            F.when(F.abs(F.col("forecast_error_zscore")) >= 2.5, F.lit(1)).otherwise(F.lit(0))
            + F.when(F.abs(F.col("demand_ramp_zscore")) >= 2.5, F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("coverage_ratio") < 0.9, F.lit(1)).otherwise(F.lit(0))
            + F.when(F.abs(F.col("demand_zscore")) >= 2.5, F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("updated_at", F.current_timestamp())
        .orderBy("period", "respondent")
    )

    assert_no_nulls(status_df, ["period", "respondent", "respondent_name", "actual_demand_mwh"], "platinum.grid_operations_hourly_stage")
    assert_unique_keys(status_df, ["period", "respondent"], "platinum.grid_operations_hourly_stage")
    assert_non_negative(
        status_df,
        ["actual_demand_mwh", "day_ahead_forecast_mwh", "total_generation_mwh", "renewable_generation_mwh", "fossil_generation_mwh", "gas_generation_mwh", "alert_count"],
        "platinum.grid_operations_hourly_stage",
    )
    assert_value_bounds(status_df, "renewable_share_pct", min_value=0.0, max_value=100.0, label="platinum.grid_operations_hourly_stage")
    assert_value_bounds(status_df, "fossil_share_pct", min_value=0.0, max_value=100.0, label="platinum.grid_operations_hourly_stage")
    assert_value_bounds(status_df, "gas_share_pct", min_value=0.0, max_value=100.0, label="platinum.grid_operations_hourly_stage")
    assert_value_bounds(status_df, "coverage_ratio", min_value=0.0, label="platinum.grid_operations_hourly_stage")

    forecast_alerts = (
        status_df.filter(F.abs(F.col("forecast_error_zscore")) >= 2.5)
        .select(
            "period",
            "respondent",
            "respondent_name",
            F.lit("forecast_error").alias("alert_type"),
            F.when(F.abs(F.col("forecast_error_zscore")) >= 3.5, F.lit("high")).otherwise(F.lit("medium")).alias("severity"),
            F.col("forecast_error_zscore").alias("metric_value"),
            F.lit(2.5).alias("threshold_value"),
            F.concat(F.lit("Forecast error z-score reached "), F.round(F.col("forecast_error_zscore"), 2)).alias("message"),
        )
    )

    ramp_alerts = (
        status_df.filter(F.abs(F.col("demand_ramp_zscore")) >= 2.5)
        .select(
            "period",
            "respondent",
            "respondent_name",
            F.lit("ramp_risk").alias("alert_type"),
            F.when(F.abs(F.col("demand_ramp_zscore")) >= 3.5, F.lit("high")).otherwise(F.lit("medium")).alias("severity"),
            F.col("demand_ramp_zscore").alias("metric_value"),
            F.lit(2.5).alias("threshold_value"),
            F.concat(F.lit("Demand ramp z-score reached "), F.round(F.col("demand_ramp_zscore"), 2)).alias("message"),
        )
    )

    coverage_alerts = (
        status_df.filter(F.col("coverage_ratio") < 0.9)
        .select(
            "period",
            "respondent",
            "respondent_name",
            F.lit("fuel_mix_support_gap").alias("alert_type"),
            F.when(F.col("coverage_ratio") < 0.8, F.lit("high")).otherwise(F.lit("medium")).alias("severity"),
            F.col("coverage_ratio").alias("metric_value"),
            F.lit(0.9).alias("threshold_value"),
            F.concat(F.lit("Generation coverage ratio dropped to "), F.round(F.col("coverage_ratio"), 2)).alias("message"),
        )
    )

    demand_alerts = (
        status_df.filter(F.abs(F.col("demand_zscore")) >= 2.5)
        .select(
            "period",
            "respondent",
            "respondent_name",
            F.lit("demand_anomaly").alias("alert_type"),
            F.when(F.abs(F.col("demand_zscore")) >= 3.5, F.lit("high")).otherwise(F.lit("medium")).alias("severity"),
            F.col("demand_zscore").alias("metric_value"),
            F.lit(2.5).alias("threshold_value"),
            F.concat(F.lit("Demand z-score reached "), F.round(F.col("demand_zscore"), 2)).alias("message"),
        )
    )

    alerts_df = forecast_alerts.unionByName(ramp_alerts).unionByName(coverage_alerts).unionByName(demand_alerts).withColumn("updated_at", F.current_timestamp())
    if alerts_df.limit(1).count() > 0:
        assert_unique_keys(alerts_df, ["period", "respondent", "alert_type"], "platinum.grid_operations_alert_hourly_stage")

    return status_df, alerts_df


def main() -> None:
    """Build and write grid operations stage tables for the current window."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("platinum_grid_operations_hourly", config)
    log_job_start(
        logger,
        "platinum_grid_operations_hourly",
        input_path=f"{args.region_fact_path},{args.fuel_fact_path}",
        stage_table=args.ops_stage_table,
        start=args.start,
        end=args.end,
    )

    jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/platform")
    jdbc_props = {
        "user": os.getenv("POSTGRES_USER", "platform"),
        "password": os.getenv("POSTGRES_PASSWORD", "platform"),
        "driver": "org.postgresql.Driver",
    }

    if not has_partitioned_parquet_input(spark, args.region_fact_path) or not has_partitioned_parquet_input(spark, args.fuel_fact_path):
        return

    region_df = read_partitioned_parquet(spark, args.region_fact_path)
    fuel_df = read_partitioned_parquet(spark, args.fuel_fact_path)
    region_df = filter_time_window(region_df, "period", args.start, args.end)
    fuel_df = filter_time_window(fuel_df, "period", args.start, args.end)

    status_df, alerts_df = build_grid_operations_status(region_df, fuel_df)
    if status_df.limit(1).count() == 0:
        return

    status_df.write.mode("overwrite").jdbc(jdbc_url, args.ops_stage_table, properties=jdbc_props)
    alerts_df.write.mode("overwrite").jdbc(jdbc_url, args.alerts_stage_table, properties=jdbc_props)
    log_job_complete(
        logger,
        "platinum_grid_operations_hourly",
        stage_table=args.ops_stage_table,
        start=args.start,
        end=args.end,
        row_count=status_df.count(),
    )


if __name__ == "__main__":
    main()
