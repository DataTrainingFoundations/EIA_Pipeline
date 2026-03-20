"""Resource planning serving-table builder.

This job reads curated Gold demand, fuel, and fuel-dimension datasets, derives
daily planning metrics, and writes a warehouse stage table for the Airflow
merge step that populates the platinum planning mart.
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
from common.io import (
    has_partitioned_parquet_input,
    path_exists,
    read_partitioned_parquet,
)
from common.logging_utils import configure_logging, log_job_complete, log_job_start
from common.quality import (
    assert_no_nulls,
    assert_non_negative,
    assert_unique_keys,
    assert_value_bounds,
)
from common.spark_session import build_spark_session
from common.windowing import filter_time_window

RENEWABLE_FUELS = ["SUN", "WND", "WAT", "GEO"]
FOSSIL_FUELS = ["COL", "NG", "OIL", "OTH"]


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the resource planning stage-builder job."""

    parser = argparse.ArgumentParser(
        description="Build resource planning daily serving tables."
    )
    parser.add_argument("--silver-base-path", default="s3a://silver")
    parser.add_argument(
        "--region-fact-path", default="s3a://gold/facts/region_demand_forecast_hourly"
    )
    parser.add_argument(
        "--fuel-fact-path", default="s3a://gold/facts/fuel_generation_hourly"
    )
    parser.add_argument("--fuel-dim-path", default="s3a://gold/dimensions/fuel_type")
    parser.add_argument(
        "--planning-stage-table", default="platinum.resource_planning_daily_stage"
    )
    parser.add_argument("--start")
    parser.add_argument("--end")
    return parser.parse_args()


def read_dimension_dataset(spark, base_path: str) -> DataFrame:  # noqa: ANN001
    """Read the full fuel dimension parquet dataset."""

    return spark.read.option("mergeSchema", "true").parquet(base_path)


def build_resource_planning_daily(
    region_df: DataFrame, fuel_df: DataFrame, fuel_dim_df: DataFrame
) -> DataFrame:
    """Build the daily planning stage table from region and fuel inputs."""

    region_df = region_df.withColumn("date", F.to_date("period"))
    fuel_df = fuel_df.withColumn("date", F.to_date("period"))

    demand_daily_df = (
        region_df.filter(F.col("actual_demand_mwh").isNotNull())
        .groupBy("date", "respondent")
        .agg(
            F.max("respondent_name").alias("respondent_name"),
            F.sum("actual_demand_mwh").alias("daily_demand_mwh"),
            F.max("actual_demand_mwh").alias("peak_hourly_demand_mwh"),
            F.avg(F.abs(F.col("forecast_error_pct"))).alias(
                "avg_abs_forecast_error_pct"
            ),
        )
    )

    fuel_with_emissions_df = fuel_df.join(
        fuel_dim_df.select(
            "fueltype", F.col("emissions_factor_kg_per_mwh").alias("emissions_factor")
        ),
        ["fueltype"],
        "left",
    ).withColumn("emissions_factor", F.coalesce(F.col("emissions_factor"), F.lit(0.0)))

    fuel_daily_df = fuel_with_emissions_df.groupBy("date", "respondent").agg(
        F.max("respondent_name").alias("fuel_respondent_name"),
        F.sum("generation_mwh").alias("total_generation_mwh"),
        F.sum(
            F.when(
                F.col("fueltype").isin(RENEWABLE_FUELS), F.col("generation_mwh")
            ).otherwise(F.lit(0.0))
        ).alias("renewable_generation_mwh"),
        F.sum(
            F.when(
                F.col("fueltype").isin(FOSSIL_FUELS), F.col("generation_mwh")
            ).otherwise(F.lit(0.0))
        ).alias("fossil_generation_mwh"),
        F.sum(
            F.when(F.col("fueltype") == "NG", F.col("generation_mwh")).otherwise(
                F.lit(0.0)
            )
        ).alias("gas_generation_mwh"),
        F.sum(F.col("generation_mwh") * F.col("emissions_factor")).alias(
            "weighted_emissions"
        ),
    )

    fuel_share_df = (
        fuel_df.groupBy("date", "respondent", "fueltype")
        .agg(F.sum("generation_mwh").alias("fuel_generation_mwh"))
        .join(
            fuel_df.groupBy("date", "respondent").agg(
                F.sum("generation_mwh").alias("total_generation_mwh")
            ),
            ["date", "respondent"],
            "left",
        )
        .withColumn(
            "fuel_share",
            F.when(
                F.col("total_generation_mwh") > 0,
                F.col("fuel_generation_mwh") / F.col("total_generation_mwh"),
            ).otherwise(F.lit(0.0)),
        )
        .groupBy("date", "respondent")
        .agg(
            (F.lit(1.0) - F.sum(F.pow(F.col("fuel_share"), F.lit(2.0)))).alias(
                "fuel_diversity_index"
            )
        )
    )

    peak_window = Window.partitionBy("respondent", F.to_date("period")).orderBy(
        F.col("actual_demand_mwh").desc(), F.col("period").asc()
    )
    peak_hours_df = (
        region_df.filter(F.col("actual_demand_mwh").isNotNull())
        .withColumn("peak_rank", F.row_number().over(peak_window))
        .filter(F.col("peak_rank") == 1)
        .select("date", "respondent", F.col("period").alias("peak_period"))
    )

    fuel_hourly_agg_df = fuel_df.groupBy("period", "respondent").agg(
        F.sum("generation_mwh").alias("total_generation_mwh"),
        F.sum(
            F.when(F.col("fueltype") == "NG", F.col("generation_mwh")).otherwise(
                F.lit(0.0)
            )
        ).alias("gas_generation_mwh"),
    )

    peak_fuel_df = peak_hours_df.join(
        fuel_hourly_agg_df,
        (peak_hours_df.peak_period == fuel_hourly_agg_df.period)
        & (peak_hours_df.respondent == fuel_hourly_agg_df.respondent),
        "left",
    ).select(
        peak_hours_df.date,
        peak_hours_df.respondent,
        F.when(
            F.col("total_generation_mwh") > 0,
            (F.col("gas_generation_mwh") / F.col("total_generation_mwh")) * 100.0,
        ).alias("peak_hour_gas_share_pct"),
    )

    planning_df = (
        demand_daily_df.join(fuel_daily_df, ["date", "respondent"], "left")
        .join(fuel_share_df, ["date", "respondent"], "left")
        .join(peak_fuel_df, ["date", "respondent"], "left")
        .withColumn(
            "respondent_name",
            F.coalesce(
                F.col("respondent_name"),
                F.col("fuel_respondent_name"),
                F.col("respondent"),
            ),
        )
        .drop("fuel_respondent_name")
        .withColumn(
            "renewable_share_pct",
            F.when(
                F.col("total_generation_mwh") > 0,
                (F.col("renewable_generation_mwh") / F.col("total_generation_mwh"))
                * 100.0,
            ),
        )
        .withColumn(
            "fossil_share_pct",
            F.when(
                F.col("total_generation_mwh") > 0,
                (F.col("fossil_generation_mwh") / F.col("total_generation_mwh"))
                * 100.0,
            ),
        )
        .withColumn(
            "gas_share_pct",
            F.when(
                F.col("total_generation_mwh") > 0,
                (F.col("gas_generation_mwh") / F.col("total_generation_mwh")) * 100.0,
            ),
        )
        .withColumn(
            "carbon_intensity_kg_per_mwh",
            F.when(
                F.col("total_generation_mwh") > 0,
                F.col("weighted_emissions") / F.col("total_generation_mwh"),
            ),
        )
        .withColumn(
            "clean_coverage_ratio",
            F.when(
                (F.col("renewable_generation_mwh").isNotNull())
                & (F.col("daily_demand_mwh") > 0),
                F.col("renewable_generation_mwh") / F.col("daily_demand_mwh"),
            ),
        )
        .withColumn("weekend_flag", F.dayofweek("date").isin([1, 7]))
        .withColumn("updated_at", F.current_timestamp())
        .select(
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
        )
        .orderBy("date", "respondent")
    )

    assert_no_nulls(
        planning_df,
        ["date", "respondent", "respondent_name", "daily_demand_mwh"],
        "platinum.resource_planning_daily_stage",
    )
    assert_unique_keys(
        planning_df, ["date", "respondent"], "platinum.resource_planning_daily_stage"
    )
    assert_non_negative(
        planning_df,
        [
            "daily_demand_mwh",
            "peak_hourly_demand_mwh",
            "avg_abs_forecast_error_pct",
            "carbon_intensity_kg_per_mwh",
            "fuel_diversity_index",
            "clean_coverage_ratio",
        ],
        "platinum.resource_planning_daily_stage",
    )
    assert_value_bounds(
        planning_df,
        "renewable_share_pct",
        min_value=0.0,
        max_value=100.0,
        label="platinum.resource_planning_daily_stage",
    )
    assert_value_bounds(
        planning_df,
        "fossil_share_pct",
        min_value=0.0,
        max_value=100.0,
        label="platinum.resource_planning_daily_stage",
    )
    assert_value_bounds(
        planning_df,
        "gas_share_pct",
        min_value=0.0,
        max_value=100.0,
        label="platinum.resource_planning_daily_stage",
    )
    assert_value_bounds(
        planning_df,
        "fuel_diversity_index",
        min_value=0.0,
        max_value=1.0,
        label="platinum.resource_planning_daily_stage",
    )
    assert_value_bounds(
        planning_df,
        "peak_hour_gas_share_pct",
        min_value=0.0,
        max_value=100.0,
        label="platinum.resource_planning_daily_stage",
    )
    return planning_df


def main() -> None:
    """Build and write the daily resource planning stage table."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("platinum_resource_planning_daily", config)
    logger = logging.getLogger(__name__)
    log_job_start(
        logger,
        "platinum_resource_planning_daily",
        input_path=f"{args.region_fact_path},{args.fuel_fact_path},{args.fuel_dim_path}",
        stage_table=args.planning_stage_table,
        start=args.start,
        end=args.end,
    )

    jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/platform")
    jdbc_props = {
        "user": os.getenv("POSTGRES_USER", "platform"),
        "password": os.getenv("POSTGRES_PASSWORD", "platform"),
        "driver": "org.postgresql.Driver",
    }

    if (
        not has_partitioned_parquet_input(spark, args.region_fact_path)
        or not has_partitioned_parquet_input(spark, args.fuel_fact_path)
        or not path_exists(spark, args.fuel_dim_path)
    ):
        return

    region_df = read_partitioned_parquet(spark, args.region_fact_path)
    fuel_df = read_partitioned_parquet(spark, args.fuel_fact_path)
    fuel_dim_df = read_dimension_dataset(spark, args.fuel_dim_path)
    region_df = filter_time_window(region_df, "period", args.start, args.end)
    fuel_df = filter_time_window(fuel_df, "period", args.start, args.end)
    planning_df = build_resource_planning_daily(region_df, fuel_df, fuel_dim_df)

    if planning_df.limit(1).count() == 0:
        return

    planning_df.write.mode("overwrite").jdbc(
        jdbc_url, args.planning_stage_table, properties=jdbc_props
    )
    log_job_complete(
        logger,
        "platinum_resource_planning_daily",
        stage_table=args.planning_stage_table,
        start=args.start,
        end=args.end,
        row_count=planning_df.count(),
    )


if __name__ == "__main__":
    main()
