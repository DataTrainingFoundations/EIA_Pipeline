"""Curated Gold dataset builder for downstream serving marts.

This job reads Silver datasets, resolves conflicts into stable Gold fact and
dimension tables, and writes the canonical parquet datasets used by platinum
jobs and dashboards.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_spark_app_config
from common.io import (
    has_partitioned_parquet_input,
    merge_partitioned_parquet,
    read_parquet_if_exists,
    read_partitioned_parquet,
)
from common.logging_utils import configure_logging, log_job_complete, log_job_start
from common.quality import assert_no_nulls, assert_non_negative, assert_unique_keys
from common.spark_session import build_spark_session
from common.windowing import filter_time_window

SUPPORTED_DATASETS = ["electricity_region_data", "electricity_fuel_type_data", "all"]
RENEWABLE_FUELS = ["SUN", "WND", "WAT", "GEO"]
FOSSIL_FUELS = ["COL", "NG", "OIL", "OTH"]
SILVER_READ_RETRIES = 3
SILVER_READ_RETRY_DELAY_SECONDS = 2.0
EMISSIONS_FACTORS = {
    "COL": 1000.0,
    "NG": 490.0,
    "OIL": 780.0,
    "OTH": 650.0,
    "NUC": 12.0,
    "SUN": 35.0,
    "WND": 12.0,
    "WAT": 24.0,
    "GEO": 45.0,
}


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the curated Gold job."""

    parser = argparse.ArgumentParser(
        description="Build curated Gold datasets for downstream serving marts."
    )
    parser.add_argument("--silver-base-path", default="s3a://silver")
    parser.add_argument("--dataset", choices=SUPPORTED_DATASETS, default="all")
    parser.add_argument(
        "--region-fact-path", default="s3a://gold/facts/region_demand_forecast_hourly"
    )
    parser.add_argument(
        "--fuel-fact-path", default="s3a://gold/facts/fuel_generation_hourly"
    )
    parser.add_argument(
        "--respondent-dim-path", default="s3a://gold/dimensions/respondent"
    )
    parser.add_argument("--fuel-dim-path", default="s3a://gold/dimensions/fuel_type")
    parser.add_argument("--start")
    parser.add_argument("--end")
    return parser.parse_args()


def build_region_hourly_metrics(region_df: DataFrame) -> DataFrame:
    """Build the curated region demand and forecast Gold fact table."""

    # Region rows can legitimately be revised across re-fetches for the same
    # business key. Gold keeps the latest loaded version for each key.
    latest_window = Window.partitionBy("period", "respondent", "type").orderBy(
        F.col("loaded_at").desc(), F.col("event_id").desc()
    )
    stable_region_df = (
        region_df.withColumn("record_rank", F.row_number().over(latest_window))
        .filter(F.col("record_rank") == 1)
        .drop("record_rank")
    )
    demand_df = (
        stable_region_df.filter(F.col("type") == "D")
        .groupBy("period", "respondent", "event_date")
        .agg(
            F.max("respondent_name").alias("demand_respondent_name"),
            F.max("value").alias("actual_demand_mwh"),
            F.max("loaded_at").alias("demand_loaded_at"),
        )
    )

    forecast_df = (
        stable_region_df.filter(F.col("type") == "DF")
        .groupBy("period", "respondent", "event_date")
        .agg(
            F.max("respondent_name").alias("forecast_respondent_name"),
            F.max("value").alias("day_ahead_forecast_mwh"),
            F.max("loaded_at").alias("forecast_loaded_at"),
        )
    )

    gold_df = (
        demand_df.join(
            forecast_df, ["period", "respondent", "event_date"], "full_outer"
        )
        .withColumn(
            "respondent_name",
            F.coalesce(
                F.col("demand_respondent_name"),
                F.col("forecast_respondent_name"),
                F.col("respondent"),
            ),
        )
        .withColumn(
            "loaded_at",
            F.greatest(F.col("demand_loaded_at"), F.col("forecast_loaded_at")),
        )
        .withColumn(
            "forecast_error_mwh",
            F.when(
                F.col("actual_demand_mwh").isNotNull()
                & F.col("day_ahead_forecast_mwh").isNotNull(),
                F.col("actual_demand_mwh") - F.col("day_ahead_forecast_mwh"),
            ),
        )
        .withColumn(
            "forecast_error_pct",
            F.when(
                F.col("day_ahead_forecast_mwh").isNotNull()
                & (F.col("day_ahead_forecast_mwh") != 0),
                (F.col("forecast_error_mwh") / F.col("day_ahead_forecast_mwh")) * 100.0,
            ),
        )
        .select(
            "period",
            "event_date",
            "respondent",
            "respondent_name",
            F.lit("MWh").alias("value_units"),
            "actual_demand_mwh",
            "day_ahead_forecast_mwh",
            "forecast_error_mwh",
            "forecast_error_pct",
            "loaded_at",
        )
        .orderBy("period", "respondent")
    )
    assert_no_nulls(
        gold_df,
        ["period", "event_date", "respondent", "respondent_name", "value_units"],
        "gold.fact_region_demand_forecast_hourly",
    )
    assert_unique_keys(
        gold_df, ["period", "respondent"], "gold.fact_region_demand_forecast_hourly"
    )
    assert_non_negative(
        gold_df,
        ["actual_demand_mwh", "day_ahead_forecast_mwh"],
        "gold.fact_region_demand_forecast_hourly",
    )
    return gold_df


def build_fuel_type_hourly_generation(fuel_df: DataFrame) -> DataFrame:
    """Build the curated fuel generation Gold fact table."""

    # Fuel rows can also be revised across re-fetches. Keep the latest version
    # per hourly business key before writing curated output.
    latest_window = Window.partitionBy("period", "respondent", "fueltype").orderBy(
        F.col("loaded_at").desc(), F.col("event_id").desc()
    )
    gold_df = (
        fuel_df.withColumn("record_rank", F.row_number().over(latest_window))
        .filter(F.col("record_rank") == 1)
        .drop("record_rank")
        .withColumn("generation_mwh", F.greatest(F.col("value"), F.lit(0.0)))
        .orderBy("period", "respondent", "fueltype")
        .select(
            "period",
            "event_date",
            "respondent",
            "respondent_name",
            "fueltype",
            "fueltype_name",
            F.lit("MWh").alias("value_units"),
            "generation_mwh",
            "loaded_at",
        )
    )
    assert_no_nulls(
        gold_df,
        [
            "period",
            "event_date",
            "respondent",
            "respondent_name",
            "fueltype",
            "generation_mwh",
            "value_units",
        ],
        "gold.fact_fuel_generation_hourly",
    )
    assert_unique_keys(
        gold_df,
        ["period", "respondent", "fueltype"],
        "gold.fact_fuel_generation_hourly",
    )
    assert_non_negative(gold_df, ["generation_mwh"], "gold.fact_fuel_generation_hourly")
    return gold_df


def write_partitioned(
    df: DataFrame, output_path: str, *, merge_keys: list[str]
) -> None:
    """Write a fact dataset while preserving earlier rows in touched day partitions."""

    merge_partitioned_parquet(
        df,
        output_path,
        merge_keys=merge_keys,
        freshness_columns=["loaded_at"],
        partition_column="event_date",
    )


def build_respondent_dimension(
    spark, region_df: DataFrame | None, fuel_df: DataFrame | None, existing_path: str
) -> DataFrame:  # noqa: ANN001
    """Build the respondent dimension from current observations plus prior state."""

    observations: list[DataFrame] = []
    existing_count_df: DataFrame | None = None
    if region_df is not None:
        observations.append(
            region_df.select(
                "respondent",
                "respondent_name",
                "event_date",
                F.lit("electricity_region_data").alias("source_dataset"),
            )
        )
    if fuel_df is not None:
        observations.append(
            fuel_df.select(
                "respondent",
                "respondent_name",
                "event_date",
                F.lit("electricity_fuel_type_data").alias("source_dataset"),
            )
        )
    existing_df = read_parquet_if_exists(
        spark,
        existing_path,
        retries=SILVER_READ_RETRIES,
        retry_delay_seconds=SILVER_READ_RETRY_DELAY_SECONDS,
    )
    if existing_df is not None:
        existing_count_df = existing_df.select(
            "respondent",
            F.col("source_dataset_count").alias("existing_source_dataset_count"),
        ).dropDuplicates(["respondent"])
        observations.append(
            existing_df.select(
                "respondent",
                "respondent_name",
                F.col("first_seen_date").alias("event_date"),
                F.lit(None).cast("string").alias("source_dataset"),
            )
        )
        observations.append(
            existing_df.select(
                "respondent",
                "respondent_name",
                F.col("last_seen_date").alias("event_date"),
                F.lit(None).cast("string").alias("source_dataset"),
            )
        )
    if not observations:
        raise ValueError(
            "gold.dim_respondent requires at least one region or fuel source"
        )
    observations_df = observations[0]
    for df in observations[1:]:
        observations_df = observations_df.unionByName(df)
    latest_window = Window.partitionBy("respondent").orderBy(
        F.col("event_date").desc(), F.col("respondent_name").desc()
    )
    latest_name_df = (
        observations_df.filter(
            F.col("respondent").isNotNull() & F.col("respondent_name").isNotNull()
        )
        .withColumn("name_rank", F.row_number().over(latest_window))
        .filter(F.col("name_rank") == 1)
        .select("respondent", "respondent_name")
    )
    dim_df = (
        observations_df.filter(
            F.col("respondent").isNotNull() & F.col("event_date").isNotNull()
        )
        .groupBy("respondent")
        .agg(
            F.min("event_date").alias("first_seen_date"),
            F.max("event_date").alias("last_seen_date"),
            F.countDistinct("source_dataset").alias("observed_source_dataset_count"),
        )
        .join(latest_name_df, ["respondent"], "left")
    )
    if existing_count_df is not None:
        dim_df = dim_df.join(existing_count_df, ["respondent"], "left")
    else:
        dim_df = dim_df.withColumn("existing_source_dataset_count", F.lit(0))
    dim_df = (
        dim_df.withColumn(
            "source_dataset_count",
            F.greatest(
                F.coalesce(F.col("observed_source_dataset_count"), F.lit(0)),
                F.coalesce(F.col("existing_source_dataset_count"), F.lit(0)),
            ),
        )
        .withColumn("updated_at", F.current_timestamp())
        .drop("observed_source_dataset_count", "existing_source_dataset_count")
        .select(
            "respondent",
            "respondent_name",
            "first_seen_date",
            "last_seen_date",
            "source_dataset_count",
            "updated_at",
        )
        .orderBy("respondent")
    )
    assert_no_nulls(
        dim_df,
        ["respondent", "respondent_name", "first_seen_date", "last_seen_date"],
        "gold.dim_respondent",
    )
    assert_unique_keys(dim_df, ["respondent"], "gold.dim_respondent")
    return dim_df


def build_fuel_type_dimension(
    spark, fuel_df: DataFrame | None, existing_path: str
) -> DataFrame:  # noqa: ANN001
    """Build the fuel-type dimension from current observations plus prior state."""

    observations: list[DataFrame] = []
    if fuel_df is not None:
        observations.append(
            fuel_df.select(
                "fueltype",
                "fueltype_name",
                "event_date",
            )
        )
    existing_df = read_parquet_if_exists(
        spark,
        existing_path,
        retries=SILVER_READ_RETRIES,
        retry_delay_seconds=SILVER_READ_RETRY_DELAY_SECONDS,
    )
    if existing_df is not None:
        observations.append(
            existing_df.select(
                "fueltype",
                "fueltype_name",
                F.current_date().alias("event_date"),
            )
        )
    if not observations:
        raise ValueError("gold.dim_fuel_type requires a fuel source")
    observations_df = observations[0]
    for df in observations[1:]:
        observations_df = observations_df.unionByName(df)
    latest_window = Window.partitionBy("fueltype").orderBy(
        F.col("event_date").desc(), F.col("fueltype_name").desc()
    )
    latest_name_df = (
        observations_df.filter(
            F.col("fueltype").isNotNull() & F.col("fueltype_name").isNotNull()
        )
        .withColumn("name_rank", F.row_number().over(latest_window))
        .filter(F.col("name_rank") == 1)
        .select("fueltype", "fueltype_name")
    )
    emissions_expr = F.create_map(
        *[
            value
            for item in EMISSIONS_FACTORS.items()
            for value in (F.lit(item[0]), F.lit(item[1]))
        ]
    )
    dim_df = (
        observations_df.filter(F.col("fueltype").isNotNull())
        .select("fueltype")
        .distinct()
        .join(latest_name_df, ["fueltype"], "left")
        .withColumn(
            "fuel_category",
            F.when(F.col("fueltype").isin(RENEWABLE_FUELS), F.lit("renewable"))
            .when(F.col("fueltype").isin(FOSSIL_FUELS), F.lit("fossil"))
            .when(F.col("fueltype") == "NUC", F.lit("nuclear"))
            .otherwise(F.lit("other")),
        )
        .withColumn(
            "emissions_factor_kg_per_mwh",
            F.coalesce(emissions_expr[F.col("fueltype")], F.lit(0.0)),
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "fueltype",
            "fueltype_name",
            "fuel_category",
            "emissions_factor_kg_per_mwh",
            "updated_at",
        )
        .orderBy("fueltype")
    )
    assert_no_nulls(
        dim_df, ["fueltype", "fueltype_name", "fuel_category"], "gold.dim_fuel_type"
    )
    assert_unique_keys(dim_df, ["fueltype"], "gold.dim_fuel_type")
    assert_non_negative(dim_df, ["emissions_factor_kg_per_mwh"], "gold.dim_fuel_type")
    return dim_df


def write_dimension(df: DataFrame, output_path: str) -> None:
    """Write a non-empty dimension dataset as parquet."""

    if df.isEmpty():
        return
    df.write.mode("overwrite").parquet(output_path)


def main() -> None:
    """Build the curated Gold fact and dimension datasets."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("gold_region_fuel_serving_hourly", config)
    logger = logging.getLogger(__name__)
    log_job_start(
        logger,
        "gold_region_fuel_serving_hourly",
        dataset_id=args.dataset,
        input_path=args.silver_base_path,
        output_path=f"{args.region_fact_path},{args.fuel_fact_path}",
        start=args.start,
        end=args.end,
    )
    region_df: DataFrame | None = None
    fuel_df: DataFrame | None = None

    if args.dataset in {"electricity_region_data", "all"}:
        region_input_path = f"{args.silver_base_path}/region_data"
        if has_partitioned_parquet_input(spark, region_input_path):
            region_df = read_partitioned_parquet(
                spark,
                region_input_path,
                retries=SILVER_READ_RETRIES,
                retry_delay_seconds=SILVER_READ_RETRY_DELAY_SECONDS,
            )
            region_df = filter_time_window(region_df, "period", args.start, args.end)
            region_gold_df = build_region_hourly_metrics(region_df)
            write_partitioned(
                region_gold_df,
                args.region_fact_path,
                merge_keys=["period", "respondent"],
            )
        else:
            logger.info(
                "Skipping Gold region fact build because no Silver input is available input_path=%s start=%s end=%s",
                region_input_path,
                args.start,
                args.end,
            )

    if args.dataset in {"electricity_fuel_type_data", "all"}:
        fuel_input_path = f"{args.silver_base_path}/fuel_type_data"
        if has_partitioned_parquet_input(spark, fuel_input_path):
            fuel_df = read_partitioned_parquet(
                spark,
                fuel_input_path,
                retries=SILVER_READ_RETRIES,
                retry_delay_seconds=SILVER_READ_RETRY_DELAY_SECONDS,
            )
            fuel_df = filter_time_window(fuel_df, "period", args.start, args.end)
            fuel_gold_df = build_fuel_type_hourly_generation(fuel_df)
            write_partitioned(
                fuel_gold_df,
                args.fuel_fact_path,
                merge_keys=["period", "respondent", "fueltype"],
            )
        else:
            logger.info(
                "Skipping Gold fuel fact build because no Silver input is available input_path=%s start=%s end=%s",
                fuel_input_path,
                args.start,
                args.end,
            )

    if region_df is not None or fuel_df is not None:
        respondent_dim_df = build_respondent_dimension(
            spark, region_df, fuel_df, args.respondent_dim_path
        )
        write_dimension(respondent_dim_df, args.respondent_dim_path)

    if fuel_df is not None:
        fuel_dim_df = build_fuel_type_dimension(spark, fuel_df, args.fuel_dim_path)
        write_dimension(fuel_dim_df, args.fuel_dim_path)
    log_job_complete(
        logger,
        "gold_region_fuel_serving_hourly",
        dataset_id=args.dataset,
        input_path=args.silver_base_path,
        output_path=f"{args.region_fact_path},{args.fuel_fact_path}",
        start=args.start,
        end=args.end,
    )


if __name__ == "__main__":
    main()
