"""Bronze-to-Silver cleanup job for dataset-specific raw events.

This job reads Bronze event envelopes, keeps only valid rows for the requested
dataset window, and writes Silver parquet partitions that downstream Gold jobs
can trust.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import coalesce, col, count, current_timestamp, lower, row_number, to_date, to_timestamp, trim

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_spark_app_config
from common.io import path_exists, write_partitioned_parquet
from common.logging_utils import configure_logging, log_job_complete, log_job_start
from common.quality import assert_allowed_values, assert_no_conflicting_records, assert_no_nulls, assert_unique_keys
from common.spark_session import build_spark_session
from common.windowing import filter_time_window

logger = logging.getLogger(__name__)
SUPPORTED_DATASETS = ["electricity_region_data", "electricity_fuel_type_data"]
ALLOWED_REGION_TYPES = ["D", "DF"]
ALLOWED_VALUE_UNITS = ["mwh", "megawatthours"]


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the Silver cleanup job."""

    parser = argparse.ArgumentParser(description="Bronze to Silver cleanup transform.")
    parser.add_argument("--bronze-path", default="s3a://bronze/events")
    parser.add_argument("--silver-base-path", default="s3a://silver")
    parser.add_argument("--dataset", choices=SUPPORTED_DATASETS, help="If omitted, process all supported datasets.")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--validation-only", action="store_true")
    return parser.parse_args()


def _parsed_period(column: Column) -> Column:
    """Parse the EIA period column using the supported timestamp formats."""

    return coalesce(
        to_timestamp(column),
        to_timestamp(column, "yyyy-MM-dd'T'HH"),
        to_timestamp(column, "yyyy-MM-dd'T'HH:mm"),
        to_timestamp(column, "yyyy-MM-dd'T'HH:mm:ss"),
    )


def _normalized_text(column: Column) -> Column:
    """Trim text fields so downstream dedupe is not whitespace-sensitive."""

    return trim(column)


def _normalized_unit(column: Column) -> Column:
    """Normalize unit labels to lower case for stable validation."""

    return lower(trim(column))


def _latest_by_event(df: DataFrame) -> DataFrame:
    """Keep the newest cleaned record for each event id."""

    event_window = Window.partitionBy("event_id").orderBy(col("loaded_at").desc(), col("period").desc())
    return df.withColumn("event_rank", row_number().over(event_window)).filter(col("event_rank") == 1).drop("event_rank")


def _validate_cleaning_ratio(raw_df: DataFrame, cleaned_df: DataFrame, dataset_id: str, dataset_name: str) -> None:
    """Fail when a dataset window loses too many rows during cleaning."""

    raw_dataset_df = raw_df.filter(col("dataset") == dataset_id)
    raw_count = raw_dataset_df.select(count("*").alias("row_count")).collect()[0]["row_count"]
    if raw_count == 0:
        return
    cleaned_count = cleaned_df.select(count("*").alias("row_count")).collect()[0]["row_count"]
    if cleaned_count == 0:
        raise ValueError(f"Validation failed: source rows were present, but no valid rows were produced for {dataset_name} in the requested window")
    retention_ratio = cleaned_count / raw_count
    if retention_ratio < 0.8:
        raise ValueError(
            f"Validation failed: only {cleaned_count} of {raw_count} source rows were retained for {dataset_name} "
            f"({retention_ratio:.1%} retention)"
        )


def clean_region_data(df: DataFrame) -> DataFrame:
    """Normalize region demand and forecast rows into the Silver region schema."""

    parsed_period = _parsed_period(col("payload")["period"])
    extracted_df = (
        df.filter(col("dataset") == "electricity_region_data")
        .select(
            col("event_id"),
            parsed_period.alias("period"),
            _normalized_text(col("payload")["respondent"]).alias("respondent"),
            _normalized_text(col("payload")["respondent-name"]).alias("respondent_name"),
            _normalized_text(col("payload")["type"]).alias("type"),
            _normalized_text(col("payload")["type-name"]).alias("type_name"),
            col("payload")["value"].cast("double").alias("value"),
            _normalized_unit(col("payload")["value-units"]).alias("value_units"),
            coalesce(col("ingestion_ts"), current_timestamp()).alias("loaded_at"),
        )
    )
    assert_no_conflicting_records(
        extracted_df.filter(col("event_id").isNotNull()),
        ["event_id"],
        ["period", "respondent", "respondent_name", "type", "type_name", "value", "value_units"],
        "silver.region_data",
    )
    cleaned_df = (
        extracted_df
        .filter(col("period").isNotNull() & col("respondent").isNotNull() & col("value").isNotNull())
        .filter(col("respondent_name").isNotNull() & col("type").isNotNull() & col("value_units").isNotNull())
        .transform(_latest_by_event)
        .withColumn("event_date", to_date(col("period")))
    )
    assert_no_nulls(cleaned_df, ["event_id", "period", "respondent", "respondent_name", "value", "event_date"], "silver.region_data")
    assert_allowed_values(cleaned_df, "type", ALLOWED_REGION_TYPES, "silver.region_data")
    assert_allowed_values(cleaned_df, "value_units", ALLOWED_VALUE_UNITS, "silver.region_data")
    assert_unique_keys(cleaned_df, ["event_id"], "silver.region_data")
    return cleaned_df


def clean_fuel_type_data(df: DataFrame) -> DataFrame:
    """Normalize fuel-type rows into the Silver fuel schema."""

    parsed_period = _parsed_period(col("payload")["period"])
    extracted_df = (
        df.filter(col("dataset") == "electricity_fuel_type_data")
        .select(
            col("event_id"),
            parsed_period.alias("period"),
            _normalized_text(col("payload")["respondent"]).alias("respondent"),
            _normalized_text(col("payload")["respondent-name"]).alias("respondent_name"),
            _normalized_text(col("payload")["fueltype"]).alias("fueltype"),
            _normalized_text(col("payload")["type-name"]).alias("fueltype_name"),
            col("payload")["value"].cast("double").alias("value"),
            _normalized_unit(col("payload")["value-units"]).alias("value_units"),
            coalesce(col("ingestion_ts"), current_timestamp()).alias("loaded_at"),
        )
    )
    assert_no_conflicting_records(
        extracted_df.filter(col("event_id").isNotNull()),
        ["event_id"],
        ["period", "respondent", "respondent_name", "fueltype", "fueltype_name", "value", "value_units"],
        "silver.fuel_type_data",
    )
    cleaned_df = (
        extracted_df
        .filter(col("period").isNotNull() & col("respondent").isNotNull() & col("value").isNotNull())
        .filter(col("respondent_name").isNotNull() & col("fueltype").isNotNull() & col("value_units").isNotNull())
        .transform(_latest_by_event)
        .withColumn("event_date", to_date(col("period")))
    )
    assert_no_nulls(cleaned_df, ["event_id", "period", "respondent", "respondent_name", "fueltype", "value", "event_date"], "silver.fuel_type_data")
    assert_allowed_values(cleaned_df, "value_units", ALLOWED_VALUE_UNITS, "silver.fuel_type_data")
    assert_unique_keys(cleaned_df, ["event_id"], "silver.fuel_type_data")
    return cleaned_df


def validate_non_empty(raw_df: DataFrame, cleaned_df: DataFrame, dataset_id: str, dataset_name: str) -> None:
    """Public validation wrapper kept for tests and pipeline readability."""

    _validate_cleaning_ratio(raw_df, cleaned_df, dataset_id, dataset_name)


def write_partitioned_dataset(df: DataFrame, output_path: str) -> None:
    """Write a non-empty Silver dataset partitioned by event date."""

    write_partitioned_parquet(df, output_path, partition_column="event_date")


def main() -> None:
    """Run cleanup for one dataset or both supported Silver datasets."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("silver_clean_transform", config)
    bronze_glob = f"{args.bronze_path}/dataset_partition=*/event_year=*/event_month=*/event_day=*/event_hour=*"
    bronze_root = args.bronze_path.rstrip("/")
    log_job_start(
        logger,
        "silver_clean_transform",
        dataset_id=args.dataset or "all",
        input_path=bronze_root,
        output_path=args.silver_base_path,
        start=args.start,
        end=args.end,
    )
    if not path_exists(spark, bronze_root):
        logger.info("Skipping Silver cleanup because the Bronze path does not exist input_path=%s", bronze_root)
        return
    bronze_df = (
        spark.read.option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(bronze_glob)
    )
    bronze_df = filter_time_window(bronze_df, "event_ts", args.start, args.end)
    raw_count = bronze_df.count()

    datasets_to_run = [args.dataset] if args.dataset else SUPPORTED_DATASETS
    if "electricity_region_data" in datasets_to_run:
        region_df = clean_region_data(bronze_df)
        if args.validation_only:
            validate_non_empty(bronze_df, region_df, "electricity_region_data", "silver.region_data")
        else:
            validate_non_empty(bronze_df, region_df, "electricity_region_data", "silver.region_data")
            write_partitioned_dataset(region_df, f"{args.silver_base_path}/region_data")
        logger.info(
            "Processed Silver dataset dataset_id=electricity_region_data input_path=%s output_path=%s start=%s end=%s raw_count=%s clean_count=%s",
            bronze_root,
            f"{args.silver_base_path}/region_data",
            args.start,
            args.end,
            raw_count,
            region_df.count(),
        )

    if "electricity_fuel_type_data" in datasets_to_run:
        fuel_df = clean_fuel_type_data(bronze_df)
        if args.validation_only:
            validate_non_empty(bronze_df, fuel_df, "electricity_fuel_type_data", "silver.fuel_type_data")
        else:
            validate_non_empty(bronze_df, fuel_df, "electricity_fuel_type_data", "silver.fuel_type_data")
            write_partitioned_dataset(fuel_df, f"{args.silver_base_path}/fuel_type_data")
        logger.info(
            "Processed Silver dataset dataset_id=electricity_fuel_type_data input_path=%s output_path=%s start=%s end=%s raw_count=%s clean_count=%s",
            bronze_root,
            f"{args.silver_base_path}/fuel_type_data",
            args.start,
            args.end,
            raw_count,
            fuel_df.count(),
        )

    log_job_complete(
        logger,
        "silver_clean_transform",
        dataset_id=args.dataset or "all",
        input_path=bronze_root,
        output_path=args.silver_base_path,
        start=args.start,
        end=args.end,
        raw_count=raw_count,
    )


if __name__ == "__main__":
    main()
