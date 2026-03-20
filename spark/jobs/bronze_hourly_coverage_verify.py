"""Bronze hourly coverage verification job.

This job scans Bronze partitions for one dataset, calculates expected versus
observed hourly coverage, and writes the result into a warehouse stage table so
Airflow can merge it and decide whether repair work is needed.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_spark_app_config
from common.io import path_exists
from common.logging_utils import configure_logging, log_job_complete, log_job_start
from common.spark_session import build_spark_session

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the Bronze coverage verification job."""

    parser = argparse.ArgumentParser(description="Verify Bronze hourly coverage.")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--verification-stage-table", required=True)
    return parser.parse_args()


def _bronze_glob(base_path: str) -> str:
    """Return the Bronze parquet glob covering every dataset/hour partition."""

    return f"{base_path.rstrip('/')}/dataset_partition=*/event_year=*/event_month=*/event_day=*/event_hour=*"


def _expected_row_count(hourly_counts_df: DataFrame) -> int | None:
    """Use the modal hourly count as the expected row count for this dataset."""

    rows = (
        hourly_counts_df.filter(F.col("observed_row_count") > 0)
        .groupBy("observed_row_count")
        .count()
        .orderBy(F.desc("count"), F.desc("observed_row_count"))
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return int(rows[0]["observed_row_count"])


def build_hourly_coverage_df(
    bronze_df: DataFrame, dataset_id: str, verification_boundary_hour: datetime
) -> DataFrame:
    """Build the hourly coverage DataFrame Airflow will merge into metadata tables."""

    hourly_counts_df = (
        bronze_df.filter(F.col("event_ts").isNotNull())
        .groupBy(F.date_trunc("hour", F.col("event_ts")).alias("hour_start_utc"))
        .agg(F.count("*").alias("observed_row_count"))
    )

    min_max_row = hourly_counts_df.agg(
        F.min("hour_start_utc").alias("oldest_hour_utc"),
        F.max("hour_start_utc").alias("newest_observed_hour_utc"),
    ).collect()[0]
    oldest_hour_utc = min_max_row["oldest_hour_utc"]
    newest_observed_hour_utc = min_max_row["newest_observed_hour_utc"]
    if oldest_hour_utc is None or newest_observed_hour_utc is None:
        return hourly_counts_df.limit(0)

    _ = newest_observed_hour_utc
    newest_hour_utc = max(
        oldest_hour_utc,
        verification_boundary_hour.replace(tzinfo=None) - timedelta(hours=1),
    )
    expected_row_count = _expected_row_count(hourly_counts_df)
    bounds_df = bronze_df.sparkSession.createDataFrame(
        [(oldest_hour_utc, newest_hour_utc)],
        ["oldest_hour_utc", "newest_hour_utc"],
    )
    hourly_window_df = bounds_df.select(
        "oldest_hour_utc",
        "newest_hour_utc",
        F.explode(
            F.expr("sequence(oldest_hour_utc, newest_hour_utc, interval 1 hour)")
        ).alias("hour_start_utc"),
    )

    coverage_df = (
        hourly_window_df.join(hourly_counts_df, ["hour_start_utc"], "left")
        .fillna({"observed_row_count": 0})
        .withColumn("dataset_id", F.lit(dataset_id))
        .withColumn("expected_row_count", F.lit(expected_row_count).cast("integer"))
        .withColumn(
            "row_count_delta",
            F.when(
                F.col("expected_row_count").isNull(), F.lit(None).cast("integer")
            ).otherwise(F.col("observed_row_count") - F.col("expected_row_count")),
        )
        .withColumn(
            "status",
            F.when(F.col("observed_row_count") == 0, F.lit("missing"))
            .when(F.col("expected_row_count").isNull(), F.lit("observed"))
            .when(
                F.col("observed_row_count") == F.col("expected_row_count"),
                F.lit("verified"),
            )
            .when(
                F.col("observed_row_count") < F.col("expected_row_count"),
                F.lit("partial"),
            )
            .otherwise(F.lit("excess")),
        )
        .withColumn("verification_boundary_hour_utc", F.lit(verification_boundary_hour))
        .withColumn("verified_at", F.current_timestamp())
        .select(
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
        )
        .orderBy("hour_start_utc")
    )
    return coverage_df


def main() -> None:
    """Verify Bronze hourly coverage for one dataset and write a stage table."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("bronze_hourly_coverage_verify", config)
    log_job_start(
        logger,
        "bronze_hourly_coverage_verify",
        dataset_id=args.dataset_id,
        input_path=args.bronze_path,
        stage_table=args.verification_stage_table,
    )

    bronze_root = args.bronze_path.rstrip("/")
    if not path_exists(spark, bronze_root):
        logger.info(
            "Bronze path does not exist; skipping verification dataset_id=%s bronze_path=%s",
            args.dataset_id,
            bronze_root,
        )
        return

    bronze_df = spark.read.option("recursiveFileLookup", "true").parquet(
        _bronze_glob(bronze_root)
    )
    dataset_column = (
        "dataset_partition"
        if "dataset_partition" in bronze_df.columns
        else "dataset" if "dataset" in bronze_df.columns else None
    )
    if dataset_column is None:
        logger.error(
            "Bronze schema is missing dataset discriminator column dataset_id=%s columns=%s",
            args.dataset_id,
            bronze_df.columns,
        )
        raise ValueError(
            f"Bronze schema is missing dataset discriminator column for {args.dataset_id}"
        )

    logger.info(
        "Using dataset discriminator column dataset_id=%s column=%s",
        args.dataset_id,
        dataset_column,
    )
    bronze_df = bronze_df.filter(F.col(dataset_column) == args.dataset_id)
    if bronze_df.limit(1).count() == 0:
        logger.info(
            "Bronze path exists but contains no rows for dataset_id=%s bronze_path=%s",
            args.dataset_id,
            bronze_root,
        )
        return

    verification_boundary_hour = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    coverage_df = build_hourly_coverage_df(
        bronze_df, args.dataset_id, verification_boundary_hour
    )
    if coverage_df.limit(1).count() == 0:
        logger.info(
            "Coverage dataframe is empty; skipping write dataset_id=%s boundary_hour=%s",
            args.dataset_id,
            verification_boundary_hour.isoformat(),
        )
        return

    row_count = coverage_df.count()
    summary_rows = coverage_df.groupBy("status").count().orderBy("status").collect()
    summary = {row["status"]: int(row["count"]) for row in summary_rows}
    logger.info(
        "Writing bronze coverage dataset_id=%s stage_table=%s row_count=%s status_summary=%s boundary_hour=%s",
        args.dataset_id,
        args.verification_stage_table,
        row_count,
        summary,
        verification_boundary_hour.isoformat(),
    )

    jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/platform")
    jdbc_props = {
        "user": os.getenv("POSTGRES_USER", "platform"),
        "password": os.getenv("POSTGRES_PASSWORD", "platform"),
        "driver": "org.postgresql.Driver",
    }
    coverage_df.write.mode("overwrite").jdbc(
        jdbc_url, args.verification_stage_table, properties=jdbc_props
    )
    log_job_complete(
        logger,
        "bronze_hourly_coverage_verify",
        dataset_id=args.dataset_id,
        input_path=bronze_root,
        stage_table=args.verification_stage_table,
        row_count=row_count,
    )


if __name__ == "__main__":
    main()
