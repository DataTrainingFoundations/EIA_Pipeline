"""Curated Gold fact builder for monthly electric power operations."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_spark_app_config
from common.io import has_partitioned_parquet_input, read_partitioned_parquet, write_partitioned_parquet
from common.logging_utils import configure_logging, log_job_complete, log_job_start
from common.quality import assert_no_nulls, assert_non_negative, assert_unique_keys
from common.spark_session import build_spark_session
from common.windowing import filter_time_window

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the monthly power Gold job."""

    parser = argparse.ArgumentParser(description="Build curated Gold monthly electric power operations fact data.")
    parser.add_argument("--silver-input-path", default="s3a://silver/electric_power_operational_data")
    parser.add_argument("--gold-output-path", default="s3a://gold/facts/electric_power_operations_monthly")
    parser.add_argument("--start")
    parser.add_argument("--end")
    return parser.parse_args()


def build_power_operations_monthly_fact(power_df: DataFrame) -> DataFrame:
    """Build the curated monthly power operations Gold fact table."""

    latest_window = Window.partitionBy("period", "location", "sector_id", "fueltype_id").orderBy(F.col("loaded_at").desc(), F.col("event_id").desc())
    gold_df = (
        power_df.withColumn("record_rank", F.row_number().over(latest_window))
        .filter(F.col("record_rank") == 1)
        .drop("record_rank")
        .withColumn("generation_mwh", F.when(F.col("generation_thousand_mwh").isNotNull(), F.greatest(F.col("generation_thousand_mwh"), F.lit(0.0)) * F.lit(1000.0)))
        .withColumn(
            "consumption_for_eg_thousand_units",
            F.when(F.col("consumption_for_eg_thousand_units").isNotNull(), F.greatest(F.col("consumption_for_eg_thousand_units"), F.lit(0.0))),
        )
        .withColumn(
            "ash_content_pct",
            F.when(F.col("ash_content_pct").isNotNull(), F.greatest(F.col("ash_content_pct"), F.lit(0.0))),
        )
        .withColumn(
            "heat_content_btu_per_unit",
            F.when(F.col("heat_content_btu_per_unit").isNotNull(), F.greatest(F.col("heat_content_btu_per_unit"), F.lit(0.0))),
        )
        .select(
            "period",
            "event_date",
            "location",
            "location_name",
            "sector_id",
            "sector_name",
            "fueltype_id",
            "fueltype_name",
            "ash_content_pct",
            "consumption_for_eg_thousand_units",
            "generation_mwh",
            "heat_content_btu_per_unit",
            "loaded_at",
        )
        .orderBy("period", "sector_id", "fueltype_id")
    )
    assert_no_nulls(
        gold_df,
        ["period", "event_date", "location", "location_name", "sector_id", "sector_name", "fueltype_id", "fueltype_name", "loaded_at"],
        "gold.fact_electric_power_operations_monthly",
    )
    assert_unique_keys(gold_df, ["period", "location", "sector_id", "fueltype_id"], "gold.fact_electric_power_operations_monthly")
    assert_non_negative(
        gold_df,
        ["ash_content_pct", "consumption_for_eg_thousand_units", "generation_mwh", "heat_content_btu_per_unit"],
        "gold.fact_electric_power_operations_monthly",
    )
    return gold_df


def main() -> None:
    """Build and write the curated monthly power operations Gold fact table."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("gold_power_operations_monthly", config)
    log_job_start(
        logger,
        "gold_power_operations_monthly",
        input_path=args.silver_input_path,
        output_path=args.gold_output_path,
        start=args.start,
        end=args.end,
    )

    if not has_partitioned_parquet_input(spark, args.silver_input_path):
        logger.info("Skipping monthly power Gold build because no Silver input is available input_path=%s start=%s end=%s", args.silver_input_path, args.start, args.end)
        return

    power_df = read_partitioned_parquet(spark, args.silver_input_path)
    power_df = filter_time_window(power_df, "period", args.start, args.end)
    gold_df = build_power_operations_monthly_fact(power_df)
    write_partitioned_parquet(gold_df, args.gold_output_path, partition_column="event_date")
    log_job_complete(
        logger,
        "gold_power_operations_monthly",
        input_path=args.silver_input_path,
        output_path=args.gold_output_path,
        start=args.start,
        end=args.end,
        row_count=gold_df.count(),
    )


if __name__ == "__main__":
    main()
