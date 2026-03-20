"""Monthly platinum stage builder for electric power operations."""

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
from common.quality import (
    assert_no_nulls,
    assert_non_negative,
    assert_unique_keys,
    assert_value_bounds,
)
from common.spark_session import build_spark_session
from common.windowing import filter_time_window

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the monthly power platinum job."""

    parser = argparse.ArgumentParser(
        description="Build monthly electric power operations serving tables."
    )
    parser.add_argument(
        "--gold-input-path",
        default="s3a://gold/facts/electric_power_operations_monthly",
    )
    parser.add_argument(
        "--platinum-table", default="platinum.electric_power_operations_monthly"
    )
    parser.add_argument(
        "--stage-table", default="platinum.electric_power_operations_monthly_stage"
    )
    parser.add_argument("--start")
    parser.add_argument("--end")
    return parser.parse_args()


def build_power_operations_monthly(
    gold_df: DataFrame, start: str | None, end: str | None
) -> DataFrame:
    """Build the monthly power operations platinum stage table."""

    total_generation_window = Window.partitionBy("period", "location")
    platinum_df = (
        gold_df.withColumn(
            "period_generation_mwh",
            F.sum("generation_mwh").over(total_generation_window),
        )
        .withColumn(
            "generation_share_pct",
            F.when(
                F.col("period_generation_mwh") > 0,
                (F.col("generation_mwh") / F.col("period_generation_mwh"))
                * F.lit(100.0),
            ),
        )
        .withColumn(
            "fuel_heat_input_mmbtu",
            F.when(
                F.col("consumption_for_eg_thousand_units").isNotNull()
                & F.col("heat_content_btu_per_unit").isNotNull(),
                F.col("consumption_for_eg_thousand_units")
                * F.col("heat_content_btu_per_unit")
                * F.lit(1000.0),
            ),
        )
        .withColumn(
            "heat_rate_btu_per_kwh",
            F.when(
                F.col("generation_mwh") > 0,
                (F.col("fuel_heat_input_mmbtu") * F.lit(1000.0))
                / F.col("generation_mwh"),
            ),
        )
        .withColumn("source_window_start", F.to_timestamp(F.lit(start)))
        .withColumn("source_window_end", F.to_timestamp(F.lit(end)))
        .withColumn("updated_at", F.current_timestamp())
        .drop("period_generation_mwh")
        .select(
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
            "fuel_heat_input_mmbtu",
            "heat_rate_btu_per_kwh",
            "loaded_at",
            "source_window_start",
            "source_window_end",
            "updated_at",
        )
        .orderBy("period", "sector_id", "fueltype_id")
    )
    assert_no_nulls(
        platinum_df,
        [
            "period",
            "location",
            "location_name",
            "sector_id",
            "sector_name",
            "fueltype_id",
            "fueltype_name",
            "loaded_at",
        ],
        "platinum.electric_power_operations_monthly_stage",
    )
    assert_unique_keys(
        platinum_df,
        ["period", "location", "sector_id", "fueltype_id"],
        "platinum.electric_power_operations_monthly_stage",
    )
    assert_non_negative(
        platinum_df,
        [
            "generation_mwh",
            "generation_share_pct",
            "consumption_for_eg_thousand_units",
            "ash_content_pct",
            "heat_content_btu_per_unit",
            "fuel_heat_input_mmbtu",
            "heat_rate_btu_per_kwh",
        ],
        "platinum.electric_power_operations_monthly_stage",
    )
    assert_value_bounds(
        platinum_df,
        "generation_share_pct",
        min_value=0.0,
        max_value=100.0,
        label="platinum.electric_power_operations_monthly_stage",
    )
    return platinum_df


def main() -> None:
    """Build and write the monthly power operations platinum stage table."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("platinum_power_operations_monthly", config)
    log_job_start(
        logger,
        "platinum_power_operations_monthly",
        input_path=args.gold_input_path,
        stage_table=args.stage_table,
        start=args.start,
        end=args.end,
    )

    jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/platform")
    jdbc_props = {
        "user": os.getenv("POSTGRES_USER", "platform"),
        "password": os.getenv("POSTGRES_PASSWORD", "platform"),
        "driver": "org.postgresql.Driver",
    }

    if not has_partitioned_parquet_input(spark, args.gold_input_path):
        return

    gold_df = read_partitioned_parquet(spark, args.gold_input_path)
    gold_df = filter_time_window(gold_df, "period", args.start, args.end)
    platinum_df = build_power_operations_monthly(gold_df, args.start, args.end)

    if platinum_df.limit(1).count() == 0:
        return

    platinum_df.write.mode("overwrite").jdbc(
        jdbc_url, args.stage_table, properties=jdbc_props
    )
    log_job_complete(
        logger,
        "platinum_power_operations_monthly",
        stage_table=args.stage_table,
        start=args.start,
        end=args.end,
        row_count=platinum_df.count(),
    )


if __name__ == "__main__":
    main()
