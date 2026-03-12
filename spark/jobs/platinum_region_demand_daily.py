from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_spark_app_config
from common.quality import assert_no_nulls, assert_non_negative, assert_unique_keys
from common.spark_session import build_spark_session
from common.windowing import filter_time_window


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Curated Gold to Platinum region demand serving table.")
    parser.add_argument("--gold-input-path", default="s3a://gold/facts/region_demand_forecast_hourly")
    parser.add_argument("--platinum-table", default="platinum.region_demand_daily")
    parser.add_argument("--stage-table", default="platinum.region_demand_daily_stage")
    parser.add_argument("--start")
    parser.add_argument("--end")
    return parser.parse_args()


def build_region_demand_daily(gold_df: DataFrame, start: str | None, end: str | None) -> DataFrame:
    platinum_df = (
        gold_df.filter(F.col("actual_demand_mwh").isNotNull())
        .withColumn("date", F.to_date("period"))
        .groupBy("date", "respondent")
        .agg(
            F.sum("actual_demand_mwh").alias("daily_demand_mwh"),
            F.avg("actual_demand_mwh").alias("avg_hourly_demand_mwh"),
            F.max("actual_demand_mwh").alias("peak_hourly_demand_mwh"),
            F.max("loaded_at").alias("loaded_at"),
        )
        .withColumn("source_window_start", F.to_timestamp(F.lit(start)))
        .withColumn("source_window_end", F.to_timestamp(F.lit(end)))
        .withColumn("updated_at", F.current_timestamp())
        .orderBy("date", "respondent")
    )
    assert_no_nulls(
        platinum_df,
        ["date", "respondent", "daily_demand_mwh", "avg_hourly_demand_mwh", "peak_hourly_demand_mwh", "loaded_at"],
        "platinum.region_demand_daily_stage",
    )
    assert_unique_keys(platinum_df, ["date", "respondent"], "platinum.region_demand_daily_stage")
    assert_non_negative(
        platinum_df,
        ["daily_demand_mwh", "avg_hourly_demand_mwh", "peak_hourly_demand_mwh"],
        "platinum.region_demand_daily_stage",
    )
    return platinum_df


def main() -> None:
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("platinum_region_demand_daily", config)

    jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/platform")
    jdbc_props = {
        "user": os.getenv("POSTGRES_USER", "platform"),
        "password": os.getenv("POSTGRES_PASSWORD", "platform"),
        "driver": "org.postgresql.Driver",
    }

    gold_df = spark.read.parquet(args.gold_input_path)
    gold_df = filter_time_window(gold_df, "period", args.start, args.end)
    platinum_df = build_region_demand_daily(gold_df, args.start, args.end)

    if platinum_df.limit(1).count() == 0:
        return

    platinum_df.write.mode("overwrite").jdbc(jdbc_url, args.stage_table, properties=jdbc_props)


if __name__ == "__main__":
    main()
