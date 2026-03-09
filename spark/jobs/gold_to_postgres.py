"""
gold_to_minio.py
----------------
Spark job: Gold Layer
  - Reads clean Parquet from MinIO silver/
  - Produces business-level aggregations:
      * Hourly generation mix by fuel type per balancing authority
      * Daily generation totals per fuel type
      * Hourly demand vs forecast per region
      * Daily peak demand per region
  - Writes aggregated Parquet to MinIO gold/

MinIO paths:
    Input:  s3a://silver/eia/electricity_generation/date=<date>/
            s3a://silver/eia/electricity_demand/date=<date>/
    Output: s3a://gold/eia/generation_mix_hourly/date=<date>/
            s3a://gold/eia/generation_daily_totals/date=<date>/
            s3a://gold/eia/demand_hourly/date=<date>/
            s3a://gold/eia/demand_daily_peak/date=<date>/
"""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    to_date,
)

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_gold(df: DataFrame, path: str) -> None:
    gold_df = df.withColumn("gold_processed_at", current_timestamp())
    count = gold_df.count()
    logger.info("Writing %d records to %s", count, path)
    (
        gold_df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(path)
    )
    logger.info("Gold write complete → %s", path)


def aggregate_generation(spark: SparkSession, date: str) -> None:
    silver_path = f"s3a://silver/eia/electricity_generation/date={date}"
    df = spark.read.parquet(silver_path)

    # 1. Hourly generation mix by fuel type per balancing authority
    hourly_mix = (
        df.groupBy("period_ts", "respondent", "respondent_name", "fueltype", "fuel_type_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("total_gwh"))
        .orderBy("period_ts", "respondent", "fueltype")
    )
    write_gold(hourly_mix, f"s3a://gold/eia/generation_mix_hourly/date={date}")

    # 2. Daily generation totals per fuel type (all BAs combined)
    daily_totals = (
        df.withColumn("date", to_date("period_ts"))
        .groupBy("date", "fueltype", "fuel_type_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("total_gwh"))
        .orderBy("date", "fueltype")
    )
    write_gold(daily_totals, f"s3a://gold/eia/generation_daily_totals/date={date}")


def aggregate_demand(spark: SparkSession, date: str) -> None:
    silver_path = f"s3a://silver/eia/electricity_demand/date={date}"
    df = spark.read.parquet(silver_path)

    # 3. Hourly demand vs forecast per region
    hourly_demand = (
        df.groupBy("period_ts", "respondent", "respondent_name", "type", "demand_type_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("total_gwh"))
        .orderBy("period_ts", "respondent", "type")
    )
    write_gold(hourly_demand, f"s3a://gold/eia/demand_hourly/date={date}")

    # 4. Daily peak demand per region (demand only, not forecast)
    daily_peak = (
        df.filter(col("type") == "D")
        .withColumn("date", to_date("period_ts"))
        .groupBy("date", "respondent", "respondent_name")
        .agg(spark_round(spark_max("value_gwh"), 4).alias("peak_gwh"))
        .orderBy("date", "respondent")
    )
    write_gold(daily_peak, f"s3a://gold/eia/demand_daily_peak/date={date}")


def run(date: str) -> None:
    app_name = f"gold_aggregations_{date}"
    spark = build_spark_session(app_name)

    logger.info("Building gold aggregations for date: %s", date)
    aggregate_generation(spark, date)
    aggregate_demand(spark, date)

    logger.info("All gold aggregations complete for %s", date)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold: Aggregate & Enrich")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.date)