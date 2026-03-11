"""
platinum_serving_tables.py
---------------------------
Spark job: Platinum Layer
  - Reads aggregated Parquet from MinIO gold/
  - Builds final denormalized serving tables optimized for the Streamlit app
  - Writes platinum Parquet to MinIO (s3a://gold/eia/platinum_*/date=<date>/)
  - Outputs are structured to match PostgreSQL warehouse schema exactly
    so the Airflow DAG can INSERT directly using psycopg2 or COPY

Serving tables produced:
    fact_generation_hourly    — hourly generation by fuel type per BA
    fact_demand_hourly        — hourly demand + forecast per region
    dim_balancing_authority   — BA dimension (upsert-safe)
    dim_fuel_type             — fuel type dimension (upsert-safe)
    agg_daily_generation      — daily rollup for dashboard charting
    agg_daily_demand_peak     — daily peak demand for KPI tiles
"""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    md5,
    concat_ws,
)

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

PLATINUM_BASE = "s3a://gold/eia"  # stored in gold bucket, platinum prefix


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_platinum(df: DataFrame, table_name: str, date: str) -> None:
    path = f"{PLATINUM_BASE}/platinum_{table_name}/date={date}"
    out_df = df.withColumn("platinum_processed_at", current_timestamp())
    count = out_df.count()
    logger.info("Writing platinum table '%s': %d rows → %s", table_name, count, path)
    (
        out_df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(path)
    )
    logger.info("Platinum write complete → %s", path)


def build_fact_generation_hourly(spark: SparkSession, date: str) -> None:
    """
    Fact table: one row per (period_ts, balancing_authority, fuel_type).
    Matches PostgreSQL table: fact_generation_hourly
    """
    df = spark.read.parquet(f"s3a://gold/eia/generation_mix_hourly/date={date}")
    fact = (
        df.select(
            md5(concat_ws("|", col("period_ts").cast("string"), col("respondent"), col("fueltype")))
                .alias("record_id"),
            col("period_ts"),
            col("respondent").alias("ba_code"),
            col("respondent_name").alias("ba_name"),
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
            col("total_gwh").alias("generation_gwh"),
            lit(date).alias("partition_date"),
        )
    )
    write_platinum(fact, "fact_generation_hourly", date)


def build_fact_demand_hourly(spark: SparkSession, date: str) -> None:
    """
    Fact table: one row per (period_ts, region, demand_type).
    Matches PostgreSQL table: fact_demand_hourly
    Separates actual demand (D) from forecast (DF) into columns.
    """
    df = spark.read.parquet(f"s3a://gold/eia/demand_hourly/date={date}")

    demand = df.filter(col("type") == "D").select(
        col("period_ts"),
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
        col("total_gwh").alias("demand_gwh"),
    )
    forecast = df.filter(col("type") == "DF").select(
        col("period_ts"),
        col("respondent").alias("ba_code"),
        col("total_gwh").alias("forecast_gwh"),
    )

    fact = (
        demand.join(forecast, on=["period_ts", "ba_code"], how="left")
        .select(
            md5(concat_ws("|", col("period_ts").cast("string"), col("ba_code")))
                .alias("record_id"),
            col("period_ts"),
            col("ba_code"),
            col("ba_name"),
            col("demand_gwh"),
            col("forecast_gwh"),
            lit(date).alias("partition_date"),
        )
    )
    write_platinum(fact, "fact_demand_hourly", date)


def build_dim_balancing_authority(spark: SparkSession, date: str) -> None:
    """
    Dimension table: unique balancing authorities.
    Matches PostgreSQL table: dim_balancing_authority (upsert on ba_code).
    """
    gen_df = spark.read.parquet(f"s3a://gold/eia/generation_mix_hourly/date={date}") \
        .select(col("respondent").alias("ba_code"), col("respondent_name").alias("ba_name"))
    dem_df = spark.read.parquet(f"s3a://gold/eia/demand_hourly/date={date}") \
        .select(col("respondent").alias("ba_code"), col("respondent_name").alias("ba_name"))

    dim = (
        gen_df.union(dem_df)
        .dropDuplicates(["ba_code"])
        .select("ba_code", "ba_name")
        .orderBy("ba_code")
    )
    write_platinum(dim, "dim_balancing_authority", date)


def build_dim_fuel_type(spark: SparkSession, date: str) -> None:
    """
    Dimension table: fuel type codes and descriptions.
    Matches PostgreSQL table: dim_fuel_type (upsert on fuel_code).
    """
    df = spark.read.parquet(f"s3a://gold/eia/generation_mix_hourly/date={date}")
    dim = (
        df.select(
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
        )
        .dropDuplicates(["fuel_code"])
        .orderBy("fuel_code")
    )
    write_platinum(dim, "dim_fuel_type", date)


def build_agg_daily_generation(spark: SparkSession, date: str) -> None:
    """
    Daily rollup: total GWh per fuel type across all BAs.
    Matches PostgreSQL table: agg_daily_generation — used for trend charts.
    """
    df = spark.read.parquet(f"s3a://gold/eia/generation_daily_totals/date={date}")
    agg = df.select(
        col("date").alias("report_date"),
        col("fueltype").alias("fuel_code"),
        col("fuel_type_name").alias("fuel_name"),
        col("total_gwh"),
        lit(date).alias("partition_date"),
    )
    write_platinum(agg, "agg_daily_generation", date)


def build_agg_daily_demand_peak(spark: SparkSession, date: str) -> None:
    """
    Daily peak demand per BA.
    Matches PostgreSQL table: agg_daily_demand_peak — used for KPI tiles.
    """
    df = spark.read.parquet(f"s3a://gold/eia/demand_daily_peak/date={date}")
    agg = df.select(
        col("date").alias("report_date"),
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
        col("peak_gwh"),
        lit(date).alias("partition_date"),
    )
    write_platinum(agg, "agg_daily_demand_peak", date)


def run(date: str) -> None:
    app_name = f"platinum_serving_{date}"
    spark = build_spark_session(app_name)

    logger.info("Building platinum serving tables for date: %s", date)

    build_dim_balancing_authority(spark, date)
    build_dim_fuel_type(spark, date)
    build_fact_generation_hourly(spark, date)
    build_fact_demand_hourly(spark, date)
    build_agg_daily_generation(spark, date)
    build_agg_daily_demand_peak(spark, date)

    logger.info("All platinum tables complete for %s", date)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Platinum: Build Serving Tables")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.date)