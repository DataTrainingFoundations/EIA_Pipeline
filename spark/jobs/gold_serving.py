"""
gold_serving.py
===============
Spark Job: Gold Layer (consolidated)
-------------------------------------
Reads clean Parquet from MinIO silver/ and writes ALL serving tables
directly to MinIO gold/ in a single pass.

This replaces the old two-step gold_to_postgres.py → platinum_serving_tables.py
pattern.  The outputs are denormalized and typed to match the PostgreSQL
warehouse schema (002_platinum_schema.sql) exactly, so the Airflow
eia_gold DAG can upsert them straight into Postgres with no further
transformation.

INPUT PATHS
-----------
    s3a://silver/eia/electricity_generation/date=<date>/
    s3a://silver/eia/electricity_demand/date=<date>/

OUTPUT PATHS (all in the gold bucket)
--------------------------------------
    gold/eia/fact_generation_hourly/date=<date>/
    gold/eia/fact_demand_hourly/date=<date>/
    gold/eia/dim_balancing_authority/date=<date>/
    gold/eia/dim_fuel_type/date=<date>/
    gold/eia/agg_daily_generation/date=<date>/
    gold/eia/agg_daily_demand_peak/date=<date>/

USAGE
-----
    spark-submit gold_serving.py --date 2024-03-15

ADDING NEW SERVING TABLES
--------------------------
Add a new _build_<table>() function and call it from run().
The function receives a SparkSession and the date string and should call
write_gold() with the output table name.
"""

from __future__ import annotations

import argparse
import logging
import os

# from pyspark.sql import DataFrame, SparkSession
# from pyspark.sql.functions import (
#     col,
#     concat_ws,
#     current_timestamp,
#     lit,
#     max as spark_max,
#     md5,
#     round as spark_round,
#     sum as spark_sum,
#     to_date,
# )

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    concat_ws,
    current_timestamp,
    lit,
    max as spark_max,
    md5,
    round as spark_round,
    sum as spark_sum,
    to_date,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Config ─────────────────────────────────────────────────────────────────────
# MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_USER     = os.environ.get("MINIO_ROOT_USER", "minioadmin")
# MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# GOLD_BASE = "s3a://gold/eia"


# ── Spark session ──────────────────────────────────────────────────────────────

def _build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


# ── Writer ─────────────────────────────────────────────────────────────────────

def _write(df: DataFrame, table_name: str, date: str) -> None:
    """Write a serving DataFrame to MinIO gold/ with snappy compression."""
    path = f"{GOLD_BASE}/{table_name}/date={date}"
    out  = df.withColumn("gold_processed_at", current_timestamp())
    n    = out.count()
    logger.info("Writing %s: %d rows → %s", table_name, n, path)
    (
        out.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(path)
    )
    logger.info("Done → %s", path)


# ── Serving table builders ─────────────────────────────────────────────────────

def _build_dim_balancing_authority(
    gen_df: DataFrame,
    dem_df: DataFrame,
    date: str,
) -> None:
    """dim_balancing_authority — unique BA codes + names from both datasets."""
    from pyspark.sql import functions as F

    gen_bas = gen_df.select(
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
    )
    dem_bas = dem_df.select(
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
    )
    dim = (
        gen_bas.union(dem_bas)
        .dropDuplicates(["ba_code"])
        .orderBy("ba_code")
    )
    _write(dim, "dim_balancing_authority", date)


def _build_dim_fuel_type(gen_df: DataFrame, date: str) -> None:
    """dim_fuel_type — unique fuel codes + names."""
    dim = (
        gen_df.select(
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
        )
        .dropDuplicates(["fuel_code"])
        .orderBy("fuel_code")
    )
    _write(dim, "dim_fuel_type", date)


def _build_fact_generation_hourly(gen_df: DataFrame, date: str) -> None:
    """
    fact_generation_hourly — one row per (period_ts, ba_code, fuel_code).
    record_id = MD5(period_ts | ba_code | fuel_code) for upsert safety.
    """
    # Aggregate within the date partition first (silver may have duplicates
    # across overlapping ingest windows).
    agg = (
        gen_df.groupBy("period_ts", "respondent", "respondent_name", "fueltype", "fuel_type_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("generation_gwh"))
    )
    fact = agg.select(
        md5(concat_ws("|", col("period_ts").cast("string"), col("respondent"), col("fueltype")))
            .alias("record_id"),
        col("period_ts"),
        col("respondent").alias("ba_code"),
        col("respondent_name").alias("ba_name"),
        col("fueltype").alias("fuel_code"),
        col("fuel_type_name").alias("fuel_name"),
        col("generation_gwh"),
        lit(date).alias("partition_date"),
    )
    _write(fact, "fact_generation_hourly", date)


def _build_fact_demand_hourly(dem_df: DataFrame, date: str) -> None:
    """
    fact_demand_hourly — one row per (period_ts, ba_code).
    Pivots demand (D) and forecast (DF) type rows into two columns.
    record_id = MD5(period_ts | ba_code).
    """
    demand = (
        dem_df.filter(col("type") == "D")
        .groupBy("period_ts", "respondent", "respondent_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("demand_gwh"))
    )
    forecast = (
        dem_df.filter(col("type") == "DF")
        .groupBy("period_ts", "respondent")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("forecast_gwh"))
    )
    fact = (
        demand.join(forecast, on=["period_ts", "respondent"], how="left")
        .select(
            md5(concat_ws("|", col("period_ts").cast("string"), col("respondent")))
                .alias("record_id"),
            col("period_ts"),
            col("respondent").alias("ba_code"),
            col("respondent_name").alias("ba_name"),
            col("demand_gwh"),
            col("forecast_gwh"),
            lit(date).alias("partition_date"),
        )
    )
    _write(fact, "fact_demand_hourly", date)


def _build_agg_daily_generation(gen_df: DataFrame, date: str) -> None:
    """
    agg_daily_generation — daily total GWh per fuel type across all BAs.
    Used by the Streamlit trend charts.
    """
    agg = (
        gen_df.withColumn("report_date", to_date("period_ts"))
        .groupBy("report_date", "fueltype", "fuel_type_name")
        .agg(spark_round(spark_sum("value_gwh"), 4).alias("total_gwh"))
        .select(
            col("report_date"),
            col("fueltype").alias("fuel_code"),
            col("fuel_type_name").alias("fuel_name"),
            col("total_gwh"),
            lit(date).alias("partition_date"),
        )
        .orderBy("report_date", "fuel_code")
    )
    _write(agg, "agg_daily_generation", date)


def _build_agg_daily_demand_peak(dem_df: DataFrame, date: str) -> None:
    """
    agg_daily_demand_peak — daily peak demand GWh per balancing authority.
    Used by the Streamlit KPI tiles.
    """
    agg = (
        dem_df.filter(col("type") == "D")
        .withColumn("report_date", to_date("period_ts"))
        .groupBy("report_date", "respondent", "respondent_name")
        .agg(spark_round(spark_max("value_gwh"), 4).alias("peak_gwh"))
        .select(
            col("report_date"),
            col("respondent").alias("ba_code"),
            col("respondent_name").alias("ba_name"),
            col("peak_gwh"),
            lit(date).alias("partition_date"),
        )
        .orderBy("report_date", "ba_code")
    )
    _write(agg, "agg_daily_demand_peak", date)


# ── Entry point ────────────────────────────────────────────────────────────────

def run(date: str) -> None:
    spark = _build_spark(f"gold_serving_{date}")
    logger.info("Building gold serving tables for date: %s", date)

    # ── Read silver inputs ────────────────────────────────────────────────────
    gen_path = f"s3a://silver/eia/electricity_generation/date={date}"
    dem_path = f"s3a://silver/eia/electricity_demand/date={date}"

    logger.info("Reading generation silver: %s", gen_path)
    gen_df = spark.read.parquet(gen_path)

    logger.info("Reading demand silver: %s", dem_path)
    dem_df = spark.read.parquet(dem_path)

    # Cache — each DataFrame is read by multiple builders
    gen_df.cache()
    dem_df.cache()

    # ── Build all serving tables ──────────────────────────────────────────────
    _build_dim_balancing_authority(gen_df, dem_df, date)
    _build_dim_fuel_type(gen_df, date)
    _build_fact_generation_hourly(gen_df, date)
    _build_fact_demand_hourly(dem_df, date)
    _build_agg_daily_generation(gen_df, date)
    _build_agg_daily_demand_peak(dem_df, date)

    logger.info("All gold serving tables complete for %s", date)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Serving: silver → gold")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.date)
