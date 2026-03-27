"""
silver_clean_transform.py
=========================
Spark Job: Silver Layer
-----------------------
Reads raw records from a Snowflake RAW table, cleans and validates them,
deduplicates, and writes clean Parquet to MinIO silver/.

The MinIO bronze layer is no longer used — Snowflake is the new raw/bronze
store.  All other silver logic (type coercion, unit normalisation,
deduplication) is unchanged.

INPUT
-----
    Snowflake table:  <SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>.<TABLE>
    Filtered to rows where: TRY_TO_DATE(_FETCHED_AT) = <date>

OUTPUT
------
    s3a://silver/eia/electricity_generation/date=<date>/
    s3a://silver/eia/electricity_demand/date=<date>/

USAGE
-----
    spark-submit silver_clean_transform.py \
        --dataset electricity_generation \
        --date 2024-03-15

ENVIRONMENT VARIABLES (Snowflake Spark connector)
-------------------------------------------------
SNOWFLAKE_URL        — <account>.snowflakecomputing.com
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    trim,
    upper,
    when,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Config ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER     = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

SF_URL      = os.environ.get("SNOWFLAKE_URL", "")
SF_USER     = os.environ.get("SNOWFLAKE_USER", "")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
SF_ROLE     = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
SF_WH       = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DB       = os.environ.get("SNOWFLAKE_DATABASE", "EIA")
SF_SCHEMA   = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")

# Dataset ID → Snowflake table name (mirrors dataset_registry.yml)
DATASET_TABLE_MAP = {
    "electricity_generation": "ELECTRICITY_GENERATION_RAW",
    "electricity_demand":     "ELECTRICITY_DEMAND_RAW",
}

# Deduplication key columns per dataset
DEDUP_COLS = {
    "electricity_generation": ["PERIOD", "RESPONDENT", "FUELTYPE"],
    "electricity_demand":     ["PERIOD", "RESPONDENT", "TYPE"],
}


# ── Spark session ──────────────────────────────────────────────────────────────

def _build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "net.snowflake:snowflake-jdbc:3.16.1,"
            "net.snowflake:spark-snowflake_2.13:2.16.0-spark_3.4",
        )
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


# ── Snowflake reader ───────────────────────────────────────────────────────────

def _read_snowflake(spark: SparkSession, table: str, date: str) -> DataFrame:
    """
    Read raw rows from Snowflake for the given date using the Spark connector.
    Filters to rows fetched on `date` so each silver run is idempotent.
    """
    sf_options = {
        "sfURL":       SF_URL,
        "sfUser":      SF_USER,
        "sfPassword":  SF_PASSWORD,
        "sfRole":      SF_ROLE,
        "sfWarehouse": SF_WH,
        "sfDatabase":  SF_DB,
        "sfSchema":    SF_SCHEMA,
        "dbtable":     table,
    }

    logger.info("Reading from Snowflake: %s.%s.%s  (date=%s)", SF_DB, SF_SCHEMA, table, date)

    df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .load()
        # Filter to just the rows ingested on the target date
        .filter(f"TRY_TO_DATE(_FETCHED_AT) = '{date}'")
    )

    count = df.count()
    logger.info("Rows read from Snowflake %s: %d", table, count)
    return df


# ── Dataset-specific cleaners ──────────────────────────────────────────────────

def _clean_generation(df: DataFrame) -> DataFrame:
    """Clean and normalise electricity generation records."""
    return (
        df.filter(col("VALUE").isNotNull())
        .filter(col("VALUE") >= 0)
        .withColumn("RESPONDENT",      upper(trim(col("RESPONDENT"))))
        .withColumn("FUELTYPE",        upper(trim(col("FUELTYPE"))))
        # Rename hyphenated Snowflake columns to underscore equivalents
        .withColumnRenamed("RESPONDENT_NAME", "respondent_name")
        .withColumnRenamed("TYPE_NAME",       "fuel_type_name")
        .withColumnRenamed("VALUE_UNITS",     "units")
        .withColumn(
            "period_ts",
            to_timestamp(col("PERIOD"), "yyyy-MM-dd'T'HH"),
        )
        .withColumn(
            "value_gwh",
            when(col("units") == "megawatthours", col("VALUE") / 1000.0)
            .otherwise(col("VALUE")),
        )
        # Standardise column names to lowercase for silver schema consistency
        .withColumnRenamed("RESPONDENT", "respondent")
        .withColumnRenamed("FUELTYPE",   "fueltype")
        .withColumnRenamed("PERIOD",     "period")
        .withColumnRenamed("VALUE",      "value")
    )


def _clean_demand(df: DataFrame) -> DataFrame:
    """Clean and normalise electricity demand records."""
    return (
        df.filter(col("VALUE").isNotNull())
        .filter(col("VALUE") >= 0)
        .withColumn("RESPONDENT", upper(trim(col("RESPONDENT"))))
        .withColumn("TYPE",       upper(trim(col("TYPE"))))
        .withColumnRenamed("RESPONDENT_NAME", "respondent_name")
        .withColumnRenamed("TYPE_NAME",       "demand_type_name")
        .withColumnRenamed("VALUE_UNITS",     "units")
        .withColumn(
            "period_ts",
            to_timestamp(col("PERIOD"), "yyyy-MM-dd'T'HH"),
        )
        .withColumn(
            "value_gwh",
            when(col("units") == "megawatthours", col("VALUE") / 1000.0)
            .otherwise(col("VALUE")),
        )
        .withColumnRenamed("RESPONDENT", "respondent")
        .withColumnRenamed("TYPE",       "type")
        .withColumnRenamed("PERIOD",     "period")
        .withColumnRenamed("VALUE",      "value")
    )


# ── Entry point ────────────────────────────────────────────────────────────────

def run(dataset: str, date: str) -> None:
    if dataset not in DATASET_TABLE_MAP:
        raise ValueError(
            f"Unknown dataset '{dataset}'. Known: {list(DATASET_TABLE_MAP)}"
        )

    sf_table    = DATASET_TABLE_MAP[dataset]
    dedup_cols  = DEDUP_COLS[dataset]
    silver_path = f"s3a://silver/eia/{dataset}/date={date}"
    app_name    = f"silver_{dataset}_{date}"

    spark = _build_spark(app_name)

    # ── Read from Snowflake ───────────────────────────────────────────────────
    raw_df = _read_snowflake(spark, sf_table, date)

    # ── Clean ─────────────────────────────────────────────────────────────────
    if dataset == "electricity_generation":
        clean_df = _clean_generation(raw_df)
    else:
        clean_df = _clean_demand(raw_df)

    # ── Deduplicate (Snowflake may have overlapping ingest windows) ───────────
    # Dedup cols were uppercased in Snowflake; after rename they're lowercase
    lower_dedup = [c.lower() for c in dedup_cols]
    dedup_df = clean_df.dropDuplicates(lower_dedup)

    # ── Add silver metadata ───────────────────────────────────────────────────
    silver_df = dedup_df.withColumn("silver_processed_at", current_timestamp())

    record_count = silver_df.count()
    logger.info("Writing %d clean records to %s", record_count, silver_path)

    (
        silver_df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(silver_path)
    )

    logger.info("Silver write complete -> %s", silver_path)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver: Snowflake RAW -> clean Parquet")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=list(DATASET_TABLE_MAP),
        help="Dataset name (electricity_generation | electricity_demand)",
    )
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.dataset, args.date)