"""
silver_clean_transform.py
--------------------------
Spark job: Silver Layer
  - Reads raw JSON from MinIO bronze/
  - Parses and validates records
  - Standardizes column names, types, and units
  - Deduplicates on (period, respondent, fueltype/type)
  - Writes clean Parquet to MinIO silver/

MinIO paths:
    Input:  s3a://bronze/eia/electricity_generation/date=<date>/
            s3a://bronze/eia/electricity_demand/date=<date>/
    Output: s3a://silver/eia/electricity_generation/date=<date>/
            s3a://silver/eia/electricity_demand/date=<date>/
"""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# EIA v2 field schemas
GENERATION_SCHEMA = StructType([
    StructField("period", StringType()),
    StructField("respondent", StringType()),
    StructField("respondent-name", StringType()),
    StructField("fueltype", StringType()),
    StructField("type-name", StringType()),
    StructField("value", FloatType()),
    StructField("value-units", StringType()),
    StructField("_dataset_id", StringType()),
    StructField("_fetched_at", StringType()),
])

DEMAND_SCHEMA = StructType([
    StructField("period", StringType()),
    StructField("respondent", StringType()),
    StructField("respondent-name", StringType()),
    StructField("type", StringType()),
    StructField("type-name", StringType()),
    StructField("value", FloatType()),
    StructField("value-units", StringType()),
    StructField("_dataset_id", StringType()),
    StructField("_fetched_at", StringType()),
])

DATASET_CONFIG = {
    "electricity_generation": {
        "schema": GENERATION_SCHEMA,
        "dedup_cols": ["period", "respondent", "fueltype"],
        "type_col": "fueltype",
    },
    "electricity_demand": {
        "schema": DEMAND_SCHEMA,
        "dedup_cols": ["period", "respondent", "type"],
        "type_col": "type",
    },
}


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


def clean_generation(df: DataFrame) -> DataFrame:
    """Clean and normalize electricity generation records."""
    return (
        df.filter(col("value").isNotNull())
        .filter(col("value") >= 0)
        .withColumn("respondent", upper(trim(col("respondent"))))
        .withColumn("fueltype", upper(trim(col("fueltype"))))
        .withColumnRenamed("respondent-name", "respondent_name")
        .withColumnRenamed("type-name", "fuel_type_name")
        .withColumnRenamed("value-units", "units")
        .withColumn(
            "period_ts",
            to_timestamp(col("period"), "yyyy-MM-dd'T'HH")
        )
        # Normalize MWh → GWh flag
        .withColumn(
            "value_gwh",
            when(col("units") == "megawatthours", col("value") / 1000.0)
            .otherwise(col("value"))
        )
    )


def clean_demand(df: DataFrame) -> DataFrame:
    """Clean and normalize electricity demand records."""
    return (
        df.filter(col("value").isNotNull())
        .filter(col("value") >= 0)
        .withColumn("respondent", upper(trim(col("respondent"))))
        .withColumn("type", upper(trim(col("type"))))
        .withColumnRenamed("respondent-name", "respondent_name")
        .withColumnRenamed("type-name", "demand_type_name")
        .withColumnRenamed("value-units", "units")
        .withColumn(
            "period_ts",
            to_timestamp(col("period"), "yyyy-MM-dd'T'HH")
        )
        .withColumn(
            "value_gwh",
            when(col("units") == "megawatthours", col("value") / 1000.0)
            .otherwise(col("value"))
        )
    )


def run(dataset: str, date: str) -> None:
    if dataset not in DATASET_CONFIG:
        raise ValueError(f"Unknown dataset '{dataset}'. Known: {list(DATASET_CONFIG)}")

    cfg = DATASET_CONFIG[dataset]
    bronze_path = f"s3a://bronze/eia/{dataset}/date={date}"
    silver_path = f"s3a://silver/eia/{dataset}/date={date}"
    app_name = f"silver_{dataset}_{date}"
    spark = build_spark_session(app_name)

    logger.info("Reading bronze data from: %s", bronze_path)
    raw_df = spark.read.format("json").load(bronze_path)

    # Parse the nested raw_json string into structured columns
    parsed_df = raw_df.select(
        from_json(col("raw_json"), cfg["schema"]).alias("data"),
        col("ingested_at"),
        col("ingestion_date"),
    ).select("data.*", "ingested_at", "ingestion_date")

    # Apply dataset-specific cleaning
    if dataset == "electricity_generation":
        clean_df = clean_generation(parsed_df)
    else:
        clean_df = clean_demand(parsed_df)

    # Deduplicate
    dedup_df = clean_df.dropDuplicates(cfg["dedup_cols"])

    # Add silver metadata
    silver_df = dedup_df.withColumn("silver_processed_at", current_timestamp())

    record_count = silver_df.count()
    logger.info("Writing %d clean records to %s", record_count, silver_path)

    (
        silver_df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(silver_path)
    )

    logger.info("Silver write complete → %s", silver_path)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver: Clean & Transform")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=list(DATASET_CONFIG),
        help="Dataset name (electricity_generation | electricity_demand)",
    )
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.dataset, args.date)