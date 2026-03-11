"""
bronze_kafka_to_minio.py
------------------------
Spark job: Bronze Layer
  - Consumes messages from EIA Kafka topics
  - Writes raw JSON partitioned by dataset and date to MinIO bronze/

Run via spark-submit from Airflow. Accepts topic and date as arguments.

Usage:
    spark-submit bronze_kafka_to_minio.py \
        --topic eia.electricity.generation \
        --date 2024-01-15

MinIO output path:
    s3a://bronze/eia/electricity_generation/date=2024-01-15/
    s3a://bronze/eia/electricity_demand/date=2024-01-15/
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

logger = logging.getLogger(__name__)

TOPIC_TO_PATH = {
    "eia.electricity.generation": "eia/electricity_generation",
    "eia.electricity.demand": "eia/electricity_demand",
}

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
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


def run(topic: str, date: str) -> None:
    if topic not in TOPIC_TO_PATH:
        raise ValueError(f"Unknown topic '{topic}'. Known topics: {list(TOPIC_TO_PATH)}")

    output_path = f"s3a://bronze/{TOPIC_TO_PATH[topic]}/date={date}"
    app_name = f"bronze_{topic.replace('.', '_')}_{date}"
    spark = build_spark_session(app_name)

    logger.info("Reading from Kafka topic: %s", topic)

    # Read from Kafka (batch mode — Airflow triggers daily)
    raw_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Keep raw value as string + metadata columns
    bronze_df = raw_df.select(
        col("value").cast("string").alias("raw_json"),
        col("key").cast("string").alias("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
        lit(date).alias("ingestion_date"),
    )

    record_count = bronze_df.count()
    logger.info("Writing %d records to %s", record_count, output_path)

    (
        bronze_df.write.mode("overwrite")
        .format("json")
        .option("compression", "gzip")
        .save(output_path)
    )

    logger.info("Bronze write complete → %s", output_path)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze: Kafka → MinIO")
    parser.add_argument("--topic", required=True, help="Kafka topic to consume")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    run(args.topic, args.date)