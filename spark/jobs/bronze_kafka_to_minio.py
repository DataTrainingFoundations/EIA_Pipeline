"""Kafka-to-Bronze batch ingestion job.

This job consumes one bounded Kafka batch, normalizes the Bronze event schema,
filters duplicates, writes new parquet partitions, and advances the explicit
offset checkpoint used by the Airflow Bronze step.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofmonth,
    from_json,
    hour,
    lit,
    month,
    to_date,
    to_timestamp,
    year,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.storagelevel import StorageLevel

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_spark_app_config
from common.io import path_exists
from common.logging_utils import configure_logging, log_job_complete, log_job_start
from common.quality import assert_no_nulls, assert_unique_keys, count_duplicate_keys
from common.schemas import KAFKA_EVENT_SCHEMA
from common.spark_session import build_spark_session

logger = logging.getLogger(__name__)
PARTITION_COLUMNS = [
    "dataset_partition",
    "event_year",
    "event_month",
    "event_day",
    "event_hour",
]


@dataclass(frozen=True)
class BronzeWritePlan:
    write_batch: DataFrame
    transformed_count: int
    duplicate_count: int
    write_count: int
    touched_partition_count: int


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the Bronze Kafka batch job."""

    parser = argparse.ArgumentParser(description="Kafka to Bronze MinIO batch job.")
    parser.add_argument("--topic", required=True, help="Kafka topic to consume.")
    parser.add_argument(
        "--trigger-available-now",
        action="store_true",
        help="Compatibility flag; bronze now runs in bounded batch mode.",
    )
    return parser.parse_args()


def transform_kafka_batch(raw_df: DataFrame) -> DataFrame:
    """Parse Kafka messages and shape them into the Bronze parquet schema."""

    parsed_df = raw_df.select(
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).select(
        from_json(col("raw_json"), KAFKA_EVENT_SCHEMA).alias("event"),
        col("raw_json"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
    )
    transformed_df = (
        parsed_df.filter(col("event").isNotNull())
        .select(
            "event.*",
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .withColumn("ingestion_ts", to_timestamp(col("ingestion_timestamp")))
        .withColumn("bronze_loaded_at", current_timestamp())
        .withColumn("dataset_partition", col("dataset"))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("event_year", year(col("event_ts")))
        .withColumn("event_month", month(col("event_ts")))
        .withColumn("event_day", dayofmonth(col("event_ts")))
        .withColumn("event_hour", hour(col("event_ts")))
        .filter(
            col("event_ts").isNotNull()
            & col("event_id").isNotNull()
            & col("dataset").isNotNull()
        )
    )
    assert_no_nulls(
        transformed_df, ["event_id", "dataset", "event_ts"], "bronze.kafka_batch"
    )
    return transformed_df


def _offsets_path(checkpoint_path: str) -> str:
    return f"{checkpoint_path.rstrip('/')}/offsets.json"


def _read_starting_offsets(
    spark: SparkSession, checkpoint_path: str, default_offsets: str
) -> str:
    """Read the last stored offset checkpoint or fall back to the default."""

    offsets_path = _offsets_path(checkpoint_path)
    if not path_exists(spark, offsets_path):
        return _normalize_batch_starting_offsets(default_offsets)
    text = "\n".join(spark.sparkContext.textFile(offsets_path).collect()).strip()
    return text or _normalize_batch_starting_offsets(default_offsets)


def _normalize_batch_starting_offsets(offsets: str) -> str:
    """Rewrite `latest` to `earliest` so bounded Bronze batches stay replay-safe."""

    return "earliest" if offsets.strip().lower() == "latest" else offsets


def _write_next_offsets(
    spark: SparkSession, checkpoint_path: str, payload: dict[str, dict[str, int]]
) -> None:
    """Persist the next Kafka offsets that the next Bronze batch should start from."""

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(_offsets_path(checkpoint_path))
    fs = path.getFileSystem(hadoop_conf)
    fs.mkdirs(path.getParent())
    stream = fs.create(path, True)
    try:
        stream.writeBytes(json.dumps(payload, separators=(",", ":")))
    finally:
        stream.close()


def _collect_next_offsets(raw_df: DataFrame, topic: str) -> dict[str, dict[str, int]]:
    """Collect the next Kafka offsets after the current raw batch."""

    rows = (
        raw_df.groupBy("partition")
        .agg((spark_max(col("offset")) + lit(1)).alias("next_offset"))
        .collect()
    )
    return {topic: {str(row["partition"]): int(row["next_offset"]) for row in rows}}


def _empty_event_id_df(spark: SparkSession) -> DataFrame:
    """Create an empty DataFrame with the same schema as the event id lookup."""

    return spark.createDataFrame(
        [], StructType([StructField("event_id", StringType(), True)])
    )


def _partition_path(base_path: str, row: dict[str, int | str]) -> str:
    """Build the Bronze partition path for one distinct batch partition."""

    return (
        f"{base_path.rstrip('/')}/dataset_partition={row['dataset_partition']}"
        f"/event_year={row['event_year']}"
        f"/event_month={row['event_month']}"
        f"/event_day={row['event_day']}"
        f"/event_hour={row['event_hour']}"
    )


def _collect_touched_partition_paths(
    batch_df: DataFrame, bronze_output_path: str
) -> list[str]:
    """Collect all Bronze partition paths touched by this batch."""

    rows = [
        row.asDict() for row in batch_df.select(*PARTITION_COLUMNS).distinct().collect()
    ]
    return [_partition_path(bronze_output_path, row) for row in rows]


def _read_existing_event_ids(
    spark: SparkSession, bronze_output_path: str, partition_paths: list[str]
) -> DataFrame:
    """Read previously-written event ids from only the touched Bronze partitions."""

    existing_paths = [path for path in partition_paths if path_exists(spark, path)]
    if not existing_paths:
        return _empty_event_id_df(spark)
    return (
        spark.read.option("mergeSchema", "true")
        .parquet(*existing_paths)
        .select("event_id")
        .dropDuplicates(["event_id"])
    )


def prepare_bronze_write_plan(
    transformed_batch: DataFrame, bronze_output_path: str
) -> BronzeWritePlan:
    """Plan the Bronze write by filtering duplicates before any append happens."""

    spark = transformed_batch.sparkSession
    transformed_batch = transformed_batch.persist(StorageLevel.MEMORY_AND_DISK)
    transformed_count = transformed_batch.count()
    incoming_duplicate_count = count_duplicate_keys(transformed_batch, ["event_id"])
    unique_transformed_batch = transformed_batch.dropDuplicates(["event_id"])
    touched_partition_paths = _collect_touched_partition_paths(
        unique_transformed_batch, bronze_output_path
    )
    existing_event_ids = _read_existing_event_ids(
        spark, bronze_output_path, touched_partition_paths
    )
    write_batch = unique_transformed_batch.join(
        existing_event_ids, on="event_id", how="left_anti"
    ).persist(StorageLevel.MEMORY_AND_DISK)
    write_count = write_batch.count()
    duplicate_count = transformed_count - write_count

    assert_unique_keys(write_batch, ["event_id"], "bronze.write_batch")
    if incoming_duplicate_count:
        logger.info(
            "Suppressed duplicate event_ids already present in batch duplicate_count=%s",
            incoming_duplicate_count,
        )

    return BronzeWritePlan(
        write_batch=write_batch,
        transformed_count=transformed_count,
        duplicate_count=duplicate_count,
        write_count=write_count,
        touched_partition_count=len(touched_partition_paths),
    )


def main() -> None:
    """Run the bounded Kafka-to-Bronze batch job."""

    configure_logging()
    args = parse_args()
    config = load_spark_app_config()
    spark = build_spark_session("bronze_kafka_to_minio", config)
    log_job_start(
        logger,
        "bronze_kafka_to_minio",
        topic=args.topic,
        input_path="kafka",
        output_path=config.bronze_output_path,
        stage_table=config.bronze_checkpoint_path,
    )

    starting_offsets = _read_starting_offsets(
        spark, config.bronze_checkpoint_path, config.kafka_starting_offsets
    )
    raw_batch = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    raw_batch = raw_batch.persist(StorageLevel.MEMORY_AND_DISK)
    raw_count = raw_batch.count()
    if raw_count == 0:
        logger.info(
            "Kafka bronze batch is empty topic=%s starting_offsets=%s",
            args.topic,
            starting_offsets,
        )
        raw_batch.unpersist()
        return

    transformed_batch = transform_kafka_batch(raw_batch).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    if transformed_batch.isEmpty():
        logger.info(
            "Kafka bronze batch produced no valid transformed rows topic=%s starting_offsets=%s raw_count=%s",
            args.topic,
            starting_offsets,
            raw_count,
        )
        transformed_batch.unpersist()
        raw_batch.unpersist()
        return

    write_plan = prepare_bronze_write_plan(transformed_batch, config.bronze_output_path)
    try:
        if write_plan.write_count > 0:
            write_plan.write_batch.write.mode("append").partitionBy(
                "dataset_partition",
                "event_year",
                "event_month",
                "event_day",
                "event_hour",
            ).parquet(config.bronze_output_path)
        else:
            logger.info(
                "Kafka bronze batch contained only duplicate events topic=%s",
                args.topic,
            )

        next_offsets = _collect_next_offsets(raw_batch, args.topic)
        _write_next_offsets(spark, config.bronze_checkpoint_path, next_offsets)
        log_job_complete(
            logger,
            "bronze_kafka_to_minio",
            topic=args.topic,
            input_path="kafka",
            output_path=config.bronze_output_path,
            raw_count=raw_count,
            clean_count=write_plan.transformed_count,
            deduped_count=write_plan.duplicate_count,
            written_count=write_plan.write_count,
            touched_partitions=write_plan.touched_partition_count,
            next_offsets=json.dumps(next_offsets, separators=(",", ":")),
        )
    finally:
        write_plan.write_batch.unpersist()
        transformed_batch.unpersist()
        raw_batch.unpersist()


if __name__ == "__main__":
    main()
