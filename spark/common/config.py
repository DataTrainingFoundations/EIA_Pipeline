from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SparkAppConfig:
    kafka_bootstrap_servers: str
    kafka_starting_offsets: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_path_style_access: str
    bronze_output_path: str
    bronze_checkpoint_path: str


def load_spark_app_config() -> SparkAppConfig:
    return SparkAppConfig(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        kafka_starting_offsets=os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        minio_path_style_access=os.getenv("MINIO_PATH_STYLE_ACCESS", "true"),
        bronze_output_path=os.getenv("BRONZE_OUTPUT_PATH", "s3a://bronze/events"),
        bronze_checkpoint_path=os.getenv("BRONZE_CHECKPOINT_PATH", "s3a://bronze/checkpoints/default"),
    )
