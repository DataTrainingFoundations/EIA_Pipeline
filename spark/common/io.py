"""Shared Spark filesystem and parquet loading helpers."""

from __future__ import annotations

import time

from pyspark.sql import DataFrame


def path_exists(spark, path_str: str) -> bool:  # noqa: ANN001
    """Return whether a Hadoop/S3 path exists for the current Spark session."""

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(hadoop_conf)
    return fs.exists(path)


def glob_has_matches(spark, path_pattern: str) -> bool:  # noqa: ANN001
    """Return whether a Hadoop/S3 glob matches at least one file."""

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(path_pattern)
    fs = path.getFileSystem(hadoop_conf)
    statuses = fs.globStatus(path)
    return statuses is not None and len(statuses) > 0


def partitioned_parquet_glob(base_path: str, partition_column: str = "event_date") -> str:
    """Return the standard parquet glob used by day-partitioned datasets."""

    return f"{base_path.rstrip('/')}/{partition_column}=*/*.parquet"


def has_partitioned_parquet_input(spark, base_path: str, partition_column: str = "event_date") -> bool:  # noqa: ANN001
    """Return whether a partitioned parquet dataset exists and has files."""

    return path_exists(spark, base_path) and glob_has_matches(spark, partitioned_parquet_glob(base_path, partition_column))


def _is_retryable_missing_file_error(exc: Exception) -> bool:
    message = str(exc)
    return "FileNotFoundException" in message or "No such file or directory" in message


def _is_empty_parquet_schema_error(exc: Exception) -> bool:
    message = str(exc)
    return "UNABLE_TO_INFER_SCHEMA" in message or "Unable to infer schema for Parquet" in message


def _read_with_retries(loader, *, retries: int, retry_delay_seconds: float):  # noqa: ANN001, ANN202
    attempts = retries + 1
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return loader()
        except Exception as exc:  # pragma: no cover - Spark raises environment-specific wrappers
            if attempt >= attempts or not _is_retryable_missing_file_error(exc):
                raise
            last_error = exc
            time.sleep(retry_delay_seconds)
    if last_error is not None:  # pragma: no cover - defensive, loop always returns or raises
        raise last_error
    raise RuntimeError("Parquet read retry loop exited unexpectedly")


def read_partitioned_parquet(
    spark,
    base_path: str,
    partition_column: str = "event_date",
    *,
    retries: int = 0,
    retry_delay_seconds: float = 1.0,
) -> DataFrame:  # noqa: ANN001
    """Read a day-partitioned parquet dataset while preserving partition columns.

    Spark drops partition columns when `recursiveFileLookup` is enabled. The
    Gold and Platinum jobs need `event_date` available from partition discovery,
    so this helper reads the partition glob with `basePath` set instead.
    """

    return _read_with_retries(
        lambda: spark.read.option("basePath", base_path).parquet(partitioned_parquet_glob(base_path, partition_column)),
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
    )


def read_parquet_if_exists(
    spark,
    path_str: str,
    *,
    retries: int = 0,
    retry_delay_seconds: float = 1.0,
) -> DataFrame | None:  # noqa: ANN001
    """Read parquet when a path exists, otherwise return `None`."""

    if not path_exists(spark, path_str):
        return None
    try:
        return _read_with_retries(
            lambda: spark.read.parquet(path_str),
            retries=retries,
            retry_delay_seconds=retry_delay_seconds,
        )
    except Exception as exc:  # pragma: no cover - Spark raises environment-specific wrappers
        if _is_empty_parquet_schema_error(exc):
            return None
        raise


def write_partitioned_parquet(df: DataFrame, output_path: str, partition_column: str = "event_date") -> None:
    """Overwrite only touched partitions for a parquet dataset.

    Airflow runs the Silver and Gold jobs for narrow hourly windows. Dynamic
    partition overwrite preserves historical partitions while still replacing
    the current window's partitions atomically.
    """

    if df.isEmpty():
        return
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(partition_column)
        .parquet(output_path)
    )
