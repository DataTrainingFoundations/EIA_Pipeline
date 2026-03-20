from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from common import io as spark_io
from common.quality import (
    assert_allowed_values,
    assert_no_conflicting_records,
    assert_no_nulls,
    assert_non_empty,
    assert_non_negative,
    assert_unique_keys,
    assert_value_bounds,
    count_duplicate_keys,
)
from common.spark_session import build_spark_session
from common.windowing import filter_time_window
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import DoubleType, LongType, StructField, StructType


class FakePath:
    def __init__(self, path_str: str, fs) -> None:  # noqa: ANN001
        self.path_str = path_str
        self._fs = fs

    def getFileSystem(self, _conf):  # noqa: ANN001
        return self._fs


class FakeRead:
    def __init__(self, *, failures_before_success: int = 0) -> None:
        self.options: list[tuple[str, str]] = []
        self.path: str | None = None
        self.failures_before_success = failures_before_success
        self.calls = 0

    def option(self, key: str, value: str):  # noqa: ANN201
        self.options.append((key, value))
        return self

    def parquet(self, path: str):  # noqa: ANN201
        self.calls += 1
        if self.calls <= self.failures_before_success:
            raise RuntimeError(
                f"java.io.FileNotFoundException: No such file or directory: {path}"
            )
        self.path = path
        return {"path": path, "options": list(self.options)}


class FakeSpark:
    def __init__(
        self, *, exists: bool, glob_status, failures_before_success: int = 0
    ) -> None:  # noqa: ANN001
        fs = SimpleNamespace(
            exists=lambda path: exists and path.path_str == "/data/input",
            globStatus=lambda path: (
                glob_status if path.path_str.endswith("*.parquet") else None
            ),
        )
        self._jvm = SimpleNamespace(
            org=SimpleNamespace(
                apache=SimpleNamespace(
                    hadoop=SimpleNamespace(
                        fs=SimpleNamespace(Path=lambda path_str: FakePath(path_str, fs))
                    )
                )
            )
        )
        self._jsc = SimpleNamespace(hadoopConfiguration=lambda: object())
        self.read = FakeRead(failures_before_success=failures_before_success)


class FakeBuilder:
    def __init__(self) -> None:
        self.app_name: str | None = None
        self.configs: list[tuple[str, str]] = []

    def appName(self, app_name: str):  # noqa: N802, ANN201
        self.app_name = app_name
        return self

    def config(self, key: str, value: str):  # noqa: ANN201
        self.configs.append((key, value))
        return self

    def getOrCreate(self):  # noqa: N802, ANN201
        return {"app_name": self.app_name, "configs": self.configs}


def test_quality_helpers_raise_for_bad_data(spark_session) -> None:
    valid_df = spark_session.createDataFrame(
        [{"id": 1, "value": 10.0, "category": "A"}]
    )
    duplicate_df = spark_session.createDataFrame(
        [{"id": 1, "value": 10.0}, {"id": 1, "value": 11.0}]
    )
    null_df = spark_session.createDataFrame(
        [(1, None)],
        StructType(
            [
                StructField("id", LongType(), False),
                StructField("value", DoubleType(), True),
            ]
        ),
    )
    negative_df = spark_session.createDataFrame([{"id": 1, "value": -1.0}])

    assert_non_empty(valid_df, "valid_df")
    assert count_duplicate_keys(duplicate_df, ["id"]) == 1
    assert_value_bounds(
        valid_df, "value", min_value=0.0, max_value=20.0, label="valid_df"
    )

    with pytest.raises(ValueError, match="produced no rows"):
        assert_non_empty(valid_df.limit(0), "empty_df")
    with pytest.raises(ValueError, match="null values"):
        assert_no_nulls(null_df, ["value"], "null_df")
    with pytest.raises(ValueError, match="duplicate keys"):
        assert_unique_keys(duplicate_df, ["id"], "duplicate_df")
    with pytest.raises(ValueError, match="unsupported values"):
        assert_allowed_values(valid_df, "category", ["B"], "valid_df")
    with pytest.raises(ValueError, match="conflicting records"):
        assert_no_conflicting_records(duplicate_df, ["id"], ["value"], "duplicate_df")
    with pytest.raises(ValueError, match="negative values"):
        assert_non_negative(negative_df, ["value"], "negative_df")
    with pytest.raises(ValueError, match="outside bounds"):
        assert_value_bounds(negative_df, "value", min_value=0.0, label="negative_df")


def test_io_helpers_and_window_filtering_cover_path_and_read_branches(
    spark_session,
) -> None:
    fake_spark = FakeSpark(exists=True, glob_status=[object()])
    partitioned = spark_io.read_partitioned_parquet(fake_spark, "/data/input")

    assert spark_io.path_exists(fake_spark, "/data/input") is True
    assert (
        spark_io.glob_has_matches(fake_spark, "/data/input/event_date=*/*.parquet")
        is True
    )
    assert (
        spark_io.partitioned_parquet_glob("s3a://silver/test")
        == "s3a://silver/test/event_date=*/*.parquet"
    )
    assert spark_io.has_partitioned_parquet_input(fake_spark, "/data/input") is True
    assert partitioned["path"].endswith("event_date=*/*.parquet")
    assert ("basePath", "/data/input") in partitioned["options"]
    assert (
        spark_io.read_parquet_if_exists(fake_spark, "/data/input")["path"]
        == "/data/input"
    )
    assert (
        spark_io.read_parquet_if_exists(
            FakeSpark(exists=False, glob_status=[]), "/missing"
        )
        is None
    )

    timestamp_df = spark_session.createDataFrame(
        [
            {"period": "2026-03-10T00:00:00Z", "value": 1},
            {"period": "2026-03-10T01:00:00Z", "value": 2},
            {"period": "2026-03-10T02:00:00Z", "value": 3},
        ]
    ).selectExpr("to_timestamp(period) as period", "value")
    filtered = filter_time_window(
        timestamp_df, "period", "2026-03-10T01:00:00+00:00", "2026-03-10T03:00:00+00:00"
    )

    assert [row["value"] for row in filtered.orderBy("value").collect()] == [2, 3]


def test_read_helpers_retry_transient_missing_file_errors(
    monkeypatch,
) -> None:  # noqa: ANN001
    monkeypatch.setattr(spark_io.time, "sleep", lambda _: None)
    fake_spark = FakeSpark(
        exists=True, glob_status=[object()], failures_before_success=2
    )

    partitioned = spark_io.read_partitioned_parquet(
        fake_spark, "/data/input", retries=2, retry_delay_seconds=0.01
    )
    existing = spark_io.read_parquet_if_exists(
        fake_spark, "/data/input", retries=2, retry_delay_seconds=0.01
    )

    assert partitioned["path"].endswith("event_date=*/*.parquet")
    assert existing["path"] == "/data/input"
    assert fake_spark.read.calls == 4


def test_read_partitioned_parquet_preserves_partition_columns(
    monkeypatch, spark_session
) -> None:  # noqa: ANN001
    dataset_path = "file:///partitioned_input"
    expected_date = datetime(2026, 3, 11).date()
    source_df = spark_session.createDataFrame(
        [{"respondent": "PJM", "event_date": expected_date}]
    )
    option_calls: list[tuple[str, str]] = []

    original_option = DataFrameReader.option

    def fake_option(self, key: str, value: str):  # noqa: ANN001, ANN202
        option_calls.append((key, value))
        return original_option(self, key, value)

    def fake_parquet(self, *paths: str):  # noqa: ANN001, ANN202
        assert paths == (spark_io.partitioned_parquet_glob(dataset_path),)
        return source_df

    monkeypatch.setattr(DataFrameReader, "option", fake_option)
    monkeypatch.setattr(DataFrameReader, "parquet", fake_parquet)

    loaded_df = spark_io.read_partitioned_parquet(spark_session, dataset_path)
    row = loaded_df.collect()[0]

    assert row["respondent"] == "PJM"
    assert row["event_date"] == expected_date
    assert ("basePath", dataset_path) in option_calls


def test_read_parquet_if_exists_returns_none_for_empty_existing_directory(
    monkeypatch, spark_session
) -> None:  # noqa: ANN001
    monkeypatch.setattr(spark_io, "path_exists", lambda *_args, **_kwargs: True)

    def fake_parquet(self, path: str):  # noqa: ANN001, ANN202
        raise RuntimeError(f"UNABLE_TO_INFER_SCHEMA for {path}")

    monkeypatch.setattr(DataFrameReader, "parquet", fake_parquet)

    assert (
        spark_io.read_parquet_if_exists(spark_session, "file:///empty_parquet_dir")
        is None
    )


def test_build_spark_session_configures_expected_defaults(
    monkeypatch,
) -> None:  # noqa: ANN001
    builder = FakeBuilder()
    monkeypatch.setattr(
        "common.spark_session.SparkSession.builder", builder, raising=False
    )

    session = build_spark_session(
        "unit-test",
        SimpleNamespace(
            minio_endpoint="minio:9000",
            minio_access_key="minioadmin",
            minio_secret_key="secret",
            minio_path_style_access="true",
        ),
    )

    assert session["app_name"] == "unit-test"
    assert ("spark.hadoop.fs.s3a.endpoint", "minio:9000") in session["configs"]
    assert ("spark.sql.session.timeZone", "UTC") in session["configs"]
