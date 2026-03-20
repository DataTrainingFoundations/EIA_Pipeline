from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql.types import StringType, StructField, StructType

from jobs.bronze_kafka_to_minio import prepare_bronze_write_plan, transform_kafka_batch


def test_transform_kafka_batch_preserves_kafka_metadata(spark_session) -> None:
    raw_df = spark_session.createDataFrame(
        [
            {
                "value": json.dumps(
                    {
                        "event_id": "evt-1",
                        "dataset": "electricity_region_data",
                        "source": "eia_api_v2",
                        "event_timestamp": "2026-01-01T00:00:00+00:00",
                        "ingestion_timestamp": "2026-01-01T00:10:00+00:00",
                        "metadata": {"route": "electricity/rto/region-data"},
                        "payload": {"period": "2026-01-01T00", "respondent": "PJM", "value": "100"},
                    }
                ).encode("utf-8"),
                "topic": "eia_electricity_region_data",
                "partition": 0,
                "offset": 42,
                "timestamp": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
            }
        ]
    )

    rows = transform_kafka_batch(raw_df).collect()

    assert len(rows) == 1
    assert rows[0]["kafka_topic"] == "eia_electricity_region_data"
    assert rows[0]["kafka_partition"] == 0
    assert rows[0]["kafka_offset"] == 42
    assert rows[0]["event_hour"] == 0


def test_prepare_bronze_write_plan_filters_existing_and_incoming_duplicates(monkeypatch, spark_session) -> None:  # noqa: ANN001
    transformed_batch = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "dataset": "electricity_region_data",
                "source": "eia_api_v2",
                "event_timestamp": "2026-01-01T00:00:00+00:00",
                "ingestion_timestamp": "2026-01-01T00:10:00+00:00",
                "metadata": {"route": "electricity/rto/region-data"},
                "payload": {"period": "2026-01-01T00", "respondent": "PJM", "value": "100"},
                "raw_json": "{}",
                "kafka_topic": "eia_electricity_region_data",
                "kafka_partition": 0,
                "kafka_offset": 1,
                "kafka_timestamp": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                "event_ts": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "ingestion_ts": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                "bronze_loaded_at": datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc),
                "dataset_partition": "electricity_region_data",
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
                "event_year": 2026,
                "event_month": 1,
                "event_day": 1,
                "event_hour": 0,
            },
            {
                "event_id": "evt-2",
                "dataset": "electricity_region_data",
                "source": "eia_api_v2",
                "event_timestamp": "2026-01-01T00:00:00+00:00",
                "ingestion_timestamp": "2026-01-01T00:20:00+00:00",
                "metadata": {"route": "electricity/rto/region-data"},
                "payload": {"period": "2026-01-01T00", "respondent": "PJM", "value": "101"},
                "raw_json": "{}",
                "kafka_topic": "eia_electricity_region_data",
                "kafka_partition": 0,
                "kafka_offset": 2,
                "kafka_timestamp": datetime(2026, 1, 1, 0, 20, tzinfo=timezone.utc),
                "event_ts": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "ingestion_ts": datetime(2026, 1, 1, 0, 20, tzinfo=timezone.utc),
                "bronze_loaded_at": datetime(2026, 1, 1, 0, 21, tzinfo=timezone.utc),
                "dataset_partition": "electricity_region_data",
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
                "event_year": 2026,
                "event_month": 1,
                "event_day": 1,
                "event_hour": 0,
            },
            {
                "event_id": "evt-2",
                "dataset": "electricity_region_data",
                "source": "eia_api_v2",
                "event_timestamp": "2026-01-01T00:00:00+00:00",
                "ingestion_timestamp": "2026-01-01T00:20:00+00:00",
                "metadata": {"route": "electricity/rto/region-data"},
                "payload": {"period": "2026-01-01T00", "respondent": "PJM", "value": "101"},
                "raw_json": "{}",
                "kafka_topic": "eia_electricity_region_data",
                "kafka_partition": 0,
                "kafka_offset": 3,
                "kafka_timestamp": datetime(2026, 1, 1, 0, 21, tzinfo=timezone.utc),
                "event_ts": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "ingestion_ts": datetime(2026, 1, 1, 0, 21, tzinfo=timezone.utc),
                "bronze_loaded_at": datetime(2026, 1, 1, 0, 22, tzinfo=timezone.utc),
                "dataset_partition": "electricity_region_data",
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
                "event_year": 2026,
                "event_month": 1,
                "event_day": 1,
                "event_hour": 0,
            },
            {
                "event_id": "evt-3",
                "dataset": "electricity_region_data",
                "source": "eia_api_v2",
                "event_timestamp": "2026-01-01T01:00:00+00:00",
                "ingestion_timestamp": "2026-01-01T01:10:00+00:00",
                "metadata": {"route": "electricity/rto/region-data"},
                "payload": {"period": "2026-01-01T01", "respondent": "PJM", "value": "102"},
                "raw_json": "{}",
                "kafka_topic": "eia_electricity_region_data",
                "kafka_partition": 0,
                "kafka_offset": 4,
                "kafka_timestamp": datetime(2026, 1, 1, 1, 10, tzinfo=timezone.utc),
                "event_ts": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "ingestion_ts": datetime(2026, 1, 1, 1, 10, tzinfo=timezone.utc),
                "bronze_loaded_at": datetime(2026, 1, 1, 1, 11, tzinfo=timezone.utc),
                "dataset_partition": "electricity_region_data",
                "event_date": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc).date(),
                "event_year": 2026,
                "event_month": 1,
                "event_day": 1,
                "event_hour": 1,
            },
        ]
    )

    existing_df = spark_session.createDataFrame(
        [("evt-1",)],
        StructType([StructField("event_id", StringType(), True)]),
    )
    monkeypatch.setattr(
        "jobs.bronze_kafka_to_minio._read_existing_event_ids",
        lambda *_args, **_kwargs: existing_df,
    )

    plan = prepare_bronze_write_plan(transformed_batch, "file:///bronze")
    written_event_ids = {row["event_id"] for row in plan.write_batch.select("event_id").collect()}

    assert plan.transformed_count == 4
    assert plan.duplicate_count == 2
    assert plan.write_count == 2
    assert plan.touched_partition_count == 2
    assert written_event_ids == {"evt-2", "evt-3"}


def test_prepare_bronze_write_plan_advances_duplicate_only_batches(monkeypatch, spark_session) -> None:  # noqa: ANN001
    existing_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "dataset": "electricity_region_data",
                "source": "eia_api_v2",
                "event_timestamp": "2026-01-01T00:00:00+00:00",
                "ingestion_timestamp": "2026-01-01T00:10:00+00:00",
                "metadata": {"route": "electricity/rto/region-data"},
                "payload": {"period": "2026-01-01T00", "respondent": "PJM", "value": "100"},
                "raw_json": "{}",
                "kafka_topic": "eia_electricity_region_data",
                "kafka_partition": 0,
                "kafka_offset": 1,
                "kafka_timestamp": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                "event_ts": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "ingestion_ts": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                "bronze_loaded_at": datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc),
                "dataset_partition": "electricity_region_data",
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
                "event_year": 2026,
                "event_month": 1,
                "event_day": 1,
                "event_hour": 0,
            }
        ]
    )
    existing_event_ids = spark_session.createDataFrame(
        [("evt-1",)],
        StructType([StructField("event_id", StringType(), True)]),
    )
    monkeypatch.setattr(
        "jobs.bronze_kafka_to_minio._read_existing_event_ids",
        lambda *_args, **_kwargs: existing_event_ids,
    )

    plan = prepare_bronze_write_plan(existing_df, "file:///bronze")

    assert plan.write_count == 0
    assert plan.duplicate_count == 1
