from datetime import datetime, timezone

import pytest

from common.io import merge_partitioned_parquet, write_partitioned_parquet
from jobs.gold_region_fuel_serving_hourly import build_region_hourly_metrics, build_respondent_dimension
from jobs.platinum_region_demand_daily import build_region_demand_daily
from jobs.platinum_resource_planning_daily import build_resource_planning_daily
from jobs.silver_clean_transform import clean_region_data, validate_non_empty


def test_clean_region_data_deduplicates_event_ids(spark_session) -> None:
    bronze_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "dataset": "electricity_region_data",
                "payload": {"period": "2026-01-01T00", "respondent": "PJM", "type": "D", "value": "100", "value-units": "MWh"},
                "ingestion_ts": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
            },
            {
                "event_id": "evt-1",
                "dataset": "electricity_region_data",
                "payload": {"period": "2026-01-01T00", "respondent": "PJM", "type": "D", "value": "100", "value-units": "MWh"},
                "ingestion_ts": datetime(2026, 1, 1, 1, 5, tzinfo=timezone.utc),
            },
        ]
    )

    cleaned_df = clean_region_data(bronze_df)
    rows = cleaned_df.collect()

    assert len(rows) == 1
    assert rows[0]["respondent"] == "PJM"
    assert rows[0]["value"] == 100.0


def test_clean_region_data_rejects_invalid_units(spark_session) -> None:
    bronze_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "dataset": "electricity_region_data",
                "payload": {"period": "2026-01-01T00", "respondent": "PJM", "respondent-name": "PJM", "type": "D", "value": "100", "value-units": "GW"},
                "ingestion_ts": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
            },
        ]
    )

    with pytest.raises(ValueError, match="unsupported values"):
        clean_region_data(bronze_df)


def test_validate_non_empty_ignores_identical_replayed_event_ids(spark_session) -> None:
    bronze_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "dataset": "electricity_region_data",
                "payload": {
                    "period": "2026-01-01T00",
                    "respondent": "PJM",
                    "respondent-name": "PJM",
                    "type": "D",
                    "value": "100",
                    "value-units": "MWh",
                },
                "ingestion_ts": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
            },
            {
                "event_id": "evt-1",
                "dataset": "electricity_region_data",
                "payload": {
                    "period": "2026-01-01T00",
                    "respondent": "PJM",
                    "respondent-name": "PJM",
                    "type": "D",
                    "value": "100",
                    "value-units": "MWh",
                },
                "ingestion_ts": datetime(2026, 1, 1, 1, 5, tzinfo=timezone.utc),
            },
        ]
    )

    cleaned_df = clean_region_data(bronze_df)

    validate_non_empty(bronze_df, cleaned_df, "electricity_region_data", "silver.region_data")


def test_region_hourly_metrics_aggregates_by_period_and_respondent(spark_session) -> None:
    region_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "respondent_name": "PJM",
                "type": "D",
                "value": 100.0,
                "loaded_at": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
            },
            {
                "event_id": "evt-2",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "respondent_name": "PJM",
                "type": "DF",
                "value": 125.0,
                "loaded_at": datetime(2026, 1, 1, 1, 10, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
            },
        ]
    )

    gold_df = build_region_hourly_metrics(region_df)
    rows = gold_df.collect()

    assert len(rows) == 1
    assert rows[0]["actual_demand_mwh"] == 100.0
    assert rows[0]["day_ahead_forecast_mwh"] == 125.0


def test_region_hourly_metrics_prefers_latest_loaded_conflicting_duplicate(spark_session) -> None:
    region_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "respondent_name": "PJM",
                "type": "D",
                "value": 100.0,
                "value_units": "mwh",
                "loaded_at": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
            },
            {
                "event_id": "evt-2",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "respondent_name": "PJM",
                "type": "D",
                "value": 120.0,
                "value_units": "mwh",
                "loaded_at": datetime(2026, 1, 1, 1, 5, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
            },
        ]
    )

    gold_df = build_region_hourly_metrics(region_df)
    rows = gold_df.collect()

    assert len(rows) == 1
    assert rows[0]["actual_demand_mwh"] == 120.0


def test_respondent_dimension_rolls_up_current_and_historical_names(spark_session, tmp_path) -> None:
    historical_dim = spark_session.createDataFrame(
        [
            {
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "first_seen_date": datetime(2025, 12, 31, tzinfo=timezone.utc).date(),
                "last_seen_date": datetime(2025, 12, 31, tzinfo=timezone.utc).date(),
                "source_dataset_count": 1,
                "updated_at": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
            }
        ]
    )
    historical_path = tmp_path / "respondent_dim"
    historical_dim.write.mode("overwrite").parquet(str(historical_path))

    region_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "respondent_name": "PJM Updated",
                "type": "D",
                "value": 100.0,
                "value_units": "mwh",
                "loaded_at": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
            }
        ]
    )

    dim_df = build_respondent_dimension(spark_session, region_df, None, str(historical_path))
    rows = dim_df.collect()

    assert len(rows) == 1
    assert rows[0]["respondent_name"] == "PJM Updated"
    assert rows[0]["first_seen_date"] == datetime(2025, 12, 31, tzinfo=timezone.utc).date()
    assert rows[0]["last_seen_date"] == datetime(2026, 1, 1, tzinfo=timezone.utc).date()


def test_respondent_dimension_preserves_existing_dataset_count(spark_session, tmp_path) -> None:
    historical_dim = spark_session.createDataFrame(
        [
            {
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "first_seen_date": datetime(2025, 12, 30, tzinfo=timezone.utc).date(),
                "last_seen_date": datetime(2025, 12, 31, tzinfo=timezone.utc).date(),
                "source_dataset_count": 2,
                "updated_at": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
            }
        ]
    )
    historical_path = tmp_path / "respondent_dim_existing_count"
    historical_dim.write.mode("overwrite").parquet(str(historical_path))

    region_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "respondent_name": "PJM Updated",
                "type": "D",
                "value": 100.0,
                "value_units": "mwh",
                "loaded_at": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
            }
        ]
    )

    dim_df = build_respondent_dimension(spark_session, region_df, None, str(historical_path))
    rows = dim_df.collect()

    assert len(rows) == 1
    assert rows[0]["source_dataset_count"] == 2


def test_respondent_dimension_defaults_dataset_count_without_history(spark_session, tmp_path) -> None:
    region_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "respondent_name": "PJM Updated",
                "type": "D",
                "value": 100.0,
                "value_units": "mwh",
                "loaded_at": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
            }
        ]
    )

    dim_df = build_respondent_dimension(spark_session, region_df, None, str(tmp_path / "missing_respondent_dim"))
    rows = dim_df.collect()

    assert len(rows) == 1
    assert rows[0]["source_dataset_count"] == 1


def test_write_partitioned_parquet_overwrites_only_touched_partitions(spark_session, tmp_path) -> None:
    output_path = tmp_path / "partitioned_dataset"
    first_df = spark_session.createDataFrame(
        [
            {
                "respondent": "PJM",
                "metric": 100.0,
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            },
            {
                "respondent": "MISO",
                "metric": 200.0,
                "event_date": datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
            },
        ]
    )
    replacement_df = spark_session.createDataFrame(
        [
            {
                "respondent": "MISO",
                "metric": 250.0,
                "event_date": datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
            }
        ]
    )

    write_partitioned_parquet(first_df, str(output_path))
    write_partitioned_parquet(replacement_df, str(output_path))

    rows = spark_session.read.parquet(str(output_path)).orderBy("event_date", "respondent").collect()

    assert len(rows) == 2
    assert rows[0]["respondent"] == "PJM"
    assert rows[0]["metric"] == 100.0
    assert rows[1]["respondent"] == "MISO"
    assert rows[1]["metric"] == 250.0


def test_merge_partitioned_parquet_preserves_same_day_rows_across_writes(spark_session, tmp_path) -> None:
    output_path = tmp_path / "merged_silver_dataset"
    first_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "loaded_at": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            }
        ]
    )
    second_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-2",
                "period": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "loaded_at": datetime(2026, 1, 1, 1, 10, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            }
        ]
    )

    merge_partitioned_parquet(first_df, str(output_path), merge_keys=["event_id"], freshness_columns=["loaded_at"])
    merge_partitioned_parquet(second_df, str(output_path), merge_keys=["event_id"], freshness_columns=["loaded_at"])

    rows = spark_session.read.parquet(str(output_path)).orderBy("period").collect()

    assert len(rows) == 2
    assert [row["event_id"] for row in rows] == ["evt-1", "evt-2"]


def test_merge_partitioned_parquet_preserves_earlier_gold_hours_when_later_hour_arrives(spark_session, tmp_path) -> None:
    output_path = tmp_path / "merged_gold_dataset"
    first_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "actual_demand_mwh": 100.0,
                "day_ahead_forecast_mwh": 95.0,
                "loaded_at": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            }
        ]
    )
    second_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "actual_demand_mwh": 110.0,
                "day_ahead_forecast_mwh": 100.0,
                "loaded_at": datetime(2026, 1, 1, 1, 10, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            }
        ]
    )

    merge_partitioned_parquet(first_df, str(output_path), merge_keys=["period", "respondent"], freshness_columns=["loaded_at"])
    merge_partitioned_parquet(second_df, str(output_path), merge_keys=["period", "respondent"], freshness_columns=["loaded_at"])

    rows = spark_session.read.parquet(str(output_path)).orderBy("period").collect()

    assert len(rows) == 2
    assert [row["actual_demand_mwh"] for row in rows] == [100.0, 110.0]


def test_merge_partitioned_parquet_replaces_same_gold_key_with_latest_loaded_at(spark_session, tmp_path) -> None:
    output_path = tmp_path / "merged_gold_updates"
    first_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "actual_demand_mwh": 100.0,
                "day_ahead_forecast_mwh": 95.0,
                "loaded_at": datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            }
        ]
    )
    second_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "actual_demand_mwh": 120.0,
                "day_ahead_forecast_mwh": 98.0,
                "loaded_at": datetime(2026, 1, 1, 0, 20, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            }
        ]
    )

    merge_partitioned_parquet(first_df, str(output_path), merge_keys=["period", "respondent"], freshness_columns=["loaded_at"])
    merge_partitioned_parquet(second_df, str(output_path), merge_keys=["period", "respondent"], freshness_columns=["loaded_at"])

    rows = spark_session.read.parquet(str(output_path)).collect()

    assert len(rows) == 1
    assert rows[0]["actual_demand_mwh"] == 120.0


def test_region_demand_daily_aggregates_hourly_curated_gold_rows(spark_session) -> None:
    gold_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
                "actual_demand_mwh": 100.0,
                "loaded_at": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
            },
            {
                "period": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                "respondent": "PJM",
                "event_date": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).date(),
                "actual_demand_mwh": 200.0,
                "loaded_at": datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc),
            },
        ]
    )

    platinum_df = build_region_demand_daily(gold_df, "2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00")
    rows = platinum_df.collect()

    assert len(rows) == 1
    assert rows[0]["daily_demand_mwh"] == 300.0
    assert rows[0]["avg_hourly_demand_mwh"] == 150.0
    assert rows[0]["peak_hourly_demand_mwh"] == 200.0


def test_resource_planning_daily_aggregates_curated_gold_inputs(spark_session) -> None:
    region_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc).date(),
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "actual_demand_mwh": 100.0,
                "day_ahead_forecast_mwh": 90.0,
                "forecast_error_mwh": 10.0,
                "forecast_error_pct": 11.111111,
                "loaded_at": datetime(2026, 1, 3, 1, 0, tzinfo=timezone.utc),
            },
            {
                "period": datetime(2026, 1, 3, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc).date(),
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "actual_demand_mwh": 200.0,
                "day_ahead_forecast_mwh": 190.0,
                "forecast_error_mwh": 10.0,
                "forecast_error_pct": 5.263158,
                "loaded_at": datetime(2026, 1, 3, 2, 0, tzinfo=timezone.utc),
            },
        ]
    )
    fuel_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc).date(),
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "fueltype": "SUN",
                "fueltype_name": "Solar",
                "generation_mwh": 80.0,
                "loaded_at": datetime(2026, 1, 3, 1, 0, tzinfo=timezone.utc),
            },
            {
                "period": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc).date(),
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "fueltype": "NG",
                "fueltype_name": "Natural Gas",
                "generation_mwh": 20.0,
                "loaded_at": datetime(2026, 1, 3, 1, 0, tzinfo=timezone.utc),
            },
            {
                "period": datetime(2026, 1, 3, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc).date(),
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "fueltype": "SUN",
                "fueltype_name": "Solar",
                "generation_mwh": 50.0,
                "loaded_at": datetime(2026, 1, 3, 2, 0, tzinfo=timezone.utc),
            },
            {
                "period": datetime(2026, 1, 3, 1, 0, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc).date(),
                "respondent": "PJM",
                "respondent_name": "PJM Interconnection",
                "fueltype": "NG",
                "fueltype_name": "Natural Gas",
                "generation_mwh": 150.0,
                "loaded_at": datetime(2026, 1, 3, 2, 0, tzinfo=timezone.utc),
            },
        ]
    )
    fuel_dim_df = spark_session.createDataFrame(
        [
            {"fueltype": "SUN", "fueltype_name": "Solar", "fuel_category": "renewable", "emissions_factor_kg_per_mwh": 35.0, "updated_at": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc)},
            {"fueltype": "NG", "fueltype_name": "Natural Gas", "fuel_category": "fossil", "emissions_factor_kg_per_mwh": 490.0, "updated_at": datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc)},
        ]
    )

    planning_df = build_resource_planning_daily(region_df, fuel_df, fuel_dim_df)
    rows = planning_df.collect()

    assert len(rows) == 1
    assert rows[0]["daily_demand_mwh"] == 300.0
    assert rows[0]["peak_hourly_demand_mwh"] == 200.0
    assert rows[0]["renewable_share_pct"] == 43.333333333333336
    assert rows[0]["gas_share_pct"] == 56.666666666666664
    assert rows[0]["weekend_flag"] is True
