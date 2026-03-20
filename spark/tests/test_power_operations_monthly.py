from datetime import datetime, timezone

from jobs.gold_power_operations_monthly import build_power_operations_monthly_fact
from jobs.platinum_power_operations_monthly import build_power_operations_monthly
from jobs.silver_clean_transform import clean_power_operational_data


def test_clean_power_operational_data_parses_monthly_rows(spark_session) -> None:
    bronze_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "dataset": "electricity_power_operational_data",
                "payload": {
                    "period": "2026-01",
                    "location": "US",
                    "stateDescription": "U.S. Total",
                    "sectorid": "1",
                    "sectorDescription": "Electric Utility",
                    "fueltypeid": "COL",
                    "fuelTypeDescription": "coal",
                    "ash-content": "12.5",
                    "ash-content-units": "percent",
                    "consumption-for-eg": "10",
                    "consumption-for-eg-units": "thousand physical units",
                    "generation": "20",
                    "generation-units": "thousand megawatthours",
                    "heat-content": "20",
                    "heat-content-units": "Btu per physical units",
                },
                "ingestion_ts": datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc),
            }
        ]
    )

    cleaned_df = clean_power_operational_data(bronze_df)
    row = cleaned_df.selectExpr("date_format(period, 'yyyy-MM') as period_month", "location", "sector_id", "fueltype_id", "generation_thousand_mwh").collect()[0]

    assert row["period_month"] == "2026-01"
    assert row["location"] == "US"
    assert row["sector_id"] == "1"
    assert row["fueltype_id"] == "COL"
    assert row["generation_thousand_mwh"] == 20.0


def test_gold_power_operations_monthly_fact_keeps_latest_business_key(spark_session) -> None:
    silver_df = spark_session.createDataFrame(
        [
            {
                "event_id": "evt-1",
                "period": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                "location": "US",
                "location_name": "U.S. Total",
                "sector_id": "1",
                "sector_name": "Electric Utility",
                "fueltype_id": "COL",
                "fueltype_name": "coal",
                "ash_content_pct": 10.0,
                "consumption_for_eg_thousand_units": 5.0,
                "generation_thousand_mwh": 20.0,
                "heat_content_btu_per_unit": 1000.0,
                "loaded_at": datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc),
            },
            {
                "event_id": "evt-2",
                "period": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                "location": "US",
                "location_name": "U.S. Total",
                "sector_id": "1",
                "sector_name": "Electric Utility",
                "fueltype_id": "COL",
                "fueltype_name": "coal",
                "ash_content_pct": 12.0,
                "consumption_for_eg_thousand_units": 6.0,
                "generation_thousand_mwh": 30.0,
                "heat_content_btu_per_unit": 1100.0,
                "loaded_at": datetime(2026, 1, 2, 1, 0, tzinfo=timezone.utc),
            },
        ]
    )

    gold_df = build_power_operations_monthly_fact(silver_df)
    row = gold_df.collect()[0]

    assert gold_df.count() == 1
    assert row["generation_mwh"] == 30000.0
    assert row["ash_content_pct"] == 12.0
    assert row["consumption_for_eg_thousand_units"] == 6.0


def test_platinum_power_operations_monthly_derives_share_and_heat_rate(spark_session) -> None:
    gold_df = spark_session.createDataFrame(
        [
            {
                "period": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                "location": "US",
                "location_name": "U.S. Total",
                "sector_id": "1",
                "sector_name": "Electric Utility",
                "fueltype_id": "COL",
                "fueltype_name": "coal",
                "generation_mwh": 60000.0,
                "consumption_for_eg_thousand_units": 10.0,
                "ash_content_pct": 12.0,
                "heat_content_btu_per_unit": 20.0,
                "loaded_at": datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc),
            },
            {
                "period": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "event_date": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                "location": "US",
                "location_name": "U.S. Total",
                "sector_id": "1",
                "sector_name": "Electric Utility",
                "fueltype_id": "NG",
                "fueltype_name": "natural gas",
                "generation_mwh": 40000.0,
                "consumption_for_eg_thousand_units": 20.0,
                "ash_content_pct": 0.0,
                "heat_content_btu_per_unit": 1.0,
                "loaded_at": datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc),
            },
        ]
    )

    platinum_df = build_power_operations_monthly(gold_df, "2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00")
    rows = {row["fueltype_id"]: row for row in platinum_df.collect()}

    assert rows["COL"]["generation_share_pct"] == 60.0
    assert rows["NG"]["generation_share_pct"] == 40.0
    assert rows["COL"]["fuel_heat_input_mmbtu"] == 200000.0
    assert rows["COL"]["heat_rate_btu_per_kwh"] == 3333.3333333333335
