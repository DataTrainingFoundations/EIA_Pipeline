from __future__ import annotations

import math

import pandas as pd
from utility_strategy_logic import (
    aggregate_location_period,
    build_priority_history,
    build_strategy_thresholds,
    derive_strategy_driver,
    derive_strategy_priority,
    is_rollup_fuel,
    map_fuel_bucket,
    trend_label,
)


def test_map_fuel_bucket_prefers_ids_then_names() -> None:
    assert map_fuel_bucket("COL", "coal") == "Coal"
    assert map_fuel_bucket("NG", "natural gas") == "Gas"
    assert map_fuel_bucket(None, "Solar thermal") == "Renewable"
    assert map_fuel_bucket(None, "mystery fuel") == "Other"


def test_aggregate_location_period_builds_strategy_metrics() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "period": pd.Timestamp("2026-01-01", tz="UTC"),
                "location": "US",
                "location_name": "U.S. Total",
                "fueltype_id": "COL",
                "fueltype_name": "coal",
                "generation_mwh": 60.0,
                "fuel_heat_input_mmbtu": 180.0,
                "cost_usd": 20.0,
                "updated_at": pd.Timestamp("2026-01-02", tz="UTC"),
            },
            {
                "period": pd.Timestamp("2026-01-01", tz="UTC"),
                "location": "US",
                "location_name": "U.S. Total",
                "fueltype_id": "NG",
                "fueltype_name": "natural gas",
                "generation_mwh": 30.0,
                "fuel_heat_input_mmbtu": 60.0,
                "cost_usd": 10.0,
                "updated_at": pd.Timestamp("2026-01-02", tz="UTC"),
            },
            {
                "period": pd.Timestamp("2026-01-01", tz="UTC"),
                "location": "US",
                "location_name": "U.S. Total",
                "fueltype_id": "SUN",
                "fueltype_name": "solar",
                "generation_mwh": 10.0,
                "fuel_heat_input_mmbtu": None,
                "cost_usd": None,
                "updated_at": pd.Timestamp("2026-01-02", tz="UTC"),
            },
        ]
    )

    result = aggregate_location_period(raw_df)
    row = result.iloc[0]

    assert row["total_generation_mwh"] == 100.0
    assert row["renewable_share_pct"] == 10.0
    assert row["gas_share_pct"] == 30.0
    assert row["coal_share_pct"] == 60.0
    assert row["largest_fuel_share_pct"] == 60.0
    assert math.isclose(row["fuel_diversity_index"], 0.54)
    assert math.isclose(row["weighted_heat_rate_btu_per_kwh"], 2666.6666666666665)
    assert row["reported_cost_coverage_pct"] == 90.0
    assert math.isclose(row["reported_avg_fuel_cost_usd"], 16.6666666667, rel_tol=1e-6)


def test_aggregate_location_period_ignores_rollup_fuel_duplicates() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "period": pd.Timestamp("2026-01-01", tz="UTC"),
                "location": "US",
                "location_name": "U.S. Total",
                "fueltype_id": "ALL",
                "fueltype_name": "all fuels",
                "generation_mwh": 100.0,
                "fuel_heat_input_mmbtu": 0.0,
                "cost_usd": 0.0,
                "updated_at": pd.Timestamp("2026-01-02", tz="UTC"),
            },
            {
                "period": pd.Timestamp("2026-01-01", tz="UTC"),
                "location": "US",
                "location_name": "U.S. Total",
                "fueltype_id": "AOR",
                "fueltype_name": "all renewables",
                "generation_mwh": 40.0,
                "fuel_heat_input_mmbtu": 0.0,
                "cost_usd": 0.0,
                "updated_at": pd.Timestamp("2026-01-02", tz="UTC"),
            },
            {
                "period": pd.Timestamp("2026-01-01", tz="UTC"),
                "location": "US",
                "location_name": "U.S. Total",
                "fueltype_id": "SUN",
                "fueltype_name": "solar",
                "generation_mwh": 40.0,
                "fuel_heat_input_mmbtu": 0.0,
                "cost_usd": 0.0,
                "updated_at": pd.Timestamp("2026-01-02", tz="UTC"),
            },
            {
                "period": pd.Timestamp("2026-01-01", tz="UTC"),
                "location": "US",
                "location_name": "U.S. Total",
                "fueltype_id": "NG",
                "fueltype_name": "natural gas",
                "generation_mwh": 60.0,
                "fuel_heat_input_mmbtu": 180.0,
                "cost_usd": 12.0,
                "updated_at": pd.Timestamp("2026-01-02", tz="UTC"),
            },
        ]
    )

    row = aggregate_location_period(raw_df).iloc[0]

    assert row["total_generation_mwh"] == 100.0
    assert row["renewable_share_pct"] == 40.0
    assert row["gas_share_pct"] == 60.0


def test_priority_derivation_and_driver_use_latest_thresholds() -> None:
    latest_df = pd.DataFrame(
        [
            {
                "coal_share_pct": 65.0,
                "gas_share_pct": 20.0,
                "renewable_share_pct": 10.0,
                "largest_fuel_share_pct": 65.0,
                "fuel_diversity_index": 0.30,
                "weighted_heat_rate_btu_per_kwh": 5000.0,
            },
            {
                "coal_share_pct": 5.0,
                "gas_share_pct": 60.0,
                "renewable_share_pct": 15.0,
                "largest_fuel_share_pct": 60.0,
                "fuel_diversity_index": 0.35,
                "weighted_heat_rate_btu_per_kwh": 4500.0,
            },
            {
                "coal_share_pct": 0.0,
                "gas_share_pct": 10.0,
                "renewable_share_pct": 50.0,
                "largest_fuel_share_pct": 50.0,
                "fuel_diversity_index": 0.60,
                "weighted_heat_rate_btu_per_kwh": 2500.0,
            },
        ]
    )

    thresholds = build_strategy_thresholds(latest_df)
    row = latest_df.iloc[0]

    assert derive_strategy_priority(row, thresholds) == "Critical"
    assert derive_strategy_driver(row) == "Inefficient heat rate"


def test_sparse_snapshot_disables_quantile_priority_thresholds() -> None:
    latest_df = pd.DataFrame(
        [
            {
                "location": "US",
                "coal_share_pct": 65.0,
                "gas_share_pct": 20.0,
                "renewable_share_pct": 10.0,
                "largest_fuel_share_pct": 65.0,
                "fuel_diversity_index": 0.30,
                "weighted_heat_rate_btu_per_kwh": 5000.0,
            },
            {
                "location": "CA",
                "coal_share_pct": 5.0,
                "gas_share_pct": 60.0,
                "renewable_share_pct": 15.0,
                "largest_fuel_share_pct": 60.0,
                "fuel_diversity_index": 0.35,
                "weighted_heat_rate_btu_per_kwh": 4500.0,
            },
        ]
    )

    thresholds = build_strategy_thresholds(latest_df)

    assert all(value is None for value in thresholds.values())
    assert derive_strategy_priority(latest_df.iloc[0], thresholds) == "Stable"


def test_is_rollup_fuel_identifies_summary_rows() -> None:
    assert is_rollup_fuel("ALL", "all fuels") is True
    assert is_rollup_fuel("AOR", "all renewables") is True
    assert is_rollup_fuel("TSN", "estimated total solar") is True
    assert is_rollup_fuel("SUN", "solar") is False


def test_build_priority_history_orders_labels() -> None:
    df = pd.DataFrame(
        [
            {"period": pd.Timestamp("2026-01-01", tz="UTC"), "strategy_priority": "Stable"},
            {"period": pd.Timestamp("2026-01-01", tz="UTC"), "strategy_priority": "Critical"},
            {"period": pd.Timestamp("2026-02-01", tz="UTC"), "strategy_priority": "Elevated"},
        ]
    )

    history = build_priority_history(df)

    assert list(history["location_count"]) == [1, 1, 1]
    assert list(history["strategy_priority"].astype(str)) == [
        "Critical",
        "Stable",
        "Elevated",
    ]


def test_trend_label_reports_direction() -> None:
    improving, improving_delta = trend_label(
        pd.Series([60.0, 45.0, 40.0]),
        higher_is_better=False,
    )
    worsening, worsening_delta = trend_label(
        pd.Series([20.0, 30.0, 40.0]),
        higher_is_better=False,
    )

    assert improving == "Improving"
    assert improving_delta == -20.0
    assert worsening == "Worsening"
    assert worsening_delta == 20.0
