from __future__ import annotations

import pandas as pd

import planning_page_logic


def test_build_planning_thresholds_returns_quantiles_and_handles_empty_values() -> None:
    thresholds = planning_page_logic.build_planning_thresholds(
        pd.DataFrame(
            {
                "carbon_intensity_kg_per_mwh": [100.0, 200.0, 300.0, 400.0],
                "peak_hour_gas_share_pct": [10.0, 20.0, 30.0, 40.0],
                "renewable_share_pct": [60.0, 50.0, 40.0, 30.0],
                "fuel_diversity_index": [0.8, 0.7, 0.6, 0.5],
                "avg_abs_forecast_error_pct": [1.0, 2.0, 3.0, 4.0],
            }
        )
    )

    assert thresholds == {
        "carbon_p90": 370.0,
        "gas_p90": 37.0,
        "renewable_p10": 33.0,
        "carbon_p75": 325.0,
        "diversity_p25": 0.575,
        "forecast_p75": 3.25,
    }

    empty_thresholds = planning_page_logic.build_planning_thresholds(pd.DataFrame())
    assert empty_thresholds == {
        "carbon_p90": None,
        "gas_p90": None,
        "renewable_p10": None,
        "carbon_p75": None,
        "diversity_p25": None,
        "forecast_p75": None,
    }


def test_derive_planning_priority_covers_expected_branches() -> None:
    thresholds = {
        "carbon_p90": 300.0,
        "gas_p90": 80.0,
        "renewable_p10": 15.0,
        "carbon_p75": 250.0,
        "diversity_p25": 0.30,
        "forecast_p75": 5.0,
    }

    assert (
        planning_page_logic.derive_planning_priority(
            pd.Series({"carbon_intensity_kg_per_mwh": 310.0}),
            thresholds,
        )
        == "Critical"
    )
    assert (
        planning_page_logic.derive_planning_priority(
            pd.Series({"peak_hour_gas_share_pct": 90.0, "renewable_share_pct": 10.0}),
            thresholds,
        )
        == "Critical"
    )
    assert (
        planning_page_logic.derive_planning_priority(
            pd.Series({"fuel_diversity_index": 0.20}),
            thresholds,
        )
        == "Elevated"
    )
    assert (
        planning_page_logic.derive_planning_priority(
            pd.Series({"avg_abs_forecast_error_pct": 6.0}),
            thresholds,
        )
        == "Elevated"
    )
    assert (
        planning_page_logic.derive_planning_priority(
            pd.Series(
                {
                    "carbon_intensity_kg_per_mwh": 120.0,
                    "peak_hour_gas_share_pct": 35.0,
                    "renewable_share_pct": 45.0,
                    "fuel_diversity_index": 0.55,
                    "avg_abs_forecast_error_pct": 2.0,
                }
            ),
            thresholds,
        )
        == "Stable"
    )


def test_derive_planning_driver_uses_fixed_precedence() -> None:
    thresholds = {
        "carbon_p90": 300.0,
        "gas_p90": 80.0,
        "renewable_p10": 15.0,
        "carbon_p75": 250.0,
        "diversity_p25": 0.30,
        "forecast_p75": 5.0,
    }

    row = pd.Series(
        {
            "carbon_intensity_kg_per_mwh": 275.0,
            "peak_hour_gas_share_pct": 95.0,
            "renewable_share_pct": 5.0,
            "fuel_diversity_index": 0.20,
            "avg_abs_forecast_error_pct": 8.0,
        }
    )
    assert planning_page_logic.derive_planning_driver(row, thresholds) == "High carbon intensity"

    assert (
        planning_page_logic.derive_planning_driver(
            pd.Series({"peak_hour_gas_share_pct": 90.0, "renewable_share_pct": 10.0}),
            thresholds,
        )
        == "Gas-dependent peak with weak renewables"
    )
    assert (
        planning_page_logic.derive_planning_driver(
            pd.Series({"fuel_diversity_index": 0.20}),
            thresholds,
        )
        == "Low fuel diversity"
    )
    assert (
        planning_page_logic.derive_planning_driver(
            pd.Series({"avg_abs_forecast_error_pct": 6.0}),
            thresholds,
        )
        == "Forecast accuracy drift"
    )
    assert (
        planning_page_logic.derive_planning_driver(
            pd.Series({"avg_abs_forecast_error_pct": 2.0}),
            thresholds,
        )
        == "Within expected range"
    )


def test_build_priority_history_uses_fixed_thresholds_for_daily_bucket_counts() -> None:
    thresholds = {
        "carbon_p90": 300.0,
        "gas_p90": 80.0,
        "renewable_p10": 15.0,
        "carbon_p75": 250.0,
        "diversity_p25": 0.30,
        "forecast_p75": 5.0,
    }
    planning_df = pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "respondent": "A",
                "carbon_intensity_kg_per_mwh": 320.0,
                "peak_hour_gas_share_pct": 30.0,
                "renewable_share_pct": 40.0,
                "fuel_diversity_index": 0.70,
                "avg_abs_forecast_error_pct": 2.0,
            },
            {
                "date": "2026-03-01",
                "respondent": "B",
                "carbon_intensity_kg_per_mwh": 110.0,
                "peak_hour_gas_share_pct": 90.0,
                "renewable_share_pct": 10.0,
                "fuel_diversity_index": 0.70,
                "avg_abs_forecast_error_pct": 2.0,
            },
            {
                "date": "2026-03-01",
                "respondent": "C",
                "carbon_intensity_kg_per_mwh": 120.0,
                "peak_hour_gas_share_pct": 25.0,
                "renewable_share_pct": 45.0,
                "fuel_diversity_index": 0.20,
                "avg_abs_forecast_error_pct": 2.0,
            },
            {
                "date": "2026-03-02",
                "respondent": "A",
                "carbon_intensity_kg_per_mwh": 120.0,
                "peak_hour_gas_share_pct": 20.0,
                "renewable_share_pct": 55.0,
                "fuel_diversity_index": 0.60,
                "avg_abs_forecast_error_pct": 2.0,
            },
            {
                "date": "2026-03-02",
                "respondent": "B",
                "carbon_intensity_kg_per_mwh": 330.0,
                "peak_hour_gas_share_pct": 20.0,
                "renewable_share_pct": 45.0,
                "fuel_diversity_index": 0.60,
                "avg_abs_forecast_error_pct": 2.0,
            },
            {
                "date": "2026-03-02",
                "respondent": "C",
                "carbon_intensity_kg_per_mwh": 110.0,
                "peak_hour_gas_share_pct": 20.0,
                "renewable_share_pct": 45.0,
                "fuel_diversity_index": 0.60,
                "avg_abs_forecast_error_pct": 6.0,
            },
        ]
    )

    history_df = planning_page_logic.build_priority_history(planning_df, thresholds)

    assert history_df.to_dict("records") == [
        {"date": pd.Timestamp("2026-03-01"), "planning_priority": "Critical", "respondent_count": 2},
        {"date": pd.Timestamp("2026-03-01"), "planning_priority": "Elevated", "respondent_count": 1},
        {"date": pd.Timestamp("2026-03-01"), "planning_priority": "Stable", "respondent_count": 0},
        {"date": pd.Timestamp("2026-03-02"), "planning_priority": "Critical", "respondent_count": 1},
        {"date": pd.Timestamp("2026-03-02"), "planning_priority": "Elevated", "respondent_count": 1},
        {"date": pd.Timestamp("2026-03-02"), "planning_priority": "Stable", "respondent_count": 1},
    ]
