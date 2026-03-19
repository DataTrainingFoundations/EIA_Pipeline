from __future__ import annotations

import pandas as pd

from ui_utils import safe_quantile

PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]


def build_planning_thresholds(latest_snapshot_df: pd.DataFrame) -> dict[str, float | None]:
    return {
        "carbon_p90": safe_quantile(latest_snapshot_df.get("carbon_intensity_kg_per_mwh", pd.Series(dtype=float)), 0.9),
        "gas_p90": safe_quantile(latest_snapshot_df.get("peak_hour_gas_share_pct", pd.Series(dtype=float)), 0.9),
        "renewable_p10": safe_quantile(latest_snapshot_df.get("renewable_share_pct", pd.Series(dtype=float)), 0.1),
        "carbon_p75": safe_quantile(latest_snapshot_df.get("carbon_intensity_kg_per_mwh", pd.Series(dtype=float)), 0.75),
        "diversity_p25": safe_quantile(latest_snapshot_df.get("fuel_diversity_index", pd.Series(dtype=float)), 0.25),
        "forecast_p75": safe_quantile(latest_snapshot_df.get("avg_abs_forecast_error_pct", pd.Series(dtype=float)), 0.75),
    }


def derive_planning_priority(row: pd.Series, thresholds: dict[str, float | None]) -> str:
    carbon = pd.to_numeric(row.get("carbon_intensity_kg_per_mwh"), errors="coerce")
    gas = pd.to_numeric(row.get("peak_hour_gas_share_pct"), errors="coerce")
    renewable = pd.to_numeric(row.get("renewable_share_pct"), errors="coerce")
    diversity = pd.to_numeric(row.get("fuel_diversity_index"), errors="coerce")
    forecast = pd.to_numeric(row.get("avg_abs_forecast_error_pct"), errors="coerce")

    critical = (
        thresholds["carbon_p90"] is not None
        and carbon >= thresholds["carbon_p90"]
    ) or (
        thresholds["gas_p90"] is not None
        and gas >= thresholds["gas_p90"]
        and thresholds["renewable_p10"] is not None
        and renewable <= thresholds["renewable_p10"]
    )
    if critical:
        return "Critical"

    elevated = (
        thresholds["carbon_p75"] is not None
        and carbon >= thresholds["carbon_p75"]
    ) or (
        thresholds["diversity_p25"] is not None
        and diversity <= thresholds["diversity_p25"]
    ) or (
        thresholds["forecast_p75"] is not None
        and forecast >= thresholds["forecast_p75"]
    )
    if elevated:
        return "Elevated"
    return "Stable"


def derive_planning_driver(row: pd.Series, thresholds: dict[str, float | None]) -> str:
    carbon = pd.to_numeric(row.get("carbon_intensity_kg_per_mwh"), errors="coerce")
    gas = pd.to_numeric(row.get("peak_hour_gas_share_pct"), errors="coerce")
    renewable = pd.to_numeric(row.get("renewable_share_pct"), errors="coerce")
    diversity = pd.to_numeric(row.get("fuel_diversity_index"), errors="coerce")
    forecast = pd.to_numeric(row.get("avg_abs_forecast_error_pct"), errors="coerce")

    if thresholds["carbon_p75"] is not None and carbon >= thresholds["carbon_p75"]:
        return "High carbon intensity"
    if (
        thresholds["gas_p90"] is not None
        and gas >= thresholds["gas_p90"]
        and thresholds["renewable_p10"] is not None
        and renewable <= thresholds["renewable_p10"]
    ):
        return "Gas-dependent peak with weak renewables"
    if thresholds["diversity_p25"] is not None and diversity <= thresholds["diversity_p25"]:
        return "Low fuel diversity"
    if thresholds["forecast_p75"] is not None and forecast >= thresholds["forecast_p75"]:
        return "Forecast accuracy drift"
    return "Within expected range"


def build_priority_history(planning_df: pd.DataFrame, thresholds: dict[str, float | None]) -> pd.DataFrame:
    if planning_df.empty:
        return pd.DataFrame(columns=["date", "planning_priority", "respondent_count"])

    history_df = planning_df.copy()
    history_df["date"] = pd.to_datetime(history_df["date"])
    history_df["planning_priority"] = history_df.apply(derive_planning_priority, axis=1, thresholds=thresholds)

    grouped_df = (
        history_df.groupby(["date", "planning_priority"], as_index=False)["respondent"]
        .nunique()
        .rename(columns={"respondent": "respondent_count"})
    )

    all_dates = sorted(grouped_df["date"].dropna().unique().tolist())
    full_index = pd.MultiIndex.from_product([all_dates, PRIORITY_ORDER], names=["date", "planning_priority"])

    expanded_df = (
        grouped_df.set_index(["date", "planning_priority"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )
    expanded_df["planning_priority"] = pd.Categorical(
        expanded_df["planning_priority"],
        categories=PRIORITY_ORDER,
        ordered=True,
    )
    return expanded_df.sort_values(["date", "planning_priority"]).reset_index(drop=True)


__all__ = [
    "build_planning_thresholds",
    "derive_planning_priority",
    "derive_planning_driver",
    "build_priority_history",
]
