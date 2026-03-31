"""Pure helpers for the Utility Strategy Director power page."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

FUEL_SCORE_COLUMNS = [
    "score_renewable",
    "score_diversity",
    "score_concentration",
    "score_gas",
    "score_coal",
    "score_heat_rate",
]

PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]
ROLLUP_FUELTYPE_IDS = {
    "ALL",
    "AOR",
    "COW",
    "FOS",
    "NGO",
    "PEL",
    "PET",
    "REN",
    "TSN",
    "TPV",
}


def map_fuel_bucket(fueltype_id: str | None, fueltype_name: str | None) -> str:
    """Map raw fuel labels into strategy buckets."""

    fuel_id = (fueltype_id or "").strip().upper()
    fuel_name = (fueltype_name or "").strip().lower()

    if fuel_id in {"SUN", "WND", "WAT", "GEO", "BIO", "WOO", "WWW"}:
        return "Renewable"
    if fuel_id in {"NG", "OOG", "PG", "BFG"}:
        return "Gas"
    if fuel_id in {"COL", "WC", "RC", "LIG", "SUB", "BIT"}:
        return "Coal"
    if fuel_id in {"NUC", "NU"}:
        return "Nuclear"
    if fuel_id in {"DFO", "RFO", "PC", "JF", "KER", "WO"}:
        return "Oil"

    if any(word in fuel_name for word in ["solar", "wind", "hydro", "water", "geo", "biomass", "wood"]):
        return "Renewable"
    if "gas" in fuel_name:
        return "Gas"
    if "coal" in fuel_name:
        return "Coal"
    if "nuclear" in fuel_name:
        return "Nuclear"
    if any(word in fuel_name for word in ["oil", "petroleum", "diesel", "kerosene", "jet fuel"]):
        return "Oil"
    return "Other"


def is_rollup_fuel(fueltype_id: str | None, fueltype_name: str | None) -> bool:
    """Return whether a fuel row is a rollup/summary row rather than a leaf fuel."""

    fuel_id = (fueltype_id or "").strip().upper()
    fuel_name = (fueltype_name or "").strip().lower()
    if fuel_id in ROLLUP_FUELTYPE_IDS:
        return True
    return any(
        pattern in fuel_name
        for pattern in (
            "all fuels",
            "all renewables",
            "all coal",
            "fossil fuels",
            "renewable",
            "estimated total",
            "& other",
        )
    )


def _safe_share(numerator: float, denominator: float) -> float:
    if denominator <= 0 or math.isnan(denominator):
        return np.nan
    return numerator / denominator * 100.0


def _weighted_average(values: pd.Series, weights: pd.Series) -> float:
    valid = values.notna() & weights.notna()
    if not valid.any():
        return np.nan
    weight_sum = float(weights[valid].sum())
    if weight_sum <= 0:
        return np.nan
    return float((values[valid] * weights[valid]).sum() / weight_sum)


def aggregate_location_period(power_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate fuel rows to location-period strategy metrics."""

    if power_df.empty:
        return pd.DataFrame(
            columns=[
                "period",
                "location",
                "location_name",
                "total_generation_mwh",
                "renewable_share_pct",
                "gas_share_pct",
                "coal_share_pct",
                "nuclear_share_pct",
                "largest_fuel_share_pct",
                "fuel_diversity_index",
                "weighted_heat_rate_btu_per_kwh",
                "reported_cost_coverage_pct",
                "reported_avg_fuel_cost_usd",
                "updated_at",
            ]
        )

    working = power_df.copy()
    working["fuel_bucket"] = working.apply(
        lambda row: map_fuel_bucket(row.get("fueltype_id"), row.get("fueltype_name")),
        axis=1,
    )
    working["is_rollup_fuel"] = working.apply(
        lambda row: is_rollup_fuel(row.get("fueltype_id"), row.get("fueltype_name")),
        axis=1,
    )

    group_keys = ["period", "location", "location_name"]
    rows: list[dict[str, object]] = []

    for keys, group in working.groupby(group_keys, dropna=False, sort=True):
        period, location, location_name = keys
        detail_group = group[~group["is_rollup_fuel"]]
        use_group_rollups = detail_group.empty
        bucket_generation: dict[str, float] = {}
        bucket_costs: list[pd.DataFrame] = []
        for fuel_bucket, bucket_df in group.groupby("fuel_bucket", dropna=False, sort=True):
            detail_bucket_df = bucket_df[~bucket_df["is_rollup_fuel"]]
            if use_group_rollups:
                source_df = detail_bucket_df if not detail_bucket_df.empty else bucket_df
            else:
                source_df = detail_bucket_df
            if source_df.empty:
                continue
            bucket_generation[str(fuel_bucket)] = float(
                source_df["generation_mwh"].sum(min_count=1) or 0.0
            )
            bucket_costs.append(source_df)

        detail_df = pd.concat(bucket_costs, ignore_index=True) if bucket_costs else group.iloc[0:0]
        generation_by_bucket = pd.Series(bucket_generation, dtype=float)
        total_generation = float(generation_by_bucket.sum() or 0.0)
        share_fractions = (
            generation_by_bucket.fillna(0.0) / total_generation
            if total_generation > 0
            else pd.Series(dtype=float)
        )
        cost_generation = detail_df.loc[
            detail_df["cost_usd"].notna(), "generation_mwh"
        ].sum(min_count=1)
        heat_rate = np.nan
        valid_heat = detail_df["fuel_heat_input_mmbtu"].notna() & detail_df["generation_mwh"].notna()
        if valid_heat.any():
            generation_sum = float(detail_df.loc[valid_heat, "generation_mwh"].sum())
            if generation_sum > 0:
                heat_rate = float(
                    detail_df.loc[valid_heat, "fuel_heat_input_mmbtu"].sum() * 1000.0 / generation_sum
                )

        rows.append(
            {
                "period": period,
                "location": location,
                "location_name": location_name,
                "total_generation_mwh": total_generation if total_generation > 0 else np.nan,
                "renewable_share_pct": _safe_share(float(generation_by_bucket.get("Renewable", 0.0)), total_generation),
                "gas_share_pct": _safe_share(float(generation_by_bucket.get("Gas", 0.0)), total_generation),
                "coal_share_pct": _safe_share(float(generation_by_bucket.get("Coal", 0.0)), total_generation),
                "nuclear_share_pct": _safe_share(float(generation_by_bucket.get("Nuclear", 0.0)), total_generation),
                "largest_fuel_share_pct": float(share_fractions.max() * 100.0) if not share_fractions.empty else np.nan,
                "fuel_diversity_index": float(1.0 - (share_fractions.pow(2).sum())) if not share_fractions.empty else np.nan,
                "weighted_heat_rate_btu_per_kwh": heat_rate,
                "reported_cost_coverage_pct": _safe_share(float(cost_generation or 0.0), total_generation),
                "reported_avg_fuel_cost_usd": _weighted_average(detail_df["cost_usd"], detail_df["generation_mwh"]),
                "updated_at": group["updated_at"].max(),
            }
        )

    return pd.DataFrame(rows).sort_values(["period", "location"]).reset_index(drop=True)


def build_strategy_thresholds(latest_snapshot_df: pd.DataFrame) -> dict[str, float | None]:
    """Build latest-snapshot quantile thresholds."""

    location_count = (
        latest_snapshot_df["location"].nunique(dropna=True)
        if "location" in latest_snapshot_df.columns
        else len(latest_snapshot_df.index)
    )
    if location_count < 3:
        return {
            "coal_p90": None,
            "coal_p75": None,
            "gas_p90": None,
            "gas_p75": None,
            "renewable_p25": None,
            "concentration_p90": None,
            "concentration_p75": None,
            "diversity_p10": None,
            "diversity_p25": None,
            "heat_rate_p90": None,
            "heat_rate_p75": None,
        }

    def q(series: pd.Series, quantile: float) -> float | None:
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return None
        return float(numeric.quantile(quantile))

    return {
        "coal_p90": q(latest_snapshot_df["coal_share_pct"], 0.90),
        "coal_p75": q(latest_snapshot_df["coal_share_pct"], 0.75),
        "gas_p90": q(latest_snapshot_df["gas_share_pct"], 0.90),
        "gas_p75": q(latest_snapshot_df["gas_share_pct"], 0.75),
        "renewable_p25": q(latest_snapshot_df["renewable_share_pct"], 0.25),
        "concentration_p90": q(latest_snapshot_df["largest_fuel_share_pct"], 0.90),
        "concentration_p75": q(latest_snapshot_df["largest_fuel_share_pct"], 0.75),
        "diversity_p10": q(latest_snapshot_df["fuel_diversity_index"], 0.10),
        "diversity_p25": q(latest_snapshot_df["fuel_diversity_index"], 0.25),
        "heat_rate_p90": q(latest_snapshot_df["weighted_heat_rate_btu_per_kwh"], 0.90),
        "heat_rate_p75": q(latest_snapshot_df["weighted_heat_rate_btu_per_kwh"], 0.75),
    }


def derive_strategy_priority(row: pd.Series, thresholds: dict[str, float | None]) -> str:
    """Assign a priority label to a location snapshot row."""

    coal = pd.to_numeric(row.get("coal_share_pct"), errors="coerce")
    gas = pd.to_numeric(row.get("gas_share_pct"), errors="coerce")
    renewable = pd.to_numeric(row.get("renewable_share_pct"), errors="coerce")
    concentration = pd.to_numeric(row.get("largest_fuel_share_pct"), errors="coerce")
    diversity = pd.to_numeric(row.get("fuel_diversity_index"), errors="coerce")
    heat_rate = pd.to_numeric(row.get("weighted_heat_rate_btu_per_kwh"), errors="coerce")

    critical = (
        (thresholds["coal_p90"] is not None and coal >= thresholds["coal_p90"])
        or (
            thresholds["concentration_p90"] is not None
            and concentration >= thresholds["concentration_p90"]
            and thresholds["diversity_p10"] is not None
            and diversity <= thresholds["diversity_p10"]
        )
        or (
            thresholds["gas_p90"] is not None
            and gas >= thresholds["gas_p90"]
            and thresholds["renewable_p25"] is not None
            and renewable <= thresholds["renewable_p25"]
        )
        or (
            thresholds["heat_rate_p90"] is not None
            and heat_rate >= thresholds["heat_rate_p90"]
        )
    )
    if critical:
        return "Critical"

    elevated = (
        (thresholds["coal_p75"] is not None and coal >= thresholds["coal_p75"])
        or (
            thresholds["concentration_p75"] is not None
            and concentration >= thresholds["concentration_p75"]
        )
        or (
            thresholds["diversity_p25"] is not None
            and diversity <= thresholds["diversity_p25"]
        )
        or (thresholds["gas_p75"] is not None and gas >= thresholds["gas_p75"])
        or (
            thresholds["renewable_p25"] is not None
            and renewable <= thresholds["renewable_p25"]
        )
        or (
            thresholds["heat_rate_p75"] is not None
            and heat_rate >= thresholds["heat_rate_p75"]
        )
    )
    if elevated:
        return "Elevated"
    return "Stable"


def derive_strategy_driver(row: pd.Series) -> str:
    """Derive the dominant structural drag for a location."""

    candidates = {
        "Coal dependence": pd.to_numeric(row.get("coal_share_pct"), errors="coerce"),
        "Gas dependence": pd.to_numeric(row.get("gas_share_pct"), errors="coerce"),
        "Low renewable share": 100.0 - pd.to_numeric(row.get("renewable_share_pct"), errors="coerce"),
        "Fuel concentration": pd.to_numeric(row.get("largest_fuel_share_pct"), errors="coerce"),
        "Low fuel diversity": 100.0 - (pd.to_numeric(row.get("fuel_diversity_index"), errors="coerce") * 100.0),
        "Inefficient heat rate": pd.to_numeric(row.get("weighted_heat_rate_btu_per_kwh"), errors="coerce"),
    }
    valid = {key: value for key, value in candidates.items() if pd.notna(value)}
    if not valid:
        return "Within expected range"
    return max(valid.items(), key=lambda item: item[1])[0]


def rank_priority_labels(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Convert priority labels into sortable categorical labels."""

    ranked = df.copy()
    ranked[column_name] = pd.Categorical(
        ranked[column_name],
        categories=PRIORITY_ORDER,
        ordered=True,
    )
    return ranked


def normalize_direct(series: pd.Series) -> pd.Series:
    """Normalize a metric where higher values are better."""

    numeric = pd.to_numeric(series, errors="coerce")
    min_val = numeric.min()
    max_val = numeric.max()
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return pd.Series([50.0] * len(numeric), index=numeric.index)
    return ((numeric - min_val) / (max_val - min_val) * 100.0).round(1)


def normalize_inverse(series: pd.Series) -> pd.Series:
    """Normalize a metric where lower values are better."""

    normalized = normalize_direct(series)
    return (100.0 - normalized).round(1)


def build_priority_history(location_period_df: pd.DataFrame) -> pd.DataFrame:
    """Return daily counts of locations by priority."""

    if location_period_df.empty:
        return pd.DataFrame(columns=["period", "strategy_priority", "location_count"])
    history = (
        location_period_df.groupby(["period", "strategy_priority"], as_index=False)
        .size()
        .rename(columns={"size": "location_count"})
    )
    history["strategy_priority"] = pd.Categorical(
        history["strategy_priority"],
        categories=PRIORITY_ORDER,
        ordered=True,
    )
    return history.sort_values(["period", "strategy_priority"]).reset_index(drop=True)


def trend_label(values: pd.Series, *, higher_is_better: bool) -> tuple[str, float]:
    """Summarize a selected trend with a label and raw delta."""

    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if len(numeric) < 2:
        return "Insufficient history", np.nan

    delta = float(numeric.iloc[-1] - numeric.iloc[0])
    tolerance = max(abs(float(numeric.mean())) * 0.01, 0.01)
    if abs(delta) <= tolerance:
        return "Stable", delta
    if higher_is_better:
        return ("Improving", delta) if delta > 0 else ("Worsening", delta)
    return ("Improving", delta) if delta < 0 else ("Worsening", delta)
