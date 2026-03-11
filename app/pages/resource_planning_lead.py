"""Resource Planning Lead dashboard.

This page reads the planning serving table from Postgres through `data_access`,
derives a ranked planning watchlist in pandas, and renders the comparison and
trend views used for longer-horizon planning decisions.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import (
    get_planning_coverage,
    list_respondents,
    load_latest_planning_snapshot,
    load_planning_watchlist,
    load_resource_planning_daily,
    table_has_rows,
)
from respondent_geo import RESPONDENT_GEO
from ui_utils import build_default_date_range, coerce_numeric, get_timezone_options, rank_priority_labels, safe_quantile

NUMERIC_COLUMNS = [
    "daily_demand_mwh",
    "peak_hourly_demand_mwh",
    "avg_abs_forecast_error_pct",
    "renewable_share_pct",
    "fossil_share_pct",
    "gas_share_pct",
    "carbon_intensity_kg_per_mwh",
    "fuel_diversity_index",
    "peak_hour_gas_share_pct",
    "clean_coverage_ratio",
]

PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]


def _derive_planning_priority(row: pd.Series, thresholds: dict[str, float | None]) -> str:
    """Classify one respondent into the planning priority used on this page."""

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


st.set_page_config(page_title="Resource Planning Lead", layout="wide")
st.title("Resource Planning Lead")
st.caption("Use this page to identify where structural planning attention is needed and which trend explains it.")

with st.expander("How to read this page"):
    st.markdown(
        """
- Start with the KPI row and planning watchlist to identify which respondents need structural attention.
- Use the latest-date rankings to compare carbon intensity, renewable share, and peak-hour gas dependence.
- Use focus trends to understand whether the issue is persistent or improving.
- Supporting analysis is intentionally secondary to the watchlist.
"""
    )

if not table_has_rows("platinum.resource_planning_daily"):
    st.warning("No planning mart rows found yet. Let the `platinum_resource_planning_daily` DAG run first.")
    st.stop()

coverage = get_planning_coverage()
max_date = pd.to_datetime(coverage["max_date"])
min_date = pd.to_datetime(coverage["min_date"])
default_start_date, default_end_date = build_default_date_range(min_date, max_date, lookback_days=30)
respondents = list_respondents("platinum.resource_planning_daily")

col_a, col_b, col_c = st.columns([2, 2, 1])
selected_range = col_a.date_input("Date range", value=(default_start_date, default_end_date), min_value=min_date.date(), max_value=max_date.date())
selected_respondents = col_b.multiselect("Respondents", respondents, default=respondents)
timezone_label = col_c.selectbox("Date label", get_timezone_options(), index=0)

if len(selected_range) != 2:
    st.stop()

# Load the filtered warehouse slices first, then derive thresholds and rankings
# locally so new teammates can trace how every chart is produced.
start_date, end_date = selected_range
filtered_respondents = selected_respondents or None
planning_df = load_resource_planning_daily(str(start_date), str(end_date), filtered_respondents)
latest_snapshot_df = load_latest_planning_snapshot(str(start_date), str(end_date), filtered_respondents)
watchlist_df = load_planning_watchlist(str(start_date), str(end_date), filtered_respondents)

if planning_df.empty or latest_snapshot_df.empty or watchlist_df.empty:
    st.warning("No planning rows found for the selected filters.")
    st.stop()

planning_df["date"] = pd.to_datetime(planning_df["date"])
planning_df = coerce_numeric(planning_df, NUMERIC_COLUMNS)
planning_df["day_type"] = planning_df["weekend_flag"].map(lambda value: "Weekend" if value else "Weekday")

latest_snapshot_df["date"] = pd.to_datetime(latest_snapshot_df["date"])
latest_snapshot_df = coerce_numeric(latest_snapshot_df, NUMERIC_COLUMNS)

watchlist_df["date"] = pd.to_datetime(watchlist_df["date"])
watchlist_df = coerce_numeric(watchlist_df, NUMERIC_COLUMNS)
thresholds = {
    "carbon_p90": safe_quantile(watchlist_df["carbon_intensity_kg_per_mwh"], 0.9),
    "gas_p90": safe_quantile(watchlist_df["peak_hour_gas_share_pct"], 0.9),
    "renewable_p10": safe_quantile(watchlist_df["renewable_share_pct"], 0.1),
    "carbon_p75": safe_quantile(watchlist_df["carbon_intensity_kg_per_mwh"], 0.75),
    "diversity_p25": safe_quantile(watchlist_df["fuel_diversity_index"], 0.25),
    "forecast_p75": safe_quantile(watchlist_df["avg_abs_forecast_error_pct"], 0.75),
}
watchlist_df["planning_priority"] = watchlist_df.apply(_derive_planning_priority, axis=1, thresholds=thresholds)
watchlist_df = rank_priority_labels(watchlist_df, "planning_priority", PRIORITY_ORDER)
watchlist_df = watchlist_df.sort_values(
    ["planning_priority", "carbon_intensity_kg_per_mwh", "peak_hour_gas_share_pct", "renewable_share_pct"],
    ascending=[True, False, False, True],
    kind="stable",
)

focus_options = watchlist_df["respondent"].dropna().tolist()
focus_respondent = st.selectbox("Focus respondent", focus_options, index=0)
focus_df = planning_df[planning_df["respondent"] == focus_respondent].copy().sort_values("date")

latest_date = latest_snapshot_df["date"].max()
metric1, metric2, metric3, metric4, metric5 = st.columns(5)
# The KPI row gives a fast "what is the current planning picture?" summary.
metric1.metric("Latest planning date", str(latest_date.date()))
metric2.metric("Respondents in scope", f"{watchlist_df['respondent'].nunique():,}")
metric3.metric("Highest carbon intensity", f"{watchlist_df['carbon_intensity_kg_per_mwh'].dropna().max():.1f}" if not watchlist_df["carbon_intensity_kg_per_mwh"].dropna().empty else "n/a")
metric4.metric("Lowest renewable share", f"{watchlist_df['renewable_share_pct'].dropna().min():.1f}%" if not watchlist_df["renewable_share_pct"].dropna().empty else "n/a")
metric5.metric("Highest peak-hour gas dependence", f"{watchlist_df['peak_hour_gas_share_pct'].dropna().max():.1f}%" if not watchlist_df["peak_hour_gas_share_pct"].dropna().empty else "n/a")

st.caption(f"Latest daily data shown through {latest_date.date()} | date label setting: `{timezone_label}`")

st.subheader("What needs structural attention now?")
st.caption("This watchlist is ranked from the latest planning snapshot using current-window quantile thresholds.")
st.dataframe(
    watchlist_df[
        [
            "respondent",
            "respondent_name",
            "daily_demand_mwh",
            "renewable_share_pct",
            "carbon_intensity_kg_per_mwh",
            "fuel_diversity_index",
            "peak_hour_gas_share_pct",
            "avg_abs_forecast_error_pct",
            "planning_priority",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

st.subheader("Where is structural attention needed?")
st.caption("These latest-date rankings show which respondents stand out most on the key planning signals.")
rank_col1, rank_col2, rank_col3 = st.columns(3)

carbon_rank_df = latest_snapshot_df.dropna(subset=["carbon_intensity_kg_per_mwh"]).sort_values("carbon_intensity_kg_per_mwh", ascending=False).head(10)
if carbon_rank_df.empty:
    rank_col1.info("No carbon intensity rows are available at the latest date.")
else:
    rank_col1.plotly_chart(
        px.bar(
            carbon_rank_df.sort_values("carbon_intensity_kg_per_mwh", ascending=True),
            x="carbon_intensity_kg_per_mwh",
            y="respondent",
            orientation="h",
            labels={"carbon_intensity_kg_per_mwh": "kg CO2e per MWh", "respondent": "Respondent"},
            title="Highest carbon intensity",
        ),
        use_container_width=True,
    )

renewable_rank_df = latest_snapshot_df.dropna(subset=["renewable_share_pct"]).sort_values("renewable_share_pct", ascending=True).head(10)
if renewable_rank_df.empty:
    rank_col2.info("No renewable share rows are available at the latest date.")
else:
    rank_col2.plotly_chart(
        px.bar(
            renewable_rank_df.sort_values("renewable_share_pct", ascending=False),
            x="renewable_share_pct",
            y="respondent",
            orientation="h",
            labels={"renewable_share_pct": "Renewable share (%)", "respondent": "Respondent"},
            title="Lowest renewable share",
        ),
        use_container_width=True,
    )

gas_rank_df = latest_snapshot_df.dropna(subset=["peak_hour_gas_share_pct"]).sort_values("peak_hour_gas_share_pct", ascending=False).head(10)
if gas_rank_df.empty:
    rank_col3.info("No peak-hour gas rows are available at the latest date.")
else:
    rank_col3.plotly_chart(
        px.bar(
            gas_rank_df.sort_values("peak_hour_gas_share_pct", ascending=True),
            x="peak_hour_gas_share_pct",
            y="respondent",
            orientation="h",
            labels={"peak_hour_gas_share_pct": "Gas share at peak (%)", "respondent": "Respondent"},
            title="Highest peak-hour gas dependence",
        ),
        use_container_width=True,
    )

st.subheader(f"What trend explains {focus_respondent}?")
st.caption("These trends show whether the selected respondent is improving or drifting further onto the watchlist.")
trend_col1, trend_col2 = st.columns(2)
trend_col3, trend_col4 = st.columns(2)

carbon_df = focus_df.dropna(subset=["carbon_intensity_kg_per_mwh"])
if carbon_df.empty:
    trend_col1.info("No carbon intensity rows are available for this respondent.")
else:
    trend_col1.plotly_chart(
        px.line(
            carbon_df,
            x="date",
            y="carbon_intensity_kg_per_mwh",
            labels={"date": "Date", "carbon_intensity_kg_per_mwh": "kg CO2e per MWh"},
            title="Is carbon intensity improving?",
        ),
        use_container_width=True,
    )

renewable_df = focus_df.dropna(subset=["renewable_share_pct"])
if renewable_df.empty:
    trend_col2.info("No renewable share rows are available for this respondent.")
else:
    trend_col2.plotly_chart(
        px.line(
            renewable_df,
            x="date",
            y="renewable_share_pct",
            labels={"date": "Date", "renewable_share_pct": "Renewable share (%)"},
            title="Is renewable share improving?",
        ),
        use_container_width=True,
    )

gas_df = focus_df.dropna(subset=["peak_hour_gas_share_pct"])
if gas_df.empty:
    trend_col3.info("No peak-hour gas rows are available for this respondent.")
else:
    trend_col3.plotly_chart(
        px.line(
            gas_df,
            x="date",
            y="peak_hour_gas_share_pct",
            labels={"date": "Date", "peak_hour_gas_share_pct": "Gas share at peak (%)"},
            title="Is peak-hour gas dependence rising?",
        ),
        use_container_width=True,
    )

forecast_df = focus_df.dropna(subset=["avg_abs_forecast_error_pct"])
if forecast_df.empty:
    trend_col4.info("No forecast accuracy rows are available for this respondent.")
else:
    trend_col4.plotly_chart(
        px.line(
            forecast_df,
            x="date",
            y="avg_abs_forecast_error_pct",
            labels={"date": "Date", "avg_abs_forecast_error_pct": "Average absolute forecast error (%)"},
            title="Is forecast accuracy worsening?",
        ),
        use_container_width=True,
    )

st.subheader("Comparative context at the latest date")
st.caption("Use this table to sort respondents by the latest planning signals and validate the watchlist ordering.")
st.dataframe(
    latest_snapshot_df[
        [
            "respondent",
            "renewable_share_pct",
            "carbon_intensity_kg_per_mwh",
            "fuel_diversity_index",
            "peak_hour_gas_share_pct",
            "clean_coverage_ratio",
        ]
    ].sort_values("carbon_intensity_kg_per_mwh", ascending=False),
    use_container_width=True,
    hide_index=True,
)

with st.expander("Supporting analysis", expanded=False):
    # These views are supporting context after the watchlist and latest-date comparisons.
    support_col1, support_col2 = st.columns(2)

    drift_df = planning_df.dropna(subset=["daily_demand_mwh"]).groupby(["respondent", "day_type"], as_index=False)["daily_demand_mwh"].mean()
    if drift_df.empty:
        support_col1.info("No weekday versus weekend drift rows are available.")
    else:
        support_col1.plotly_chart(
            px.bar(
                drift_df,
                x="respondent",
                y="daily_demand_mwh",
                color="day_type",
                barmode="group",
                labels={"daily_demand_mwh": "Average daily demand (MWh)", "respondent": "Respondent"},
                title="Weekend versus weekday demand drift",
            ),
            use_container_width=True,
        )

    map_df = latest_snapshot_df.copy()
    map_df["lat"] = map_df["respondent"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lat"))
    map_df["lon"] = map_df["respondent"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lon"))
    map_df["location_label"] = map_df["respondent"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("label", code))
    map_df = map_df.dropna(subset=["lat", "lon", "renewable_share_pct"])
    if map_df.empty:
        support_col2.info("No mapped planning rows are available at the latest date.")
    else:
        support_col2.plotly_chart(
            px.scatter_geo(
                map_df,
                lat="lat",
                lon="lon",
                color="renewable_share_pct",
                size="daily_demand_mwh",
                hover_name="respondent",
                hover_data={"carbon_intensity_kg_per_mwh": ':.1f', "location_label": True},
                scope="usa",
                title="Approximate clean share map",
            ),
            use_container_width=True,
        )
        support_col2.caption("Approximate respondent coordinates only; this map is supporting context.")

    raw_download_df = focus_df[
        [
            "date",
            "respondent",
            "respondent_name",
            "daily_demand_mwh",
            "renewable_share_pct",
            "carbon_intensity_kg_per_mwh",
            "fuel_diversity_index",
            "peak_hour_gas_share_pct",
            "avg_abs_forecast_error_pct",
        ]
    ].copy()
    st.download_button(
        label="Download focus respondent planning data as CSV",
        data=raw_download_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{focus_respondent.lower()}_resource_planning.csv",
        mime="text/csv",
    )
