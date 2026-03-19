"""Resource Planning Lead dashboard.

This page reads the planning serving table from Postgres through `data_access`,
derives a ranked planning watchlist in pandas, and renders triage-first views
for longer-horizon planning decisions.
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
from planning_page_logic import (
    build_planning_thresholds,
    build_priority_history,
    derive_planning_driver,
    derive_planning_priority,
)
from respondent_geo import RESPONDENT_GEO
from ui_utils import build_default_date_range, coerce_numeric, rank_priority_labels

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

PRIORITY_COLORS = {
    "Critical": "#E24B4A",
    "Elevated": "#EF9F27",
    "Stable": "#1D9E75",
}

WATCHLIST_DISPLAY_COLUMNS = {
    "respondent": "Respondent",
    "respondent_name": "Name",
    "planning_priority": "Priority",
    "primary_driver": "Primary driver",
    "carbon_intensity_kg_per_mwh": "Carbon intensity",
    "renewable_share_pct": "Renewable share",
    "peak_hour_gas_share_pct": "Peak-hour gas share",
    "fuel_diversity_index": "Fuel diversity",
    "avg_abs_forecast_error_pct": "Forecast error",
    "clean_coverage_ratio": "Clean coverage ratio",
}


def _color_priority(value: str) -> str:
    color = PRIORITY_COLORS.get(value, "")
    if not color:
        return ""
    return f"background-color: {color}; color: #ffffff"


def _format_value(value: float | int | str | None, suffix: str = "", decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    if isinstance(value, str):
        return value
    return f"{value:.{decimals}f}{suffix}"


def _describe_trend(values: pd.Series, lower_is_better: bool) -> str:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if len(numeric) < 2:
        return "Not enough history in the selected window."

    start_value = float(numeric.iloc[0])
    end_value = float(numeric.iloc[-1])
    tolerance = max(float(numeric.std(ddof=0) or 0.0) * 0.25, abs(start_value) * 0.03, 0.5)
    delta = end_value - start_value

    if abs(delta) <= tolerance:
        return "Flat across the selected window."

    improving = delta < 0 if lower_is_better else delta > 0
    return "Improving across the selected window." if improving else "Worsening across the selected window."


def _plot_rank_chart(
    container,
    df: pd.DataFrame,
    metric_column: str,
    title: str,
    x_label: str,
    threshold_value: float | None,
    empty_message: str,
) -> None:
    if df.empty:
        container.info(empty_message)
        return

    fig = px.bar(
        df,
        x=metric_column,
        y="respondent",
        orientation="h",
        color_discrete_sequence=["#185FA5"],
        labels={metric_column: x_label, "respondent": ""},
        title=title,
    )
    if threshold_value is not None:
        fig.add_vline(x=threshold_value, line_dash="dash", line_color="#E24B4A")
    fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=45, b=0))
    container.plotly_chart(fig, use_container_width=True)


st.set_page_config(
    page_title="Resource Planning Lead",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem; color: #666; }
    div[data-testid="stExpander"] summary { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

if not table_has_rows("platinum.resource_planning_daily"):
    st.warning(
        "No planning mart rows found yet. Let the `platinum_resource_planning_daily` DAG run first."
    )
    st.stop()

coverage = get_planning_coverage()
max_date = pd.to_datetime(coverage["max_date"])
min_date = pd.to_datetime(coverage["min_date"])
default_start_date, default_end_date = build_default_date_range(min_date, max_date, lookback_days=30)
respondents = list_respondents("platinum.resource_planning_daily")

with st.sidebar:
    st.title("Resource Planning Lead")
    st.caption("Filter the dashboard below.")
    st.divider()

    selected_range = st.date_input(
        "Date range",
        value=(default_start_date, default_end_date),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )
    selected_respondents = st.multiselect(
        "Respondents",
        respondents,
        default=respondents,
        help="Leave blank to include all respondents.",
    )

    st.divider()
    with st.expander("How to read this page"):
        st.markdown(
            """
- KPI strip - current planning pressure at a glance.
- Watchlist - ranked list; Critical rows need structural attention now.
- Where? - latest rankings showing which respondents stand out most.
- Focus - drill into one respondent's planning trend.
- Evidence - latest supporting table plus the historical priority mix.
- Supporting analysis - secondary context and raw export.
"""
        )

if len(selected_range) != 2:
    st.info("Select a start and end date to load data.")
    st.stop()

start_date, end_date = selected_range
filtered_respondents = selected_respondents or None

with st.spinner("Loading resource planning data..."):
    planning_df = load_resource_planning_daily(str(start_date), str(end_date), filtered_respondents)
    latest_snapshot_df = load_latest_planning_snapshot(str(start_date), str(end_date), filtered_respondents)
    watchlist_raw = load_planning_watchlist(str(start_date), str(end_date), filtered_respondents)

if planning_df.empty or latest_snapshot_df.empty or watchlist_raw.empty:
    st.warning("No planning rows found for the selected filters.")
    st.stop()

planning_df["date"] = pd.to_datetime(planning_df["date"])
planning_df = coerce_numeric(planning_df, NUMERIC_COLUMNS)
planning_df["day_type"] = planning_df["weekend_flag"].map(lambda value: "Weekend" if value else "Weekday")

latest_snapshot_df["date"] = pd.to_datetime(latest_snapshot_df["date"])
latest_snapshot_df = coerce_numeric(latest_snapshot_df, NUMERIC_COLUMNS)

watchlist_df = watchlist_raw.copy()
watchlist_df["date"] = pd.to_datetime(watchlist_df["date"])
watchlist_df = coerce_numeric(watchlist_df, NUMERIC_COLUMNS)

thresholds = build_planning_thresholds(latest_snapshot_df)

for df in (watchlist_df, latest_snapshot_df):
    df["planning_priority"] = df.apply(derive_planning_priority, axis=1, thresholds=thresholds)
    df["primary_driver"] = df.apply(derive_planning_driver, axis=1, thresholds=thresholds)

watchlist_df = rank_priority_labels(watchlist_df, "planning_priority", PRIORITY_ORDER)
watchlist_df = watchlist_df.sort_values(
    [
        "planning_priority",
        "carbon_intensity_kg_per_mwh",
        "peak_hour_gas_share_pct",
        "renewable_share_pct",
        "respondent",
    ],
    ascending=[True, False, False, True, True],
    kind="stable",
).reset_index(drop=True)

priority_history_df = build_priority_history(planning_df, thresholds)

focus_options = watchlist_df["respondent"].dropna().tolist()
focus_respondent = st.selectbox(
    "Focus respondent",
    options=focus_options,
    index=0,
    help="Select a respondent to inspect its planning trends.",
)
focus_df = planning_df[planning_df["respondent"] == focus_respondent].copy().sort_values("date")
focus_snapshot_df = watchlist_df[watchlist_df["respondent"] == focus_respondent].head(1)

latest_date = latest_snapshot_df["date"].max()
n_respondents = watchlist_df["respondent"].nunique()
n_critical = int((watchlist_df["planning_priority"] == "Critical").sum())
n_elevated = int((watchlist_df["planning_priority"] == "Elevated").sum())
highest_carbon = watchlist_df["carbon_intensity_kg_per_mwh"].dropna().max()
lowest_renewable = watchlist_df["renewable_share_pct"].dropna().min()

st.title("Resource Planning Lead")
st.caption(
    f"Showing **{start_date}** -> **{end_date}** | "
    f"**{n_respondents}** respondents | "
    f"Latest loaded date: **{latest_date.date()}**"
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Respondents in scope", f"{n_respondents:,}")
k2.metric("Critical priority", f"{n_critical:,}")
k3.metric("Elevated priority", f"{n_elevated:,}")
k4.metric("Highest carbon intensity", _format_value(highest_carbon))
k5.metric("Lowest renewable share", _format_value(lowest_renewable, suffix="%"))

st.divider()

st.subheader("Watchlist - what needs structural attention now?")
st.caption("Ranked Critical -> Elevated -> Stable. Primary driver explains why each row sits on the watchlist.")

display_watchlist = watchlist_df[list(WATCHLIST_DISPLAY_COLUMNS.keys())].rename(columns=WATCHLIST_DISPLAY_COLUMNS)
st.dataframe(
    display_watchlist.style.applymap(_color_priority, subset=["Priority"]),
    use_container_width=True,
    hide_index=True,
    height=min(40 + 35 * len(display_watchlist), 400),
)

st.divider()

st.subheader("Where is structural attention needed?")
st.caption("Latest-snapshot rankings. Use these to decide which respondent should get planning attention first.")

rank_col1, rank_col2 = st.columns(2)
rank_col3, rank_col4 = st.columns(2)

carbon_rank_df = (
    latest_snapshot_df.dropna(subset=["carbon_intensity_kg_per_mwh"])
    .sort_values("carbon_intensity_kg_per_mwh", ascending=False)
    .head(10)
    .sort_values("carbon_intensity_kg_per_mwh")
)
_plot_rank_chart(
    rank_col1,
    carbon_rank_df,
    "carbon_intensity_kg_per_mwh",
    "Highest carbon intensity",
    "kg CO2e per MWh",
    thresholds["carbon_p90"],
    "No carbon intensity rows are available at the latest date.",
)

renewable_rank_df = (
    latest_snapshot_df.dropna(subset=["renewable_share_pct"])
    .sort_values("renewable_share_pct", ascending=True)
    .head(10)
    .sort_values("renewable_share_pct", ascending=False)
)
_plot_rank_chart(
    rank_col2,
    renewable_rank_df,
    "renewable_share_pct",
    "Lowest renewable share",
    "Renewable share (%)",
    thresholds["renewable_p10"],
    "No renewable share rows are available at the latest date.",
)

gas_rank_df = (
    latest_snapshot_df.dropna(subset=["peak_hour_gas_share_pct"])
    .sort_values("peak_hour_gas_share_pct", ascending=False)
    .head(10)
    .sort_values("peak_hour_gas_share_pct")
)
_plot_rank_chart(
    rank_col3,
    gas_rank_df,
    "peak_hour_gas_share_pct",
    "Highest peak-hour gas dependence",
    "Gas share at peak (%)",
    thresholds["gas_p90"],
    "No peak-hour gas rows are available at the latest date.",
)

diversity_rank_df = (
    latest_snapshot_df.dropna(subset=["fuel_diversity_index"])
    .sort_values("fuel_diversity_index", ascending=True)
    .head(10)
    .sort_values("fuel_diversity_index", ascending=False)
)
_plot_rank_chart(
    rank_col4,
    diversity_rank_df,
    "fuel_diversity_index",
    "Lowest fuel diversity",
    "Fuel diversity index",
    thresholds["diversity_p25"],
    "No fuel diversity rows are available at the latest date.",
)

st.divider()

st.subheader(f"Focus - {focus_respondent}")
focus_driver = (
    focus_snapshot_df["primary_driver"].iloc[0]
    if not focus_snapshot_df.empty
    else "Within expected range"
)
st.caption(
    f"Current primary driver: **{focus_driver}**. "
    "The charts below show whether this respondent is improving, flat, or worsening over the selected window."
)

focus_metric1, focus_metric2, focus_metric3, focus_metric4 = st.columns(4)
if focus_snapshot_df.empty:
    focus_metric1.metric("Priority", "n/a")
    focus_metric2.metric("Carbon intensity", "n/a")
    focus_metric3.metric("Renewable share", "n/a")
    focus_metric4.metric("Clean coverage ratio", "n/a")
else:
    focus_row = focus_snapshot_df.iloc[0]
    focus_metric1.metric("Priority", str(focus_row["planning_priority"]))
    focus_metric2.metric("Carbon intensity", _format_value(focus_row["carbon_intensity_kg_per_mwh"]))
    focus_metric3.metric("Renewable share", _format_value(focus_row["renewable_share_pct"], suffix="%"))
    focus_metric4.metric("Clean coverage ratio", _format_value(focus_row["clean_coverage_ratio"], decimals=2))

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
            title="Carbon intensity trend",
        ),
        use_container_width=True,
    )
    trend_col1.caption(_describe_trend(carbon_df["carbon_intensity_kg_per_mwh"], lower_is_better=True))

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
            title="Renewable share trend",
        ),
        use_container_width=True,
    )
    trend_col2.caption(_describe_trend(renewable_df["renewable_share_pct"], lower_is_better=False))

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
            title="Peak-hour gas dependence trend",
        ),
        use_container_width=True,
    )
    trend_col3.caption(_describe_trend(gas_df["peak_hour_gas_share_pct"], lower_is_better=True))

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
            title="Forecast error trend",
        ),
        use_container_width=True,
    )
    trend_col4.caption(_describe_trend(forecast_df["avg_abs_forecast_error_pct"], lower_is_better=True))

st.divider()

st.subheader("Planning evidence - what supports action?")
st.caption("Filter the latest planning snapshot by priority and driver, then use the daily mix chart to see whether pressure is persistent.")

evidence_filter1, evidence_filter2 = st.columns(2)
priority_options = ["All"] + PRIORITY_ORDER
driver_options = ["All"] + sorted(watchlist_df["primary_driver"].dropna().unique().tolist())
selected_priority = evidence_filter1.selectbox("Priority filter", priority_options)
selected_driver = evidence_filter2.selectbox("Primary driver filter", driver_options)

evidence_df = watchlist_df.copy()
if selected_priority != "All":
    evidence_df = evidence_df[evidence_df["planning_priority"] == selected_priority]
if selected_driver != "All":
    evidence_df = evidence_df[evidence_df["primary_driver"] == selected_driver]

display_evidence = evidence_df[list(WATCHLIST_DISPLAY_COLUMNS.keys())].rename(columns=WATCHLIST_DISPLAY_COLUMNS)
st.dataframe(
    display_evidence.style.applymap(_color_priority, subset=["Priority"]),
    use_container_width=True,
    hide_index=True,
    height=min(40 + 35 * max(len(display_evidence), 1), 400),
)

if priority_history_df.empty:
    st.info("No priority history is available for the selected window.")
else:
    fig_priority_mix = px.bar(
        priority_history_df,
        x="date",
        y="respondent_count",
        color="planning_priority",
        barmode="stack",
        color_discrete_map=PRIORITY_COLORS,
        category_orders={"planning_priority": PRIORITY_ORDER},
        labels={
            "date": "Date",
            "respondent_count": "Respondent count",
            "planning_priority": "Priority",
        },
        title="Daily respondent counts by planning priority",
    )
    fig_priority_mix.update_layout(margin=dict(l=0, r=0, t=50, b=0))
    st.plotly_chart(fig_priority_mix, use_container_width=True)

st.divider()

with st.expander("Supporting analysis", expanded=False):
    st.caption("Use these views as secondary context after the watchlist, rankings, and evidence sections are clear.")

    support_col1, support_col2 = st.columns(2)

    drift_df = (
        planning_df.dropna(subset=["daily_demand_mwh"])
        .groupby(["respondent", "day_type"], as_index=False)["daily_demand_mwh"]
        .mean()
    )
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
        support_col1.caption("Planning demand drift can help explain whether the current watchlist pressure is structural or calendar-driven.")

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
                hover_data={"carbon_intensity_kg_per_mwh": ":.1f", "location_label": True},
                scope="usa",
                title="Approximate clean share map",
            ),
            use_container_width=True,
        )
        support_col2.caption("Approximate respondent coordinates only. Use this map as orientation, not as precise geospatial evidence.")

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
            "clean_coverage_ratio",
        ]
    ].copy()
    st.download_button(
        label="Download focus respondent planning data as CSV",
        data=raw_download_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{focus_respondent.lower()}_resource_planning.csv",
        mime="text/csv",
    )
