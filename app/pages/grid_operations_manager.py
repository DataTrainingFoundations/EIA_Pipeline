"""Grid Operations Manager — Streamlit page.

Reads platinum.grid_operations_hourly and platinum.grid_operations_alert_hourly
from Postgres via the project's data_access helpers and renders a triage-first
dashboard for grid operations staff.

Layout
------
1. Sidebar   — filters (date range, respondents, timezone)
2. KPI strip — latest period, respondents in scope/alert, high-severity count,
               lowest coverage ratio
3. Watchlist — priority-ranked table (Critical → Elevated → Stable)
4. Where?    — top forecast misses + lowest coverage ratio bar charts
5. Focus     — per-respondent demand/forecast, ramp stress, generation gap trends
6. Alerts    — active alert table + daily alert-volume bar chart
7. Expander  — fuel mix area chart, approximate US map, CSV download
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data_access import (
    get_grid_operations_coverage,
    list_respondents,
    load_grid_operations_alerts,
    load_grid_operations_hourly,
    load_grid_watchlist,
    load_latest_grid_alerts,
    load_latest_grid_operations_snapshot,
    table_has_rows,
)
from respondent_geo import RESPONDENT_GEO
from ui_utils import (
    build_default_date_range,
    coerce_numeric,
    convert_timestamp_series,
    format_timestamp,
    get_timezone_options,
    rank_priority_labels,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUMERIC_COLUMNS = [
    "actual_demand_mwh",
    "day_ahead_forecast_mwh",
    "forecast_error_mwh",
    "forecast_error_pct",
    "forecast_error_zscore",
    "demand_ramp_mwh",
    "demand_ramp_zscore",
    "demand_zscore",
    "total_generation_mwh",
    "renewable_generation_mwh",
    "fossil_generation_mwh",
    "gas_generation_mwh",
    "renewable_share_pct",
    "fossil_share_pct",
    "gas_share_pct",
    "generation_gap_mwh",
    "coverage_ratio",
    "metric_value",
    "threshold_value",
]

PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]

PRIORITY_COLORS = {
    "Critical": "#E24B4A",
    "Elevated": "#EF9F27",
    "Stable": "#1D9E75",
}

SEVERITY_RANK = {"high": 2, "medium": 1, "low": 0}

WATCHLIST_DISPLAY_COLUMNS = {
    "respondent": "Respondent",
    "respondent_name": "Name",
    "priority_label": "Priority",
    "actual_demand_mwh": "Actual demand (MWh)",
    "forecast_error_mwh": "Forecast error (MWh)",
    "forecast_error_zscore": "Error z-score",
    "demand_ramp_zscore": "Ramp z-score",
    "coverage_ratio": "Coverage ratio",
    "active_alert_count": "Active alerts",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _derive_priority(row: pd.Series) -> str:
    coverage = pd.to_numeric(row.get("coverage_ratio"), errors="coerce")
    ferr_z = abs(pd.to_numeric(row.get("forecast_error_zscore"), errors="coerce"))
    ramp_z = abs(pd.to_numeric(row.get("demand_ramp_zscore"), errors="coerce"))

    if bool(row.get("has_high_alert")) or (pd.notna(coverage) and coverage < 0.8):
        return "Critical"
    if (
        bool(row.get("has_medium_alert"))
        or (pd.notna(ferr_z) and ferr_z >= 2.5)
        or (pd.notna(ramp_z) and ramp_z >= 2.5)
    ):
        return "Elevated"
    return "Stable"


def _priority_badge(label: str) -> str:
    color = PRIORITY_COLORS.get(label, "#888")
    return f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600">{label}</span>'


def _color_priority(val: str) -> str:
    colors = {"Critical": "#E24B4A", "Elevated": "#EF9F27", "Stable": "#1D9E75"}
    return f"background-color: {colors.get(val, '')}"


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Grid Operations Manager",
    page_icon="⚡",
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

# ---------------------------------------------------------------------------
# Guard: ensure data exists
# ---------------------------------------------------------------------------

if not table_has_rows("platinum.grid_operations_hourly"):
    st.warning(
        "⚠️ No rows found in `platinum.grid_operations_hourly`. "
        "Let the `platinum_grid_operations_hourly` DAG complete at least one run first."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Coverage + respondent list (used to build sidebar widgets)
# ---------------------------------------------------------------------------

coverage = get_grid_operations_coverage()
max_period = pd.to_datetime(coverage["max_period"], utc=True)
min_period = pd.to_datetime(coverage["min_period"], utc=True)
default_start, default_end = build_default_date_range(min_period, max_period, lookback_days=7)
all_respondents = list_respondents("platinum.grid_operations_hourly")

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/8/8e/Electricity_symbol.svg/120px-Electricity_symbol.svg.png", width=40)
    st.title("Grid Ops Manager")
    st.caption("Filter the dashboard below.")
    st.divider()

    selected_range = st.date_input(
        "Date range (UTC)",
        value=(default_start, default_end),
        min_value=min_period.date(),
        max_value=max_period.date(),
    )

    selected_respondents = st.multiselect(
        "Respondents",
        options=all_respondents,
        default=all_respondents,
        help="Leave blank to include all respondents.",
    )

    display_tz = st.selectbox("Display timezone", get_timezone_options(), index=0)

    st.divider()
    with st.expander("How to read this page"):
        st.markdown(
            """
- **KPI strip** — immediate system health at a glance.
- **Watchlist** — ranked list; *Critical* rows need action now.
- **Where?** — bar charts showing which respondents are most stressed.
- **Focus** — drill into a single respondent's trends.
- **Alerts** — active alert table and historical volume.
- **Supporting analysis** — fuel mix, map, raw data export.
"""
        )

if len(selected_range) != 2:
    st.info("Select a start and end date to load data.")
    st.stop()

start_date, end_date = selected_range
start_ts = f"{start_date}T00:00:00+00:00"
end_ts = f"{end_date}T23:59:59+00:00"
filtered_respondents = selected_respondents or None

# ---------------------------------------------------------------------------
# Data load
# ---------------------------------------------------------------------------

with st.spinner("Loading grid operations data…"):
    ops_df = load_grid_operations_hourly(start_ts, end_ts, filtered_respondents)
    watchlist_raw = load_grid_watchlist(start_ts, end_ts, filtered_respondents)
    snapshot_df = load_latest_grid_operations_snapshot(start_ts, end_ts, filtered_respondents)
    latest_alerts_df = load_latest_grid_alerts(start_ts, end_ts, filtered_respondents)
    alerts_history_df = load_grid_operations_alerts(start_ts, end_ts, filtered_respondents)

if ops_df.empty or watchlist_raw.empty:
    st.warning("No grid operations data found for the selected filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Coerce & enrich
# ---------------------------------------------------------------------------

ops_df["period"] = pd.to_datetime(ops_df["period"], utc=True)
ops_df["period_display"] = convert_timestamp_series(ops_df["period"], display_tz)
ops_df = coerce_numeric(ops_df, NUMERIC_COLUMNS)

watchlist_df = watchlist_raw.copy()
watchlist_df["period"] = pd.to_datetime(watchlist_df["period"], utc=True)
watchlist_df = coerce_numeric(
    watchlist_df,
    ["actual_demand_mwh", "forecast_error_mwh", "forecast_error_zscore",
     "demand_ramp_zscore", "coverage_ratio", "active_alert_count"],
)
watchlist_df["priority_label"] = watchlist_df.apply(_derive_priority, axis=1)
watchlist_df = rank_priority_labels(watchlist_df, "priority_label", PRIORITY_ORDER)
watchlist_df = watchlist_df.sort_values(
    ["priority_label", "has_high_alert", "active_alert_count", "coverage_ratio"],
    ascending=[True, False, False, True],
    kind="stable",
)

snapshot_df["period"] = pd.to_datetime(snapshot_df["period"], utc=True)
snapshot_df["period_display"] = convert_timestamp_series(snapshot_df["period"], display_tz)
snapshot_df = coerce_numeric(snapshot_df, NUMERIC_COLUMNS)

if not latest_alerts_df.empty:
    latest_alerts_df["period"] = pd.to_datetime(latest_alerts_df["period"], utc=True)
    latest_alerts_df["period_display"] = convert_timestamp_series(latest_alerts_df["period"], display_tz)
    latest_alerts_df = coerce_numeric(latest_alerts_df, ["metric_value", "threshold_value"])

if not alerts_history_df.empty:
    alerts_history_df["period"] = pd.to_datetime(alerts_history_df["period"], utc=True)
    alerts_history_df["period_display"] = convert_timestamp_series(alerts_history_df["period"], display_tz)
    alerts_history_df["alert_day"] = alerts_history_df["period_display"].dt.date

# ---------------------------------------------------------------------------
# KPI derivations
# ---------------------------------------------------------------------------

latest_period = snapshot_df["period"].max()
latest_period_str = format_timestamp(latest_period, display_tz)
n_respondents = watchlist_df["respondent"].nunique()
n_in_alert = int((watchlist_df["active_alert_count"] > 0).sum())
n_critical = int((watchlist_df["priority_label"] == "Critical").sum())
n_high_severity = (
    int((latest_alerts_df["severity"].str.lower() == "high").sum())
    if not latest_alerts_df.empty else 0
)
lowest_coverage = snapshot_df["coverage_ratio"].dropna().min()
lowest_coverage_str = f"{lowest_coverage:.2f}" if pd.notna(lowest_coverage) else "n/a"

# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------

st.title("⚡ Grid Operations Manager")
st.caption(
    f"Showing **{start_date}** → **{end_date}** · "
    f"**{n_respondents}** respondents · "
    f"Latest loaded period: **{latest_period_str}**"
)

# ---------------------------------------------------------------------------
# Section 1 — KPI strip
# ---------------------------------------------------------------------------

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Respondents in scope", f"{n_respondents:,}")
k2.metric("In alert", f"{n_in_alert:,}", delta=None, help="Respondents with ≥1 active alert in the latest snapshot")
k3.metric(
    "Critical priority",
    f"{n_critical:,}",
    delta=None,
    help="Critical = high alert active OR coverage ratio < 0.80",
)
k4.metric("High-severity alerts", f"{n_high_severity:,}")
k5.metric("Lowest coverage ratio", lowest_coverage_str, help="Minimum coverage_ratio across all respondents in the latest snapshot")

st.divider()

# ---------------------------------------------------------------------------
# Section 2 — Watchlist
# ---------------------------------------------------------------------------

st.subheader("Watchlist — what needs attention now?")
st.caption("Ranked Critical → Elevated → Stable. Critical rows require immediate triage.")

display_watchlist = (
    watchlist_df[list(WATCHLIST_DISPLAY_COLUMNS.keys())]
    .rename(columns=WATCHLIST_DISPLAY_COLUMNS)
    .reset_index(drop=True)
)

st.dataframe(
    display_watchlist.style.applymap(_color_priority, subset=["Priority"]),
    use_container_width=True,
    hide_index=True,
    height=min(40 + 35 * len(display_watchlist), 400),
)

st.divider()

# ---------------------------------------------------------------------------
# Section 3 — Where is stress concentrated?
# ---------------------------------------------------------------------------

st.subheader("Where is intervention needed?")
st.caption("Latest-snapshot rankings. Use these to prioritise which respondent to focus on.")

col_left, col_right = st.columns(2)

# Forecast miss bar
forecast_rank = (
    snapshot_df.dropna(subset=["forecast_error_mwh"])
    .assign(abs_error=lambda d: d["forecast_error_mwh"].abs())
    .sort_values("abs_error", ascending=False)
    .head(10)
    .sort_values("forecast_error_mwh")
)
if forecast_rank.empty:
    col_left.info("No forecast-backed rows in the latest snapshot.")
else:
    fig_fe = px.bar(
        forecast_rank,
        x="forecast_error_mwh",
        y="respondent",
        orientation="h",
        color="forecast_error_mwh",
        color_continuous_scale=["#1D9E75", "#f7f7f7", "#E24B4A"],
        color_continuous_midpoint=0,
        labels={"forecast_error_mwh": "Forecast error (MWh)", "respondent": ""},
        title="Top current forecast misses",
    )
    fig_fe.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=40, b=0))
    col_left.plotly_chart(fig_fe, use_container_width=True)

# Coverage ratio bar
coverage_rank = (
    snapshot_df.dropna(subset=["coverage_ratio"])
    .sort_values("coverage_ratio")
    .head(10)
)
if coverage_rank.empty:
    col_right.info("No coverage rows in the latest snapshot.")
else:
    fig_cov = px.bar(
        coverage_rank.sort_values("coverage_ratio", ascending=False),
        x="coverage_ratio",
        y="respondent",
        orientation="h",
        color="coverage_ratio",
        color_continuous_scale=["#E24B4A", "#EF9F27", "#1D9E75"],
        range_color=[0, 1],
        labels={"coverage_ratio": "Coverage ratio", "respondent": ""},
        title="Lowest current generation coverage",
    )
    fig_cov.add_vline(x=0.8, line_dash="dash", line_color="#E24B4A", annotation_text="0.8 threshold")
    fig_cov.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=40, b=0))
    col_right.plotly_chart(fig_cov, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Section 4 — Focus trends
# ---------------------------------------------------------------------------

focus_options = watchlist_df["respondent"].dropna().tolist()
focus_respondent = st.selectbox(
    "Focus respondent",
    options=focus_options,
    index=0,
    help="Select a respondent to drill into its hourly trends.",
)
focus_df = ops_df[ops_df["respondent"] == focus_respondent].copy().sort_values("period")

st.subheader(f"Trends — {focus_respondent}")
st.caption("Hourly trends for the selected respondent over the current date range.")

trend_l, trend_r = st.columns(2)

# Demand vs forecast
demand_melt = (
    focus_df[["period_display", "actual_demand_mwh", "day_ahead_forecast_mwh"]]
    .melt(id_vars="period_display", var_name="series", value_name="mwh")
    .dropna(subset=["mwh"])
    .replace({"actual_demand_mwh": "Actual demand", "day_ahead_forecast_mwh": "Day-ahead forecast"})
)
if demand_melt.empty:
    trend_l.info("No overlapping actual and forecast series for this respondent.")
else:
    fig_dem = px.line(
        demand_melt,
        x="period_display",
        y="mwh",
        color="series",
        color_discrete_map={"Actual demand": "#185FA5", "Day-ahead forecast": "#EF9F27"},
        labels={"period_display": f"Period ({display_tz})", "mwh": "MWh", "series": ""},
        title="Actual demand vs day-ahead forecast",
    )
    fig_dem.update_layout(legend=dict(orientation="h", y=1.1), margin=dict(l=0, r=0, t=50, b=0))
    trend_l.plotly_chart(fig_dem, use_container_width=True)

# Ramp stress
ramp_df = focus_df.dropna(subset=["demand_ramp_mwh", "demand_ramp_zscore"], how="all")
if ramp_df.empty:
    trend_r.info("No ramp stress data for this respondent.")
else:
    fig_ramp = go.Figure()
    fig_ramp.add_trace(go.Bar(
        x=ramp_df["period_display"],
        y=ramp_df["demand_ramp_mwh"],
        name="Ramp (MWh)",
        marker_color="#185FA5",
        opacity=0.6,
        yaxis="y",
    ))
    fig_ramp.add_trace(go.Scatter(
        x=ramp_df["period_display"],
        y=ramp_df["demand_ramp_zscore"],
        name="Ramp z-score",
        line=dict(color="#E24B4A", width=2),
        yaxis="y2",
    ))
    fig_ramp.add_hline(y=2.5, line_dash="dot", line_color="#E24B4A", annotation_text="+2.5σ", yref="y2")
    fig_ramp.add_hline(y=-2.5, line_dash="dot", line_color="#E24B4A", annotation_text="-2.5σ", yref="y2")
    fig_ramp.update_layout(
        title="Demand ramp stress",
        yaxis=dict(title="Ramp (MWh)"),
        yaxis2=dict(title="Z-score", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=0, r=0, t=50, b=0),
    )
    trend_r.plotly_chart(fig_ramp, use_container_width=True)

# Generation coverage trend (full width)
cov_trend = focus_df.dropna(subset=["generation_gap_mwh", "coverage_ratio"], how="all")
if not cov_trend.empty:
    fig_cov_trend = go.Figure()
    fig_cov_trend.add_trace(go.Bar(
        x=cov_trend["period_display"],
        y=cov_trend["generation_gap_mwh"],
        name="Generation gap (MWh)",
        marker_color="#EF9F27",
        opacity=0.5,
        yaxis="y",
    ))
    fig_cov_trend.add_trace(go.Scatter(
        x=cov_trend["period_display"],
        y=cov_trend["coverage_ratio"],
        name="Coverage ratio",
        line=dict(color="#1D9E75", width=2),
        yaxis="y2",
    ))
    fig_cov_trend.add_hline(y=0.8, line_dash="dash", line_color="#E24B4A", annotation_text="0.8 threshold", yref="y2")
    fig_cov_trend.update_layout(
        title="Generation gap and coverage ratio",
        yaxis=dict(title="Generation gap (MWh)"),
        yaxis2=dict(title="Coverage ratio", overlaying="y", side="right", showgrid=False, range=[0, 1.2]),
        legend=dict(orientation="h", y=1.05),
        margin=dict(l=0, r=0, t=50, b=0),
    )
    st.plotly_chart(fig_cov_trend, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Section 5 — Alerts
# ---------------------------------------------------------------------------

st.subheader("Active alerts — what evidence supports action?")
st.caption("Latest-snapshot alerts. Filter by severity or type to triage.")

alert_f1, alert_f2 = st.columns(2)
severity_opts = ["All"] + (
    sorted(latest_alerts_df["severity"].dropna().str.title().unique().tolist())
    if not latest_alerts_df.empty else []
)
type_opts = ["All"] + (
    sorted(latest_alerts_df["alert_type"].dropna().unique().tolist())
    if not latest_alerts_df.empty else []
)
sel_severity = alert_f1.selectbox("Severity filter", severity_opts)
sel_type = alert_f2.selectbox("Alert type filter", type_opts)

filtered_alerts = latest_alerts_df.copy()
if not filtered_alerts.empty:
    if sel_severity != "All":
        filtered_alerts = filtered_alerts[filtered_alerts["severity"].str.lower() == sel_severity.lower()]
    if sel_type != "All":
        filtered_alerts = filtered_alerts[filtered_alerts["alert_type"] == sel_type]

if filtered_alerts.empty:
    st.info("No active alerts match the selected filters.")
else:
    alert_display_cols = ["period_display", "respondent", "respondent_name",
                          "alert_type", "severity", "metric_value", "threshold_value", "message"]
    alert_display_cols = [c for c in alert_display_cols if c in filtered_alerts.columns]

    def _color_severity(val: str) -> str:
        colors = {"high": "#fde8e8", "medium": "#fef3e2", "low": "#e6f5ef"}
        return f"background-color: {colors.get(str(val).lower(), '')}"

    st.dataframe(
        filtered_alerts[alert_display_cols]
        .rename(columns={"period_display": f"Period ({display_tz})"})
        .style.applymap(_color_severity, subset=["severity"]),
        use_container_width=True,
        hide_index=True,
    )

# Alert volume history
if not alerts_history_df.empty:
    alert_vol = (
        alerts_history_df.groupby(["alert_day", "severity"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    fig_vol = px.bar(
        alert_vol,
        x="alert_day",
        y="count",
        color="severity",
        color_discrete_map={"high": "#E24B4A", "medium": "#EF9F27", "low": "#1D9E75"},
        labels={"alert_day": f"Day ({display_tz})", "count": "Alert count", "severity": "Severity"},
        title="Daily alert volume — are counts stabilizing or worsening?",
    )
    fig_vol.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_vol, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Section 6 — Supporting analysis (expander)
# ---------------------------------------------------------------------------

with st.expander("Supporting analysis", expanded=False):
    sup_l, sup_r = st.columns(2)

    # Fuel mix
    fuel_cols = ["period_display", "renewable_share_pct", "fossil_share_pct", "gas_share_pct"]
    fuel_melt = (
        focus_df[fuel_cols]
        .melt(id_vars="period_display", var_name="metric", value_name="value")
        .dropna(subset=["value"])
        .replace({
            "renewable_share_pct": "Renewable %",
            "fossil_share_pct": "Fossil %",
            "gas_share_pct": "Gas %",
        })
    )
    if fuel_melt.empty:
        sup_l.info("No fuel mix data for this respondent.")
    else:
        fig_fuel = px.area(
            fuel_melt,
            x="period_display",
            y="value",
            color="metric",
            color_discrete_map={"Renewable %": "#1D9E75", "Fossil %": "#E24B4A", "Gas %": "#EF9F27"},
            labels={"period_display": f"Period ({display_tz})", "value": "Share (%)", "metric": ""},
            title=f"Fuel mix — {focus_respondent}",
        )
        fig_fuel.update_layout(legend=dict(orientation="h", y=1.1), margin=dict(l=0, r=0, t=50, b=0))
        sup_l.plotly_chart(fig_fuel, use_container_width=True)

    # Map
    map_df = snapshot_df.copy()
    map_df["lat"] = map_df["respondent"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lat"))
    map_df["lon"] = map_df["respondent"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lon"))
    map_df["location_label"] = map_df["respondent"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("label", c))
    map_df = map_df.dropna(subset=["lat", "lon", "actual_demand_mwh"])

    if map_df.empty:
        sup_r.info("No mapped operational rows for the latest snapshot.")
    else:
        fig_map = px.scatter_geo(
            map_df,
            lat="lat",
            lon="lon",
            color="coverage_ratio",
            size="actual_demand_mwh",
            hover_name="respondent",
            hover_data={
                "respondent_name": True,
                "coverage_ratio": ":.2f",
                "actual_demand_mwh": ":,.0f",
                "location_label": True,
                "lat": False,
                "lon": False,
            },
            color_continuous_scale=["#E24B4A", "#EF9F27", "#1D9E75"],
            range_color=[0, 1],
            scope="usa",
            title="Approximate coverage ratio by location",
        )
        fig_map.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        sup_r.plotly_chart(fig_map, use_container_width=True)
        sup_r.caption("⚠️ Approximate respondent coordinates only — not primary operating evidence.")

    # CSV download
    st.divider()
    raw_dl = focus_df[[
        "period_display", "respondent", "respondent_name",
        "actual_demand_mwh", "day_ahead_forecast_mwh", "forecast_error_mwh",
        "forecast_error_zscore", "demand_ramp_mwh", "demand_ramp_zscore",
        "total_generation_mwh", "renewable_share_pct", "fossil_share_pct",
        "gas_share_pct", "generation_gap_mwh", "coverage_ratio",
    ]].copy()
    st.download_button(
        label=f"⬇️ Download {focus_respondent} hourly data as CSV",
        data=raw_dl.to_csv(index=False).encode("utf-8"),
        file_name=f"{focus_respondent.lower()}_grid_ops_{start_date}_{end_date}.csv",
        mime="text/csv",
    )
