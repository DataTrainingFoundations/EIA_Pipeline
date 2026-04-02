"""Demand and forecast monitoring dashboard."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data_access import (
    get_demand_coverage,
    list_ba_codes,
    load_daily_demand_peak,
    load_demand_hourly,
    load_latest_demand_snapshot,
    table_has_rows,
)
from respondent_geo import RESPONDENT_GEO
from ui_utils import (
    build_default_date_range,
    coerce_numeric,
    convert_timestamp_series,
    format_timestamp,
    get_timezone_options,
    safe_quantile,
)

NUMERIC_COLS = [
    "demand_gwh",
    "forecast_gwh",
    "forecast_error_gwh",
    "forecast_error_pct",
    "peak_gwh",
]
PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]
PRIORITY_COLORS = {
    "Critical": "#b42318",
    "Elevated": "#c76d1f",
    "Stable": "#157f5b",
}


def _derive_priority(row: pd.Series, thresholds: dict[str, float | None]) -> str:
    err_pct = pd.to_numeric(row.get("forecast_error_pct"), errors="coerce")
    if pd.isna(err_pct):
        return "Stable"
    abs_err = abs(err_pct)
    if thresholds["err_p90"] and abs_err >= thresholds["err_p90"]:
        return "Critical"
    if thresholds["err_p75"] and abs_err >= thresholds["err_p75"]:
        return "Elevated"
    return "Stable"


def _style_watchlist(value: str) -> str:
    color = PRIORITY_COLORS.get(value)
    if not color:
        return ""
    return f"background-color: {color}; color: white; font-weight: 600;"


st.set_page_config(
    page_title="Demand & Forecast Tracker",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] { font-size: 1.35rem; font-weight: 650; }
    [data-testid="stMetricLabel"] { font-size: 0.82rem; }
    .page-note { color: #4b5563; margin-bottom: 0.5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

if not table_has_rows("FACT_DEMAND_HOURLY"):
    st.warning("No demand rows found yet. Let the pipeline run first.")
    st.stop()

coverage = get_demand_coverage()
max_period = pd.to_datetime(coverage["max_period"], utc=True)
min_period = pd.to_datetime(coverage["min_period"], utc=True)
default_start, default_end = build_default_date_range(min_period, max_period, lookback_days=7)
all_bas = list_ba_codes("FACT_DEMAND_HOURLY")

with st.sidebar:
    st.title("Demand Tracker")
    st.caption("Filter the latest demand and forecast behavior before rendering charts.")
    selected_range = st.date_input(
        "UTC date range",
        value=(default_start, default_end),
        min_value=min_period.date(),
        max_value=max_period.date(),
    )
    selected_bas = st.multiselect("Balancing authorities", all_bas, default=all_bas)
    display_tz = st.selectbox("Display timezone", get_timezone_options(), index=0)
    with st.expander("How to read this page"):
        st.markdown(
            """
- KPI strip: latest system demand, worst miss, and watchlist count.
- Watchlist: BAs ranked by current forecast miss severity.
- Snapshot rankings: largest over-runs and under-runs right now.
- Focus BA: demand versus forecast plus error volatility for one BA.
- Supporting analysis: national map, scatter check, and CSV export.
"""
        )

if len(selected_range) != 2:
    st.info("Select a start and end date to load demand data.")
    st.stop()

start_date, end_date = selected_range
start_ts = f"{start_date}T00:00:00+00:00"
end_ts = f"{end_date}T23:59:59+00:00"
ba_filter = selected_bas or None

with st.spinner("Loading demand data from Snowflake..."):
    hourly_df = load_demand_hourly(start_ts, end_ts, ba_filter)
    latest_df = load_latest_demand_snapshot(start_ts, end_ts, ba_filter)
    peak_df = load_daily_demand_peak(str(start_date), str(end_date), ba_filter)

if hourly_df.empty:
    st.warning("No demand rows found for the selected filters.")
    st.stop()

hourly_df["period_ts"] = pd.to_datetime(hourly_df["period_ts"], utc=True)
hourly_df["period_display"] = convert_timestamp_series(hourly_df["period_ts"], display_tz)
hourly_df = coerce_numeric(hourly_df, NUMERIC_COLS)

hourly_df["forecast_error_gwh"] = hourly_df["demand_gwh"] - hourly_df["forecast_gwh"]
hourly_df["forecast_error_pct"] = (
    hourly_df["forecast_error_gwh"] / hourly_df["forecast_gwh"].replace(0, pd.NA) * 100
)

latest_df = coerce_numeric(latest_df, NUMERIC_COLS)
if "forecast_error_gwh" not in latest_df.columns:
    latest_df["forecast_error_gwh"] = latest_df["demand_gwh"] - latest_df["forecast_gwh"]
if "forecast_error_pct" not in latest_df.columns:
    latest_df["forecast_error_pct"] = (
        latest_df["forecast_error_gwh"] / latest_df["forecast_gwh"].replace(0, pd.NA) * 100
    )

peak_df["report_date"] = pd.to_datetime(peak_df["report_date"])
peak_df = coerce_numeric(peak_df, ["peak_gwh"])

watchlist_df = latest_df.copy()
thresholds = {
    "err_p90": safe_quantile(watchlist_df["forecast_error_pct"].abs(), 0.9),
    "err_p75": safe_quantile(watchlist_df["forecast_error_pct"].abs(), 0.75),
}
watchlist_df["priority"] = watchlist_df.apply(_derive_priority, axis=1, thresholds=thresholds)
priority_dtype = pd.CategoricalDtype(categories=PRIORITY_ORDER, ordered=True)
watchlist_df["priority"] = watchlist_df["priority"].astype(priority_dtype)
watchlist_df = watchlist_df.sort_values(
    ["priority", "forecast_error_pct"],
    ascending=[True, False],
    key=lambda series: series.abs() if series.name == "forecast_error_pct" else series,
)

latest_period = hourly_df["period_ts"].max()
window_hours = hourly_df["period_ts"].nunique()
bas_in_scope = hourly_df["ba_code"].nunique()
total_demand = latest_df["demand_gwh"].sum()
latest_forecast = latest_df["forecast_gwh"].sum()
worst_miss = latest_df["forecast_error_pct"].abs().max()
critical_count = int((watchlist_df["priority"] == "Critical").sum())

st.title("Demand & Forecast Tracker")
st.markdown(
    (
        f"<div class='page-note'>Window: <strong>{start_date}</strong> to "
        f"<strong>{end_date}</strong> | Latest period: "
        f"<strong>{format_timestamp(latest_period, display_tz)}</strong> | "
        f"BAs: <strong>{bas_in_scope}</strong> | Hourly points: "
        f"<strong>{window_hours}</strong></div>"
    ),
    unsafe_allow_html=True,
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest demand (GWh)", f"{total_demand:,.1f}" if pd.notna(total_demand) else "n/a")
k2.metric("Latest forecast (GWh)", f"{latest_forecast:,.1f}" if pd.notna(latest_forecast) else "n/a")
k3.metric("Worst forecast miss", f"{worst_miss:.1f}%" if pd.notna(worst_miss) else "n/a")
k4.metric("Critical BAs", f"{critical_count:,}")
k5.metric("BAs in scope", f"{bas_in_scope:,}")

st.subheader("Watchlist")
st.caption("Current latest-snapshot forecast miss severity. Critical BAs are above the 90th percentile of absolute forecast error.")
watchlist_display = watchlist_df[
    [
        "ba_code",
        "ba_name",
        "priority",
        "demand_gwh",
        "forecast_gwh",
        "forecast_error_gwh",
        "forecast_error_pct",
    ]
].rename(
    columns={
        "ba_code": "BA",
        "ba_name": "Balancing authority",
        "priority": "Priority",
        "demand_gwh": "Demand (GWh)",
        "forecast_gwh": "Forecast (GWh)",
        "forecast_error_gwh": "Error (GWh)",
        "forecast_error_pct": "Error (%)",
    }
)
st.dataframe(
    watchlist_display.style.applymap(_style_watchlist, subset=["Priority"]),
    use_container_width=True,
    hide_index=True,
)

st.subheader("Latest snapshot rankings")
st.caption("Largest forecast over-runs and under-runs for the latest hour in the selected window.")
rank_left, rank_right = st.columns(2)

over_df = (
    latest_df[latest_df["forecast_error_gwh"] > 0]
    .dropna(subset=["forecast_error_gwh"])
    .sort_values("forecast_error_gwh", ascending=False)
    .head(10)
)
under_df = (
    latest_df[latest_df["forecast_error_gwh"] < 0]
    .dropna(subset=["forecast_error_gwh"])
    .sort_values("forecast_error_gwh", ascending=True)
    .head(10)
)

if not over_df.empty:
    rank_left.plotly_chart(
        px.bar(
            over_df.sort_values("forecast_error_gwh", ascending=True),
            x="forecast_error_gwh",
            y="ba_code",
            orientation="h",
            color_discrete_sequence=["#c76d1f"],
            labels={"forecast_error_gwh": "Error (GWh)", "ba_code": "BA"},
            title="Largest over-runs",
        ),
        use_container_width=True,
    )
else:
    rank_left.info("No demand over-runs in the latest snapshot.")

if not under_df.empty:
    rank_right.plotly_chart(
        px.bar(
            under_df.sort_values("forecast_error_gwh", ascending=False),
            x="forecast_error_gwh",
            y="ba_code",
            orientation="h",
            color_discrete_sequence=["#2563eb"],
            labels={"forecast_error_gwh": "Error (GWh)", "ba_code": "BA"},
            title="Largest under-runs",
        ),
        use_container_width=True,
    )
else:
    rank_right.info("No demand under-runs in the latest snapshot.")

focus_options = sorted(hourly_df["ba_code"].dropna().unique().tolist())
focus_ba = st.selectbox("Focus balancing authority", focus_options, index=0)
focus_df = hourly_df[hourly_df["ba_code"] == focus_ba].copy().sort_values("period_ts")

st.subheader(f"Focus BA: {focus_ba}")
focus_left, focus_right = st.columns(2)

if not focus_df.empty:
    trend_df = (
        focus_df[["period_display", "demand_gwh", "forecast_gwh"]]
        .melt(id_vars="period_display", var_name="series", value_name="gwh")
        .dropna(subset=["gwh"])
    )
    trend_df["series"] = trend_df["series"].replace(
        {"demand_gwh": "Actual demand", "forecast_gwh": "Day-ahead forecast"}
    )
    focus_left.plotly_chart(
        px.line(
            trend_df,
            x="period_display",
            y="gwh",
            color="series",
            color_discrete_map={
                "Actual demand": "#2563eb",
                "Day-ahead forecast": "#c76d1f",
            },
            labels={"period_display": f"Period ({display_tz})", "gwh": "GWh", "series": ""},
            title="Demand vs forecast",
        ),
        use_container_width=True,
    )

    error_df = focus_df.dropna(subset=["forecast_error_gwh"]).copy()
    if not error_df.empty:
        error_mean = error_df["forecast_error_gwh"].mean()
        error_std = error_df["forecast_error_gwh"].std()
        error_df["error_zscore"] = (
            (error_df["forecast_error_gwh"] - error_mean) / error_std
            if error_std and error_std > 0
            else 0.0
        )

        error_fig = go.Figure()
        error_fig.add_trace(
            go.Bar(
                x=error_df["period_display"],
                y=error_df["forecast_error_gwh"],
                name="Error (GWh)",
                marker=dict(color=error_df["forecast_error_gwh"], colorscale="RdBu", cmid=0),
                opacity=0.75,
                yaxis="y",
            )
        )
        error_fig.add_trace(
            go.Scatter(
                x=error_df["period_display"],
                y=error_df["error_zscore"],
                name="Error z-score",
                line=dict(color="#7c3aed", width=2),
                yaxis="y2",
            )
        )
        error_fig.update_layout(
            title="Forecast error and volatility",
            yaxis=dict(title="Error (GWh)"),
            yaxis2=dict(title="Z-score", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", y=1.08),
        )
        focus_right.plotly_chart(error_fig, use_container_width=True)
    else:
        focus_right.info("No forecast error data available for this BA.")

st.subheader("Daily peak demand")
st.caption("Peak hourly demand per BA per day across the selected window.")
if not peak_df.empty:
    top_peak_bas = (
        peak_df.groupby("ba_code")["peak_gwh"].mean()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )
    st.plotly_chart(
        px.line(
            peak_df[peak_df["ba_code"].isin(top_peak_bas)],
            x="report_date",
            y="peak_gwh",
            color="ba_code",
            labels={"report_date": "Date", "peak_gwh": "Peak GWh", "ba_code": "BA"},
            title="Top 10 BAs by average peak demand",
        ),
        use_container_width=True,
    )
else:
    st.info("No daily peak demand rows for the selected filters.")

with st.expander("Supporting analysis", expanded=False):
    support_left, support_right = st.columns(2)

    map_df = latest_df.copy()
    map_df["lat"] = map_df["ba_code"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lat"))
    map_df["lon"] = map_df["ba_code"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lon"))
    map_df = map_df.dropna(subset=["lat", "lon", "demand_gwh"])
    if not map_df.empty:
        support_left.plotly_chart(
            px.scatter_geo(
                map_df,
                lat="lat",
                lon="lon",
                color="forecast_error_pct",
                size="demand_gwh",
                hover_name="ba_code",
                hover_data={
                    "ba_name": True,
                    "forecast_error_pct": ":.1f",
                    "demand_gwh": ":.2f",
                },
                color_continuous_scale="RdBu",
                color_continuous_midpoint=0,
                scope="usa",
                title="Forecast error by BA",
            ),
            use_container_width=True,
        )
        support_left.caption("Approximate BA coordinates only.")
    else:
        support_left.info("No mappable rows in the latest snapshot.")

    snapshot_scatter = latest_df.dropna(subset=["demand_gwh", "forecast_gwh"])
    if not snapshot_scatter.empty:
        scatter_fig = px.scatter(
            snapshot_scatter,
            x="forecast_gwh",
            y="demand_gwh",
            text="ba_code",
            color="forecast_error_pct",
            color_continuous_scale="RdBu",
            color_continuous_midpoint=0,
            labels={
                "forecast_gwh": "Forecast (GWh)",
                "demand_gwh": "Actual demand (GWh)",
                "forecast_error_pct": "Error (%)",
            },
            title="Actual vs forecast",
        )
        max_point = max(snapshot_scatter["forecast_gwh"].max(), snapshot_scatter["demand_gwh"].max())
        scatter_fig.add_shape(
            type="line",
            x0=0,
            y0=0,
            x1=max_point,
            y1=max_point,
            line=dict(color="#6b7280", dash="dash"),
        )
        scatter_fig.update_traces(textposition="top center")
        support_right.plotly_chart(scatter_fig, use_container_width=True)
    else:
        support_right.info("No overlapping actual and forecast rows for the latest snapshot.")

    if not focus_df.empty:
        st.download_button(
            label=f"Download {focus_ba} demand data as CSV",
            data=focus_df[
                [
                    "period_display",
                    "ba_code",
                    "ba_name",
                    "demand_gwh",
                    "forecast_gwh",
                    "forecast_error_gwh",
                    "forecast_error_pct",
                ]
            ].to_csv(index=False).encode("utf-8"),
            file_name=f"{focus_ba.lower()}_demand_hourly_{start_date}_{end_date}.csv",
            mime="text/csv",
        )
