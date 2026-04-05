"""Demand & Forecast Tracker dashboard.

Monitors hourly electricity demand vs day-ahead forecast per balancing authority.
Surfaces regions with the largest forecast misses and tracks daily peak demand trends.

Data source: Snowflake GOLD.FACT_HOURLY via Snowpark (data_access helpers).
"""

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

# ── Constants ─────────────────────────────────────────────────────────────────

NUMERIC_COLS = [
    "demand_gwh",
    "forecast_gwh",
    "forecast_error_gwh",
    "forecast_error_pct",
    "peak_gwh",
]

PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]

PRIORITY_COLORS = {
    "Critical": "#E24B4A",
    "Elevated": "#EF9F27",
    "Stable":   "#1D9E75",
}

WATCHLIST_DISPLAY_COLUMNS = {
    "ba_code":             "BA",
    "ba_name":             "Name",
    "priority":            "Priority",
    "demand_gwh":          "Demand (GWh)",
    "forecast_gwh":        "Forecast (GWh)",
    "forecast_error_gwh":  "Error (GWh)",
    "forecast_error_pct":  "Error (%)",
}

# ── Helpers ───────────────────────────────────────────────────────────────────


def _derive_priority(row: pd.Series, thresholds: dict) -> str:
    err_pct = pd.to_numeric(row.get("forecast_error_pct"), errors="coerce")
    if pd.isna(err_pct):
        return "Stable"
    abs_err = abs(err_pct)
    if thresholds["err_p90"] and abs_err >= thresholds["err_p90"]:
        return "Critical"
    if thresholds["err_p75"] and abs_err >= thresholds["err_p75"]:
        return "Elevated"
    return "Stable"


def _color_priority(val: str) -> str:
    colors = {"Critical": "#E24B4A", "Elevated": "#EF9F27", "Stable": "#1D9E75"}
    return f"background-color: {colors.get(val, '')}"


# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Demand & Forecast Tracker",
    page_icon="📊",
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

# ── Guard: ensure data exists ─────────────────────────────────────────────────

if not table_has_rows("FACT_HOURLY"):
    st.warning(
        "⚠️ No rows found in `FACT_HOURLY`. "
        "Let the `eia_gold` DAG complete at least one run first."
    )
    st.stop()

# ── Coverage + BA list (used to build sidebar widgets) ───────────────────────

coverage    = get_demand_coverage()
max_period  = pd.to_datetime(coverage["max_period"], utc=True)
min_period  = pd.to_datetime(coverage["min_period"], utc=True)
default_start, default_end = build_default_date_range(
    min_period, max_period, lookback_days=7
)
all_bas = list_ba_codes("FACT_HOURLY")

# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📊 Demand Tracker")
    st.caption("Filter the dashboard below.")
    st.divider()

    selected_range = st.date_input(
        "Date range (UTC)",
        value=(default_start, default_end),
        min_value=min_period.date(),
        max_value=max_period.date(),
    )

    selected_bas = st.multiselect(
        "Balancing authorities",
        options=all_bas,
        default=all_bas,
        help="Leave blank to include all BAs.",
    )

    display_tz = st.selectbox("Display timezone", get_timezone_options(), index=0)

    st.divider()
    with st.expander("How to read this page"):
        st.markdown("""
- **KPI strip** — system-wide demand health at a glance.
- **Watchlist** — BAs ranked by forecast error severity; Critical rows need attention now.
- **Where?** — bar charts showing which BAs have the largest forecast misses right now.
- **Focus BA trends** — hourly demand vs forecast + forecast error waterfall for one BA.
- **Daily peak demand** — rolling peak trend across the top BAs.
- **Supporting analysis** — US map and CSV export.
""")

if len(selected_range) != 2:
    st.info("Select a start and end date to load data.")
    st.stop()

start_date, end_date = selected_range
start_ts  = f"{start_date}T00:00:00+00:00"
end_ts    = f"{end_date}T23:59:59+00:00"
ba_filter = selected_bas or None

# ── Data load ─────────────────────────────────────────────────────────────────

with st.spinner("Loading demand data from Snowflake…"):
    hourly_df = load_demand_hourly(start_ts, end_ts, ba_filter)
    latest_df = load_latest_demand_snapshot(start_ts, end_ts, ba_filter)
    peak_df   = load_daily_demand_peak(str(start_date), str(end_date), ba_filter)

if hourly_df.empty:
    st.warning("No demand rows found for the selected filters.")
    st.stop()

# ── Coerce & enrich ───────────────────────────────────────────────────────────

hourly_df["period_ts"]      = pd.to_datetime(hourly_df["period_ts"], utc=True)
hourly_df["period_display"] = convert_timestamp_series(hourly_df["period_ts"], display_tz)
hourly_df = coerce_numeric(hourly_df, NUMERIC_COLS)

# Compute forecast error columns from hourly data for focus-BA waterfall
hourly_df["forecast_error_gwh"] = hourly_df["demand_gwh"] - hourly_df["forecast_gwh"]
hourly_df["forecast_error_pct"] = (
    hourly_df["forecast_error_gwh"] / hourly_df["forecast_gwh"].replace(0, float("nan")) * 100
)

latest_df = coerce_numeric(latest_df, NUMERIC_COLS)
# Ensure error columns exist on latest_df
if "forecast_error_gwh" not in latest_df.columns:
    latest_df["forecast_error_gwh"] = latest_df["demand_gwh"] - latest_df["forecast_gwh"]
if "forecast_error_pct" not in latest_df.columns:
    latest_df["forecast_error_pct"] = (
        latest_df["forecast_error_gwh"]
        / latest_df["forecast_gwh"].replace(0, float("nan"))
        * 100
    )

peak_df["report_date"] = pd.to_datetime(peak_df["report_date"])
peak_df = coerce_numeric(peak_df, ["peak_gwh"])

# ── Watchlist derivation ──────────────────────────────────────────────────────

watchlist_df = latest_df.copy()
thresholds = {
    "err_p90": safe_quantile(watchlist_df["forecast_error_pct"].abs(), 0.9),
    "err_p75": safe_quantile(watchlist_df["forecast_error_pct"].abs(), 0.75),
}
watchlist_df["priority"] = watchlist_df.apply(
    _derive_priority, axis=1, thresholds=thresholds
)
priority_dtype = pd.CategoricalDtype(categories=PRIORITY_ORDER, ordered=True)
watchlist_df["priority"] = watchlist_df["priority"].astype(priority_dtype)
watchlist_df = watchlist_df.sort_values(
    ["priority", "forecast_error_pct"],
    ascending=[True, False],
    key=lambda col: col.abs() if col.name == "forecast_error_pct" else col,
)

# ── KPI derivations ───────────────────────────────────────────────────────────

latest_period  = hourly_df["period_ts"].max()
n_bas          = hourly_df["ba_code"].nunique()
total_demand   = latest_df["demand_gwh"].sum()
worst_miss     = latest_df["forecast_error_pct"].abs().max()
critical_count = int((watchlist_df["priority"] == "Critical").sum())

# ── Page header ───────────────────────────────────────────────────────────────

st.title("📊 Demand & Forecast Tracker")
st.caption(
    f"Showing **{start_date}** → **{end_date}** · "
    f"**{n_bas}** balancing authorities · "
    f"Latest period: **{format_timestamp(latest_period, display_tz)}**"
)

# ── Section 1 — KPI strip ─────────────────────────────────────────────────────

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest period",       format_timestamp(latest_period, display_tz))
k2.metric("BAs in scope",        f"{n_bas:,}")
k3.metric(
    "Total demand (GWh)",
    f"{total_demand:,.1f}" if pd.notna(total_demand) else "n/a",
)
k4.metric(
    "Worst forecast miss",
    f"{worst_miss:.1f}%" if pd.notna(worst_miss) else "n/a",
    help="Largest absolute forecast error % across all BAs in the latest snapshot",
)
k5.metric(
    "Critical BAs",
    f"{critical_count:,}",
    help="BAs whose forecast error % is at or above the 90th-percentile threshold",
)

st.divider()

# ── Section 2 — Watchlist ─────────────────────────────────────────────────────

st.subheader("Watchlist — what needs attention now?")
st.caption(
    "BAs ranked Critical → Elevated → Stable by forecast error severity "
    "in the latest hourly snapshot."
)

display_cols = [c for c in WATCHLIST_DISPLAY_COLUMNS if c in watchlist_df.columns]
display_watchlist = (
    watchlist_df[display_cols]
    .rename(columns=WATCHLIST_DISPLAY_COLUMNS)
    .reset_index(drop=True)
)

st.dataframe(
    display_watchlist.style.map(_color_priority, subset=["Priority"]),
    use_container_width=True,
    hide_index=True,
    height=min(40 + 35 * len(display_watchlist), 400),
)

st.divider()

# ── Section 3 — Where are the largest forecast misses? ───────────────────────

st.subheader("Where are the largest forecast misses right now?")
st.caption("Latest hourly snapshot — top 10 BAs by absolute forecast error (GWh).")

rank_col1, rank_col2 = st.columns(2)

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
    fig_over = px.bar(
        over_df.sort_values("forecast_error_gwh", ascending=True),
        x="forecast_error_gwh",
        y="ba_code",
        orientation="h",
        color_discrete_sequence=["#f97316"],
        labels={"forecast_error_gwh": "Over-forecast error (GWh)", "ba_code": "BA"},
        title="Largest demand over-runs (actual > forecast)",
    )
    fig_over.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    rank_col1.plotly_chart(fig_over, use_container_width=True)
else:
    rank_col1.info("No over-forecast BAs in the latest snapshot.")

if not under_df.empty:
    fig_under = px.bar(
        under_df.sort_values("forecast_error_gwh", ascending=False),
        x="forecast_error_gwh",
        y="ba_code",
        orientation="h",
        color_discrete_sequence=["#3b82f6"],
        labels={"forecast_error_gwh": "Under-forecast error (GWh)", "ba_code": "BA"},
        title="Largest demand under-runs (actual < forecast)",
    )
    fig_under.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    rank_col2.plotly_chart(fig_under, use_container_width=True)
else:
    rank_col2.info("No under-forecast BAs in the latest snapshot.")

st.divider()

# ── Section 4 — Focus BA trends ───────────────────────────────────────────────

focus_options  = sorted(hourly_df["ba_code"].dropna().unique().tolist())
focus_ba       = st.selectbox(
    "Focus balancing authority",
    focus_options,
    index=0,
    help="Drill into a single BA's hourly demand, forecast, and error trend.",
)
focus_df = hourly_df[hourly_df["ba_code"] == focus_ba].copy().sort_values("period_ts")

st.subheader(f"Trends — {focus_ba}")
st.caption("Hourly demand vs forecast and forecast error for the selected BA.")

trend_l, trend_r = st.columns(2)

# Demand vs forecast line chart
demand_melt = (
    focus_df[["period_display", "demand_gwh", "forecast_gwh"]]
    .melt(id_vars="period_display", var_name="series", value_name="gwh")
    .dropna(subset=["gwh"])
)
demand_melt["series"] = demand_melt["series"].replace(
    {"demand_gwh": "Actual demand", "forecast_gwh": "Day-ahead forecast"}
)

if not demand_melt.empty:
    fig_dem = px.line(
        demand_melt,
        x="period_display",
        y="gwh",
        color="series",
        color_discrete_map={
            "Actual demand":      "#3b82f6",
            "Day-ahead forecast": "#f97316",
        },
        labels={
            "period_display": f"Period ({display_tz})",
            "gwh":            "GWh",
            "series":         "",
        },
        title=f"{focus_ba} — actual demand vs day-ahead forecast",
    )
    fig_dem.update_layout(
        legend=dict(orientation="h", y=1.1), margin=dict(l=0, r=0, t=50, b=0)
    )
    trend_l.plotly_chart(fig_dem, use_container_width=True)
else:
    trend_l.info("No overlapping demand and forecast series for this BA.")

# Forecast error z-score / waterfall (dual-axis)
error_df = focus_df.dropna(subset=["forecast_error_gwh"])
if not error_df.empty:
    # Compute a rolling z-score for the error series
    err_mean  = error_df["forecast_error_gwh"].mean()
    err_std   = error_df["forecast_error_gwh"].std()
    error_df  = error_df.copy()
    error_df["error_zscore"] = (
        (error_df["forecast_error_gwh"] - err_mean) / err_std
        if err_std and err_std > 0
        else 0.0
    )

    fig_err = go.Figure()
    fig_err.add_trace(
        go.Bar(
            x=error_df["period_display"],
            y=error_df["forecast_error_gwh"],
            name="Error (GWh)",
            marker=dict(
                color=error_df["forecast_error_gwh"],
                colorscale="RdBu",
                cmid=0,
            ),
            opacity=0.75,
            yaxis="y",
        )
    )
    fig_err.add_trace(
        go.Scatter(
            x=error_df["period_display"],
            y=error_df["error_zscore"],
            name="Error z-score",
            line=dict(color="#7c3aed", width=2),
            yaxis="y2",
        )
    )
    fig_err.add_hline(
        y=2.0,
        line_dash="dot",
        line_color="#E24B4A",
        annotation_text="+2σ",
        yref="y2",
    )
    fig_err.add_hline(
        y=-2.0,
        line_dash="dot",
        line_color="#E24B4A",
        annotation_text="-2σ",
        yref="y2",
    )
    fig_err.update_layout(
        title=f"{focus_ba} — hourly forecast error (actual minus forecast)",
        yaxis=dict(title="Error (GWh)"),
        yaxis2=dict(
            title="Z-score",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=0, r=0, t=50, b=0),
    )
    trend_r.plotly_chart(fig_err, use_container_width=True)
else:
    trend_r.info("No forecast error data for this BA.")

st.divider()

# ── Section 5 — Daily peak demand trend ──────────────────────────────────────

st.subheader("Daily peak demand trend")
st.caption("Peak hourly demand per BA per day across the selected window — top 10 BAs by average peak.")

if not peak_df.empty:
    top_peak_bas = (
        peak_df.groupby("ba_code")["peak_gwh"].mean()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )
    fig_peak = px.line(
        peak_df[peak_df["ba_code"].isin(top_peak_bas)],
        x="report_date",
        y="peak_gwh",
        color="ba_code",
        labels={"report_date": "Date", "peak_gwh": "Peak GWh", "ba_code": "BA"},
        title="Daily peak demand — top 10 BAs by average peak",
    )
    fig_peak.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_peak, use_container_width=True)
else:
    st.info("No daily peak demand data available for the selected filters.")

st.divider()

# ── Section 6 — Supporting analysis ──────────────────────────────────────────

with st.expander("Supporting analysis", expanded=False):
    sup_l, sup_r = st.columns(2)

    # US scatter map — forecast error by BA
    map_df          = latest_df.copy()
    map_df["lat"]   = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lat"))
    map_df["lon"]   = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lon"))
    map_df["label"] = map_df["ba_code"].map(
        lambda c: RESPONDENT_GEO.get(c, {}).get("label", c)
    )
    map_df = map_df.dropna(subset=["lat", "lon", "demand_gwh"])

    if not map_df.empty:
        fig_map = px.scatter_geo(
            map_df,
            lat="lat",
            lon="lon",
            color="forecast_error_pct",
            size="demand_gwh",
            size_max=50,
            hover_name="ba_code",
            hover_data={
                "demand_gwh":         ":.2f",
                "forecast_error_pct": ":.1f",
                "lat":                False,
                "lon":                False,
            },
            color_continuous_scale="RdBu",
            color_continuous_midpoint=0,
            scope="usa",
            title="Forecast error by BA (latest hour) — red = over-run, blue = under-run",
        )
        fig_map.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        sup_l.plotly_chart(fig_map, use_container_width=True)
        sup_l.caption("⚠️ Approximate BA coordinates only.")
    else:
        sup_l.info("No mappable rows in the latest snapshot.")

    # Demand vs forecast scatter — all BAs in the latest snapshot
    snap_plot = latest_df.dropna(subset=["demand_gwh", "forecast_gwh"])
    if not snap_plot.empty:
        fig_scatter = px.scatter(
            snap_plot,
            x="forecast_gwh",
            y="demand_gwh",
            text="ba_code",
            color="forecast_error_pct",
            color_continuous_scale="RdBu",
            color_continuous_midpoint=0,
            labels={
                "forecast_gwh":       "Forecast (GWh)",
                "demand_gwh":         "Actual (GWh)",
                "forecast_error_pct": "Error (%)",
            },
            title="Actual vs forecast — all BAs (latest snapshot)",
        )
        # Perfect-forecast reference line
        max_val = max(snap_plot["forecast_gwh"].max(), snap_plot["demand_gwh"].max())
        fig_scatter.add_shape(
            type="line",
            x0=0, y0=0, x1=max_val, y1=max_val,
            line=dict(dash="dash", color="grey", width=1),
        )
        fig_scatter.update_traces(textposition="top center")
        fig_scatter.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        sup_r.plotly_chart(fig_scatter, use_container_width=True)
        sup_r.caption(
            "Points above the dashed line = actual demand exceeded forecast. "
            "Points below = forecast was too high."
        )
    else:
        sup_r.info("No overlapping demand and forecast rows in the latest snapshot.")

    # CSV download for the focus BA
    if not focus_df.empty:
        st.divider()
        dl_cols = [
            c for c in [
                "period_display", "ba_code",
                "demand_gwh", "forecast_gwh",
                "forecast_error_gwh", "forecast_error_pct",
            ]
            if c in focus_df.columns
        ]
        st.download_button(
            label=f"⬇️ Download {focus_ba} hourly demand data as CSV",
            data=focus_df[dl_cols].to_csv(index=False).encode("utf-8"),
            file_name=f"{focus_ba.lower()}_demand_hourly_{start_date}_{end_date}.csv",
            mime="text/csv",
        )