"""Demand & Forecast Tracker dashboard.

Monitors hourly electricity demand vs day-ahead forecast per balancing authority.
Surfaces regions with the largest forecast misses and tracks daily peak demand trends.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import (
    get_demand_coverage,
    list_ba_codes,
    load_demand_hourly,
    load_daily_demand_peak,
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


def _derive_priority(row: pd.Series, thresholds: dict) -> str:
    """Classify one BA's forecast miss into a priority tier."""
    err_pct = pd.to_numeric(row.get("forecast_error_pct"), errors="coerce")
    err_abs = abs(pd.to_numeric(row.get("forecast_error_gwh"), errors="coerce"))

    if thresholds["err_p90"] and abs(err_pct) >= thresholds["err_p90"]:
        return "Critical"
    if thresholds["err_p75"] and abs(err_pct) >= thresholds["err_p75"]:
        return "Elevated"
    return "Stable"


st.set_page_config(page_title="Demand & Forecast Tracker", layout="wide")
st.title("Demand & Forecast Tracker")
st.caption("Monitor electricity demand vs forecast and surface balancing authorities with the largest misses.")

with st.expander("How to read this page"):
    st.markdown("""
- **KPI row** — current snapshot totals and worst forecast miss.
- **Latest snapshot rankings** — which BAs have the largest forecast error right now.
- **Watchlist** — BAs ranked by forecast miss severity using current-window quantile thresholds.
- **Focus BA trends** — hourly demand vs forecast for one selected BA.
- **Daily peak demand** — rolling peak demand trend across BAs.
""")

if not table_has_rows("fact_demand_hourly"):
    st.warning("No demand rows found yet. Let the pipeline run first.")
    st.stop()

# ── Filters ───────────────────────────────────────────────────────────────────
coverage = get_demand_coverage()
max_period = pd.to_datetime(coverage["max_period"], utc=True)
min_period = pd.to_datetime(coverage["min_period"], utc=True)
default_start, default_end = build_default_date_range(min_period, max_period, lookback_days=7)
all_bas = list_ba_codes("fact_demand_hourly")

col_a, col_b, col_c = st.columns([2, 2, 1])
selected_range = col_a.date_input(
    "UTC date range",
    value=(default_start, default_end),
    min_value=min_period.date(),
    max_value=max_period.date(),
)
selected_bas = col_b.multiselect("Balancing authorities", all_bas, default=all_bas)
display_tz = col_c.selectbox("Timezone", get_timezone_options(), index=0)

if len(selected_range) != 2:
    st.stop()

start_date, end_date = selected_range
start_ts = f"{start_date}T00:00:00+00:00"
end_ts = f"{end_date}T23:59:59+00:00"
ba_filter = selected_bas or None

# ── Load data ─────────────────────────────────────────────────────────────────
hourly_df = load_demand_hourly(start_ts, end_ts, ba_filter)
latest_df = load_latest_demand_snapshot(start_ts, end_ts, ba_filter)
peak_df = load_daily_demand_peak(str(start_date), str(end_date), ba_filter)

if hourly_df.empty:
    st.warning("No demand rows found for the selected filters.")
    st.stop()

hourly_df["period_ts"] = pd.to_datetime(hourly_df["period_ts"], utc=True)
hourly_df["period_display"] = convert_timestamp_series(hourly_df["period_ts"], display_tz)
hourly_df = coerce_numeric(hourly_df, NUMERIC_COLS)

latest_df = coerce_numeric(latest_df, NUMERIC_COLS)
peak_df["report_date"] = pd.to_datetime(peak_df["report_date"])
peak_df = coerce_numeric(peak_df, ["peak_gwh"])

# ── Watchlist derivation ──────────────────────────────────────────────────────
watchlist_df = latest_df.copy()
thresholds = {
    "err_p90": safe_quantile(watchlist_df["forecast_error_pct"].abs(), 0.9),
    "err_p75": safe_quantile(watchlist_df["forecast_error_pct"].abs(), 0.75),
}
watchlist_df["priority"] = watchlist_df.apply(_derive_priority, axis=1, thresholds=thresholds)

priority_order = pd.CategoricalDtype(categories=PRIORITY_ORDER, ordered=True)
watchlist_df["priority"] = watchlist_df["priority"].astype(priority_order)
watchlist_df = watchlist_df.sort_values(
    ["priority", "forecast_error_pct"],
    ascending=[True, False],
    key=lambda col: col.abs() if col.name == "forecast_error_pct" else col,
)

# ── KPI row ───────────────────────────────────────────────────────────────────
latest_period = hourly_df["period_ts"].max()
total_demand = latest_df["demand_gwh"].sum()
total_forecast = latest_df["forecast_gwh"].sum()
worst_miss = latest_df["forecast_error_pct"].abs().max()
critical_count = int((watchlist_df["priority"] == "Critical").sum())

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest period", format_timestamp(latest_period, display_tz))
k2.metric("BAs in scope", f"{hourly_df['ba_code'].nunique():,}")
k3.metric("Total demand (GWh)", f"{total_demand:,.1f}" if pd.notna(total_demand) else "n/a")
k4.metric("Worst forecast miss", f"{worst_miss:.1f}%" if pd.notna(worst_miss) else "n/a")
k5.metric("Critical BAs", f"{critical_count:,}")

# ── Watchlist ─────────────────────────────────────────────────────────────────
st.subheader("Forecast miss watchlist")
st.caption("BAs ranked by forecast error severity in the latest hourly snapshot.")
st.dataframe(
    watchlist_df[[
        "ba_code", "ba_name", "demand_gwh", "forecast_gwh",
        "forecast_error_gwh", "forecast_error_pct", "priority",
    ]].rename(columns={
        "forecast_error_gwh": "error_gwh",
        "forecast_error_pct": "error_pct",
    }),
    use_container_width=True,
    hide_index=True,
)

# ── Latest snapshot rankings ──────────────────────────────────────────────────
st.subheader("Where are the largest forecast misses right now?")
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
    rank_col1.plotly_chart(
        px.bar(
            over_df.sort_values("forecast_error_gwh", ascending=True),
            x="forecast_error_gwh", y="ba_code", orientation="h",
            color_discrete_sequence=["#f97316"],
            labels={"forecast_error_gwh": "Over-forecast error (GWh)", "ba_code": "BA"},
            title="Largest demand over-runs (actual > forecast)",
        ),
        use_container_width=True,
    )
else:
    rank_col1.info("No over-forecast BAs in the latest snapshot.")

if not under_df.empty:
    rank_col2.plotly_chart(
        px.bar(
            under_df.sort_values("forecast_error_gwh", ascending=False),
            x="forecast_error_gwh", y="ba_code", orientation="h",
            color_discrete_sequence=["#3b82f6"],
            labels={"forecast_error_gwh": "Under-forecast error (GWh)", "ba_code": "BA"},
            title="Largest demand under-runs (actual < forecast)",
        ),
        use_container_width=True,
    )
else:
    rank_col2.info("No under-forecast BAs in the latest snapshot.")

# ── Focus BA hourly trend ─────────────────────────────────────────────────────
st.subheader("Focus balancing authority — demand vs forecast")
focus_options = sorted(hourly_df["ba_code"].dropna().unique().tolist())
focus_ba = st.selectbox("Focus BA", focus_options, index=0)
focus_df = hourly_df[hourly_df["ba_code"] == focus_ba].copy().sort_values("period_ts")

if not focus_df.empty:
    demand_plot = (
        focus_df[["period_display", "demand_gwh", "forecast_gwh"]]
        .melt(id_vars="period_display", var_name="series", value_name="gwh")
        .dropna(subset=["gwh"])
    )
    demand_plot["series"] = demand_plot["series"].replace({
        "demand_gwh": "Actual demand",
        "forecast_gwh": "Day-ahead forecast",
    })

    st.plotly_chart(
        px.line(
            demand_plot,
            x="period_display", y="gwh", color="series",
            color_discrete_map={"Actual demand": "#3b82f6", "Day-ahead forecast": "#f97316"},
            labels={"period_display": f"Period ({display_tz})", "gwh": "GWh", "series": ""},
            title=f"{focus_ba} — actual demand vs day-ahead forecast",
        ),
        use_container_width=True,
    )

    error_df = focus_df.dropna(subset=["forecast_error_gwh"])
    if not error_df.empty:
        st.plotly_chart(
            px.bar(
                error_df,
                x="period_display", y="forecast_error_gwh",
                color="forecast_error_gwh",
                color_continuous_scale="RdBu",
                color_continuous_midpoint=0,
                labels={"period_display": f"Period ({display_tz})", "forecast_error_gwh": "Error (GWh)"},
                title=f"{focus_ba} — hourly forecast error (actual minus forecast)",
            ),
            use_container_width=True,
        )

# ── Daily peak demand trend ───────────────────────────────────────────────────
st.subheader("Daily peak demand trend")
st.caption("Peak hourly demand per BA per day across the selected window.")

if not peak_df.empty:
    top_peak_bas = (
        peak_df.groupby("ba_code")["peak_gwh"].mean()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )
    peak_plot = peak_df[peak_df["ba_code"].isin(top_peak_bas)]
    st.plotly_chart(
        px.line(
            peak_plot,
            x="report_date", y="peak_gwh", color="ba_code",
            labels={"report_date": "Date", "peak_gwh": "Peak GWh", "ba_code": "BA"},
            title="Daily peak demand — top 10 BAs by average peak",
        ),
        use_container_width=True,
    )

# ── Supporting analysis ───────────────────────────────────────────────────────
with st.expander("Supporting analysis", expanded=False):
    map_df = latest_df.copy()
    map_df["lat"] = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lat"))
    map_df["lon"] = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lon"))
    map_df["label"] = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("label", c))
    map_df = map_df.dropna(subset=["lat", "lon", "demand_gwh"])

    if not map_df.empty:
        st.plotly_chart(
            px.scatter_geo(
                map_df,
                lat="lat", lon="lon",
                color="forecast_error_pct",
                size="demand_gwh",
                hover_name="ba_code",
                hover_data={"ba_name": True, "demand_gwh": ":.2f", "forecast_error_pct": ":.1f"},
                color_continuous_scale="RdBu",
                color_continuous_midpoint=0,
                scope="usa",
                title="Forecast error by BA (latest hour) — red = over-run, blue = under-run",
            ),
            use_container_width=True,
        )
        st.caption("Approximate BA coordinates only.")

    if not focus_df.empty:
        st.download_button(
            label="Download focus BA demand data as CSV",
            data=focus_df[[
                "period_display", "ba_code", "ba_name",
                "demand_gwh", "forecast_gwh", "forecast_error_gwh",
            ]].to_csv(index=False).encode("utf-8"),
            file_name=f"{focus_ba.lower()}_demand_hourly.csv",
            mime="text/csv",
        )
