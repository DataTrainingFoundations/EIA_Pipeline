"""Generation Mix Monitor dashboard.

Tracks hourly electricity generation by fuel type across balancing authorities.
Surfaces which BAs are most dependent on fossil fuels right now and shows
renewable vs fossil share trends.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import (
    get_generation_coverage,
    list_ba_codes,
    list_fuel_codes,
    load_generation_hourly,
    load_daily_generation,
    load_latest_generation_snapshot,
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

NUMERIC_COLS = ["generation_gwh", "total_gwh", "renewable_pct", "fossil_pct", "gas_pct"]

FUEL_COLORS = {
    "NG":  "#f97316",   # orange — natural gas
    "COL": "#78716c",   # stone — coal
    "NUC": "#a855f7",   # purple — nuclear
    "WND": "#22c55e",   # green — wind
    "SUN": "#eab308",   # yellow — solar
    "WAT": "#3b82f6",   # blue — hydro
}

st.set_page_config(page_title="Generation Mix Monitor", layout="wide")
st.title("Generation Mix Monitor")
st.caption("Track hourly electricity generation by fuel type and identify which regions are leaning on fossil fuels.")

with st.expander("How to read this page"):
    st.markdown("""
- **KPI row** — current snapshot totals and renewable share across all selected BAs.
- **Latest snapshot rankings** — which BAs have the highest fossil or gas share right now.
- **Focus trends** — daily generation mix for one selected BA over the time window.
- **Map** — approximate geographic concentration of fossil vs renewable generation.
""")

if not table_has_rows("fact_generation_hourly"):
    st.warning("No generation rows found yet. Let the pipeline run first.")
    st.stop()

# ── Filters ───────────────────────────────────────────────────────────────────
coverage = get_generation_coverage()
max_period = pd.to_datetime(coverage["max_period"], utc=True)
min_period = pd.to_datetime(coverage["min_period"], utc=True)
default_start, default_end = build_default_date_range(min_period, max_period, lookback_days=7)
all_bas = list_ba_codes("fact_generation_hourly")
all_fuels = list_fuel_codes()

col_a, col_b, col_c, col_d = st.columns([2, 2, 2, 1])
selected_range = col_a.date_input(
    "UTC date range",
    value=(default_start, default_end),
    min_value=min_period.date(),
    max_value=max_period.date(),
)
selected_bas = col_b.multiselect("Balancing authorities", all_bas, default=all_bas)
selected_fuels = col_c.multiselect("Fuel types", all_fuels, default=all_fuels)
display_tz = col_d.selectbox("Timezone", get_timezone_options(), index=0)

if len(selected_range) != 2:
    st.stop()

start_date, end_date = selected_range
start_ts = f"{start_date}T00:00:00+00:00"
end_ts = f"{end_date}T23:59:59+00:00"
ba_filter = selected_bas or None
fuel_filter = selected_fuels or None

# ── Load data ─────────────────────────────────────────────────────────────────
hourly_df = load_generation_hourly(start_ts, end_ts, ba_filter, fuel_filter)
daily_df = load_daily_generation(str(start_date), str(end_date), fuel_filter)
latest_df = load_latest_generation_snapshot(start_ts, end_ts, ba_filter)

if hourly_df.empty:
    st.warning("No generation rows found for the selected filters.")
    st.stop()

hourly_df["period_ts"] = pd.to_datetime(hourly_df["period_ts"], utc=True)
hourly_df["period_display"] = convert_timestamp_series(hourly_df["period_ts"], display_tz)
hourly_df = coerce_numeric(hourly_df, NUMERIC_COLS)

daily_df["report_date"] = pd.to_datetime(daily_df["report_date"])
daily_df = coerce_numeric(daily_df, ["total_gwh"])

latest_df = coerce_numeric(latest_df, ["total_gwh", "renewable_pct", "fossil_pct", "gas_pct"])

# ── KPI row ───────────────────────────────────────────────────────────────────
latest_period = hourly_df["period_ts"].max()
total_gwh = hourly_df["generation_gwh"].sum()
renewable_gwh = hourly_df[hourly_df["fuel_code"].isin(["WND", "SUN", "WAT"])]["generation_gwh"].sum()
fossil_gwh = hourly_df[hourly_df["fuel_code"].isin(["NG", "COL"])]["generation_gwh"].sum()
renewable_share = renewable_gwh / total_gwh * 100 if total_gwh > 0 else 0
fossil_share = fossil_gwh / total_gwh * 100 if total_gwh > 0 else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest period", format_timestamp(latest_period, display_tz))
k2.metric("BAs in scope", f"{hourly_df['ba_code'].nunique():,}")
k3.metric("Total generation (GWh)", f"{total_gwh:,.1f}")
k4.metric("Renewable share", f"{renewable_share:.1f}%")
k5.metric("Fossil share", f"{fossil_share:.1f}%")

# ── Daily generation mix trend (all BAs combined) ─────────────────────────────
st.subheader("Daily generation mix trend")
st.caption("Total GWh per fuel type across all selected balancing authorities.")

if not daily_df.empty:
    st.plotly_chart(
        px.area(
            daily_df,
            x="report_date",
            y="total_gwh",
            color="fuel_code",
            color_discrete_map=FUEL_COLORS,
            labels={"report_date": "Date", "total_gwh": "GWh", "fuel_code": "Fuel"},
            title="Generation mix by fuel type (daily)",
        ),
        use_container_width=True,
    )

# ── Latest snapshot rankings ──────────────────────────────────────────────────
st.subheader("Where is fossil dependence highest right now?")
st.caption("Latest hourly snapshot — ranked by fossil share across balancing authorities.")
rank_col1, rank_col2 = st.columns(2)

fossil_rank = latest_df.dropna(subset=["fossil_pct"]).sort_values("fossil_pct", ascending=False).head(10)
if not fossil_rank.empty:
    rank_col1.plotly_chart(
        px.bar(
            fossil_rank.sort_values("fossil_pct", ascending=True),
            x="fossil_pct",
            y="ba_code",
            orientation="h",
            labels={"fossil_pct": "Fossil share (%)", "ba_code": "BA"},
            title="Highest fossil share (latest hour)",
        ),
        use_container_width=True,
    )

renewable_rank = latest_df.dropna(subset=["renewable_pct"]).sort_values("renewable_pct", ascending=False).head(10)
if not renewable_rank.empty:
    rank_col2.plotly_chart(
        px.bar(
            renewable_rank.sort_values("renewable_pct", ascending=True),
            x="renewable_pct",
            y="ba_code",
            orientation="h",
            color_discrete_sequence=["#22c55e"],
            labels={"renewable_pct": "Renewable share (%)", "ba_code": "BA"},
            title="Highest renewable share (latest hour)",
        ),
        use_container_width=True,
    )

# ── Focus BA trend ────────────────────────────────────────────────────────────
st.subheader("Focus balancing authority — hourly generation mix")
focus_options = sorted(hourly_df["ba_code"].dropna().unique().tolist())
focus_ba = st.selectbox("Focus BA", focus_options, index=0)

focus_df = hourly_df[hourly_df["ba_code"] == focus_ba].copy().sort_values("period_ts")

if not focus_df.empty:
    st.plotly_chart(
        px.area(
            focus_df,
            x="period_display",
            y="generation_gwh",
            color="fuel_code",
            color_discrete_map=FUEL_COLORS,
            labels={
                "period_display": f"Period ({display_tz})",
                "generation_gwh": "GWh",
                "fuel_code": "Fuel",
            },
            title=f"{focus_ba} — hourly generation mix",
        ),
        use_container_width=True,
    )

    # Renewable vs fossil share over time for the focus BA
    share_df = (
        focus_df.groupby("period_display")
        .apply(lambda g: pd.Series({
            "renewable_pct": g[g["fuel_code"].isin(["WND", "SUN", "WAT"])]["generation_gwh"].sum()
                             / g["generation_gwh"].sum() * 100 if g["generation_gwh"].sum() > 0 else None,
            "fossil_pct": g[g["fuel_code"].isin(["NG", "COL"])]["generation_gwh"].sum()
                          / g["generation_gwh"].sum() * 100 if g["generation_gwh"].sum() > 0 else None,
        }))
        .reset_index()
        .melt(id_vars="period_display", var_name="metric", value_name="pct")
        .dropna(subset=["pct"])
    )
    share_df["metric"] = share_df["metric"].replace({
        "renewable_pct": "Renewable %",
        "fossil_pct": "Fossil %",
    })

    st.plotly_chart(
        px.line(
            share_df,
            x="period_display",
            y="pct",
            color="metric",
            color_discrete_map={"Renewable %": "#22c55e", "Fossil %": "#f97316"},
            labels={"period_display": f"Period ({display_tz})", "pct": "Share (%)", "metric": ""},
            title=f"{focus_ba} — renewable vs fossil share trend",
        ),
        use_container_width=True,
    )

# ── Supporting analysis ───────────────────────────────────────────────────────
with st.expander("Supporting analysis", expanded=False):
    map_col, table_col = st.columns(2)

    map_df = latest_df.copy()
    map_df["lat"] = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lat"))
    map_df["lon"] = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("lon"))
    map_df["label"] = map_df["ba_code"].map(lambda c: RESPONDENT_GEO.get(c, {}).get("label", c))
    map_df = map_df.dropna(subset=["lat", "lon", "total_gwh"])

    if not map_df.empty:
        map_col.plotly_chart(
            px.scatter_geo(
                map_df,
                lat="lat", lon="lon",
                color="renewable_pct",
                size="total_gwh",
                hover_name="ba_code",
                hover_data={"ba_name": True, "renewable_pct": ":.1f", "fossil_pct": ":.1f", "total_gwh": ":.2f"},
                color_continuous_scale="RdYlGn",
                scope="usa",
                title="Renewable share by BA (latest hour)",
            ),
            use_container_width=True,
        )
        map_col.caption("Approximate BA coordinates only.")

    st.download_button(
        label="Download focus BA hourly data as CSV",
        data=focus_df[["period_display", "ba_code", "ba_name", "fuel_code", "fuel_name", "generation_gwh"]]
               .to_csv(index=False).encode("utf-8"),
        file_name=f"{focus_ba.lower()}_generation_hourly.csv",
        mime="text/csv",
    )
