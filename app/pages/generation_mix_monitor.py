"""Generation mix monitoring dashboard."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import (
    get_generation_coverage,
    list_ba_codes,
    list_fuel_codes,
    load_daily_generation,
    load_generation_hourly,
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
)

NUMERIC_COLS = ["generation_gwh", "total_gwh", "renewable_pct", "fossil_pct", "gas_pct"]
RENEWABLE_FUELS = {"WND", "SUN", "WAT"}
FOSSIL_FUELS = {"NG", "COL"}
FUEL_COLORS = {
    "NG": "#c76d1f",
    "COL": "#57534e",
    "NUC": "#6d28d9",
    "WND": "#16a34a",
    "SUN": "#ca8a04",
    "WAT": "#2563eb",
}


def _share_frame(df: pd.DataFrame) -> pd.DataFrame:
    totals = df.groupby("period_display", dropna=False)["generation_gwh"].sum().rename("total_gwh")
    renewable = (
        df[df["fuel_code"].isin(RENEWABLE_FUELS)]
        .groupby("period_display", dropna=False)["generation_gwh"]
        .sum()
        .rename("renewable_gwh")
    )
    fossil = (
        df[df["fuel_code"].isin(FOSSIL_FUELS)]
        .groupby("period_display", dropna=False)["generation_gwh"]
        .sum()
        .rename("fossil_gwh")
    )
    grouped = pd.concat([totals, renewable, fossil], axis=1).reset_index()
    grouped["renewable_pct"] = grouped["renewable_gwh"] / grouped["total_gwh"] * 100
    grouped["fossil_pct"] = grouped["fossil_gwh"] / grouped["total_gwh"] * 100
    grouped = grouped.melt(id_vars="period_display", var_name="metric", value_name="pct").dropna(subset=["pct"])
    grouped["metric"] = grouped["metric"].replace(
        {"renewable_pct": "Renewable share", "fossil_pct": "Fossil share"}
    )
    return grouped[grouped["metric"].isin(["Renewable share", "Fossil share"])]


st.set_page_config(
    page_title="Generation Mix Monitor",
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

if not table_has_rows("FACT_GENERATION_HOURLY"):
    st.warning("No generation rows found yet. Let the pipeline run first.")
    st.stop()

coverage = get_generation_coverage()
max_period = pd.to_datetime(coverage["max_period"], utc=True)
min_period = pd.to_datetime(coverage["min_period"], utc=True)
default_start, default_end = build_default_date_range(min_period, max_period, lookback_days=7)
all_bas = list_ba_codes("FACT_GENERATION_HOURLY")
all_fuels = list_fuel_codes()

with st.sidebar:
    st.title("Generation Mix")
    st.caption("Focus the dashboard by date, balancing authority, and fuel mix.")
    selected_range = st.date_input(
        "UTC date range",
        value=(default_start, default_end),
        min_value=min_period.date(),
        max_value=max_period.date(),
    )
    selected_bas = st.multiselect("Balancing authorities", all_bas, default=all_bas)
    selected_fuels = st.multiselect("Fuel types", all_fuels, default=all_fuels)
    display_tz = st.selectbox("Display timezone", get_timezone_options(), index=0)
    with st.expander("How to read this page"):
        st.markdown(
            """
- KPI strip: latest system generation and mix share.
- Daily mix trend: fuel composition across the selected window.
- Snapshot rankings: who is most fossil-heavy or renewable-heavy right now.
- Focus BA: hourly mix and share trend for one balancing authority.
- Supporting analysis: national map and CSV export.
"""
        )

if len(selected_range) != 2:
    st.info("Select a start and end date to load generation data.")
    st.stop()

start_date, end_date = selected_range
start_ts = f"{start_date}T00:00:00+00:00"
end_ts = f"{end_date}T23:59:59+00:00"
ba_filter = selected_bas or None
fuel_filter = selected_fuels or None

with st.spinner("Loading generation data from Snowflake..."):
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

latest_period = hourly_df["period_ts"].max()
window_hours = hourly_df["period_ts"].nunique()
total_gwh = hourly_df["generation_gwh"].sum()
renewable_gwh = hourly_df[hourly_df["fuel_code"].isin(RENEWABLE_FUELS)]["generation_gwh"].sum()
fossil_gwh = hourly_df[hourly_df["fuel_code"].isin(FOSSIL_FUELS)]["generation_gwh"].sum()
renewable_share = renewable_gwh / total_gwh * 100 if total_gwh > 0 else 0
fossil_share = fossil_gwh / total_gwh * 100 if total_gwh > 0 else 0
gas_leaders = latest_df["gas_pct"].ge(50).sum() if "gas_pct" in latest_df else 0

st.title("Generation Mix Monitor")
st.markdown(
    (
        f"<div class='page-note'>Window: <strong>{start_date}</strong> to "
        f"<strong>{end_date}</strong> | Latest period: "
        f"<strong>{format_timestamp(latest_period, display_tz)}</strong> | "
        f"BAs: <strong>{hourly_df['ba_code'].nunique()}</strong> | Hourly points: "
        f"<strong>{window_hours}</strong></div>"
    ),
    unsafe_allow_html=True,
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest generation (GWh)", f"{total_gwh:,.1f}")
k2.metric("Renewable share", f"{renewable_share:.1f}%")
k3.metric("Fossil share", f"{fossil_share:.1f}%")
k4.metric("Gas-heavy BAs", f"{int(gas_leaders):,}")
k5.metric("Fuels in scope", f"{hourly_df['fuel_code'].nunique():,}")

st.subheader("Daily generation mix trend")
st.caption("Fuel composition across the selected date window.")
if not daily_df.empty:
    st.plotly_chart(
        px.area(
            daily_df,
            x="report_date",
            y="total_gwh",
            color="fuel_code",
            color_discrete_map=FUEL_COLORS,
            labels={"report_date": "Date", "total_gwh": "GWh", "fuel_code": "Fuel"},
            title="Daily generation by fuel type",
        ),
        use_container_width=True,
    )
else:
    st.info("No daily aggregate rows for the selected filters.")

st.subheader("Latest snapshot rankings")
st.caption("Latest hourly snapshot ranked by fossil and renewable share.")
rank_left, rank_right = st.columns(2)

fossil_rank = latest_df.dropna(subset=["fossil_pct"]).sort_values("fossil_pct", ascending=False).head(10)
renewable_rank = (
    latest_df.dropna(subset=["renewable_pct"]).sort_values("renewable_pct", ascending=False).head(10)
)

if not fossil_rank.empty:
    rank_left.plotly_chart(
        px.bar(
            fossil_rank.sort_values("fossil_pct", ascending=True),
            x="fossil_pct",
            y="ba_code",
            orientation="h",
            color_discrete_sequence=["#c76d1f"],
            labels={"fossil_pct": "Fossil share (%)", "ba_code": "BA"},
            title="Highest fossil share",
        ),
        use_container_width=True,
    )
else:
    rank_left.info("No fossil-share snapshot rows are available.")

if not renewable_rank.empty:
    rank_right.plotly_chart(
        px.bar(
            renewable_rank.sort_values("renewable_pct", ascending=True),
            x="renewable_pct",
            y="ba_code",
            orientation="h",
            color_discrete_sequence=["#16a34a"],
            labels={"renewable_pct": "Renewable share (%)", "ba_code": "BA"},
            title="Highest renewable share",
        ),
        use_container_width=True,
    )
else:
    rank_right.info("No renewable-share snapshot rows are available.")

focus_options = sorted(hourly_df["ba_code"].dropna().unique().tolist())
focus_ba = st.selectbox("Focus balancing authority", focus_options, index=0)
focus_df = hourly_df[hourly_df["ba_code"] == focus_ba].copy().sort_values("period_ts")

st.subheader(f"Focus BA: {focus_ba}")
focus_left, focus_right = st.columns(2)
if not focus_df.empty:
    focus_left.plotly_chart(
        px.area(
            focus_df,
            x="period_display",
            y="generation_gwh",
            color="fuel_code",
            color_discrete_map=FUEL_COLORS,
            labels={"period_display": f"Period ({display_tz})", "generation_gwh": "GWh", "fuel_code": "Fuel"},
            title="Hourly generation mix",
        ),
        use_container_width=True,
    )

    share_df = _share_frame(focus_df)
    if not share_df.empty:
        focus_right.plotly_chart(
            px.line(
                share_df,
                x="period_display",
                y="pct",
                color="metric",
                color_discrete_map={"Renewable share": "#16a34a", "Fossil share": "#c76d1f"},
                labels={"period_display": f"Period ({display_tz})", "pct": "Share (%)", "metric": ""},
                title="Renewable vs fossil share",
            ),
            use_container_width=True,
        )
    else:
        focus_right.info("No share trend rows for the selected BA.")

with st.expander("Supporting analysis", expanded=False):
    support_left, support_right = st.columns(2)

    map_df = latest_df.copy()
    map_df["lat"] = map_df["ba_code"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lat"))
    map_df["lon"] = map_df["ba_code"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lon"))
    map_df = map_df.dropna(subset=["lat", "lon", "total_gwh"])
    if not map_df.empty:
        support_left.plotly_chart(
            px.scatter_geo(
                map_df,
                lat="lat",
                lon="lon",
                color="renewable_pct",
                size="total_gwh",
                size_max=50,
                hover_name="ba_code",
                hover_data={
                    "ba_name": True,
                    "renewable_pct": ":.1f",
                    "fossil_pct": ":.1f",
                    "total_gwh": ":.2f",
                },
                color_continuous_scale="RdYlGn",
                scope="usa",
                title="Renewable share by BA",
            ),
            use_container_width=True,
        )
        support_left.caption("Approximate BA coordinates only.")
    else:
        support_left.info("No mappable rows in the latest snapshot.")

    gas_rank = latest_df.dropna(subset=["gas_pct"]).sort_values("gas_pct", ascending=False).head(15)
    if not gas_rank.empty:
        support_right.plotly_chart(
            px.scatter(
                gas_rank,
                x="total_gwh",
                y="gas_pct",
                size="fossil_pct",
                color="renewable_pct",
                text="ba_code",
                color_continuous_scale="Viridis",
                labels={
                    "total_gwh": "Total generation (GWh)",
                    "gas_pct": "Gas share (%)",
                    "renewable_pct": "Renewable share (%)",
                    "fossil_pct": "Fossil share (%)",
                },
                title="Gas exposure vs total generation",
            ),
            use_container_width=True,
        )
    else:
        support_right.info("No gas share snapshot rows are available.")

    if not focus_df.empty:
        st.download_button(
            label=f"Download {focus_ba} generation data as CSV",
            data=focus_df[
                [
                    "period_display",
                    "ba_code",
                    "ba_name",
                    "fuel_code",
                    "fuel_name",
                    "generation_gwh",
                ]
            ].to_csv(index=False).encode("utf-8"),
            file_name=f"{focus_ba.lower()}_generation_hourly_{start_date}_{end_date}.csv",
            mime="text/csv",
        )
