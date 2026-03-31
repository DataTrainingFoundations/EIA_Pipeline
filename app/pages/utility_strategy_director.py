"""Utility Strategy Director dashboard built from monthly power operations."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from data_access import (
    get_backfill_status,
    get_power_operations_coverage,
    list_power_locations,
    list_power_sectors,
    load_latest_power_operations_snapshot,
    load_power_operations_monthly,
    table_has_rows,
)
from ui_utils import build_default_date_range, coerce_numeric
from utility_strategy_logic import (
    FUEL_SCORE_COLUMNS,
    aggregate_location_period,
    build_priority_history,
    build_strategy_thresholds,
    derive_strategy_driver,
    derive_strategy_priority,
    is_rollup_fuel,
    map_fuel_bucket,
    normalize_direct,
    normalize_inverse,
    rank_priority_labels,
    trend_label,
)

NUMERIC_COLUMNS = [
    "generation_mwh",
    "generation_share_pct",
    "consumption_for_eg_thousand_units",
    "ash_content_pct",
    "heat_content_btu_per_unit",
    "cost_usd",
    "fuel_heat_input_mmbtu",
    "heat_rate_btu_per_kwh",
]

PRIORITY_COLORS = {
    "Critical": "#E24B4A",
    "Elevated": "#EF9F27",
    "Stable": "#1D9E75",
}

SCORE_LABELS = {
    "score_renewable": "Renewable",
    "score_diversity": "Diversity",
    "score_concentration": "Concentration",
    "score_gas": "Gas dependence",
    "score_coal": "Coal dependence",
    "score_heat_rate": "Heat rate",
}

FUEL_BUCKET_ORDER = ["Renewable", "Gas", "Coal", "Nuclear", "Oil", "Other"]

COLOR_BG = "#0B1220"
COLOR_PANEL = "#0F172A"
COLOR_TEXT = "#F8FAFC"
COLOR_MUTED = "#CBD5E1"
COLOR_GRID = "rgba(248,250,252,0.10)"


def _style_figure(fig: go.Figure, *, height: int | None = None) -> go.Figure:
    fig.update_layout(
        plot_bgcolor=COLOR_PANEL,
        paper_bgcolor=COLOR_PANEL,
        font_color=COLOR_TEXT,
        margin=dict(l=10, r=10, t=55, b=10),
    )
    fig.update_xaxes(showgrid=True, gridcolor=COLOR_GRID, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor=COLOR_GRID, zeroline=False)
    if height is not None:
        fig.update_layout(height=height)
    return fig


def _color_priority(value: str) -> str:
    color = PRIORITY_COLORS.get(value, "")
    if not color:
        return ""
    return f"background-color: {color}; color: #ffffff"


def _format_value(value: float | int | str | pd.Timestamp | None, suffix: str = "", decimals: int = 1) -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return "n/a"
    if isinstance(value, pd.Timestamp):
        return str(value.date())
    if isinstance(value, str):
        return value
    return f"{value:.{decimals}f}{suffix}"


def _metric_delta(delta: float, suffix: str = "") -> str:
    if pd.isna(delta):
        return "n/a"
    return f"{delta:+.2f}{suffix}"


def _best_position(row: pd.Series) -> str:
    scores = pd.to_numeric(row[FUEL_SCORE_COLUMNS], errors="coerce")
    if scores.isna().all():
        return "Insufficient data"
    return SCORE_LABELS[scores.idxmax()]


def _radar_chart(focus_row: pd.Series, portfolio_medians: dict[str, float]) -> go.Figure:
    categories = [SCORE_LABELS[column] for column in FUEL_SCORE_COLUMNS]
    focus_values = [float(focus_row[column]) for column in FUEL_SCORE_COLUMNS]
    median_values = [float(portfolio_medians[column]) for column in FUEL_SCORE_COLUMNS]
    categories = categories + [categories[0]]
    focus_values = focus_values + [focus_values[0]]
    median_values = median_values + [median_values[0]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=median_values,
            theta=categories,
            fill="toself",
            name="Portfolio median",
            line=dict(color="#60A5FA", width=2),
            fillcolor="rgba(96,165,250,0.18)",
        )
    )
    fig.add_trace(
        go.Scatterpolar(
            r=focus_values,
            theta=categories,
            fill="toself",
            name=str(focus_row["location"]),
            line=dict(color="#F59E0B", width=2),
            fillcolor="rgba(245,158,11,0.24)",
        )
    )
    fig.update_layout(
        polar=dict(
            bgcolor=COLOR_PANEL,
            radialaxis=dict(range=[0, 100], showgrid=True, gridcolor=COLOR_GRID),
            angularaxis=dict(gridcolor=COLOR_GRID),
        ),
        plot_bgcolor=COLOR_PANEL,
        paper_bgcolor=COLOR_PANEL,
        font_color=COLOR_TEXT,
        margin=dict(l=10, r=10, t=55, b=10),
        title="Selected location vs portfolio median",
        legend=dict(orientation="h", y=1.08),
    )
    return fig


def _score_breakdown_chart(focus_row: pd.Series) -> go.Figure:
    labels = [SCORE_LABELS[column] for column in FUEL_SCORE_COLUMNS]
    values = [float(focus_row[column]) for column in FUEL_SCORE_COLUMNS]
    fig = px.bar(
        x=labels,
        y=values,
        labels={"x": "", "y": "Score"},
        title="Current strategic score breakdown",
    )
    fig.update_traces(marker_color="#F59E0B")
    fig.update_yaxes(range=[0, 100])
    return fig


st.set_page_config(
    page_title="Utility Strategy Director",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top right, rgba(45,212,191,0.10), transparent 22%),
            linear-gradient(180deg, #0B1220 0%, #111B31 100%);
    }
    .usd-hero {
        background: linear-gradient(135deg, rgba(15,23,42,0.96), rgba(8,47,73,0.96));
        border: 1px solid rgba(148,163,184,0.20);
        border-radius: 18px;
        padding: 1.35rem 1.45rem;
        margin-bottom: 1rem;
        box-shadow: 0 16px 40px rgba(0,0,0,0.18);
    }
    .usd-kicker { color: #7dd3fc; text-transform: uppercase; letter-spacing: 0.12em; font-size: 0.72rem; margin-bottom: 0.45rem; }
    .usd-title { font-size: 2rem; font-weight: 700; color: #f8fafc; margin-bottom: 0.55rem; }
    .usd-copy { color: #cbd5e1; max-width: 58rem; line-height: 1.5; }
    .usd-note { border: 1px solid rgba(148,163,184,0.18); border-radius: 16px; padding: 0.95rem 1rem; background: rgba(15,23,42,0.72); color: #e2e8f0; }
    [data-testid="stMetricValue"] { font-size: 1.3rem; font-weight: 650; }
    [data-testid="stMetricLabel"] { font-size: 0.84rem; color: #cbd5e1; }
    div[data-testid="stExpander"] summary { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="usd-hero">
        <div class="usd-kicker">Power portfolio strategy</div>
        <div class="usd-title">Utility Strategy Director</div>
        <div class="usd-copy">
            This page is a long-horizon strategy view built from monthly electric power operations data.
            The ranked entities are <strong>locations</strong>, not utility respondents. Use it to identify
            structurally exposed power portfolios by fuel mix, concentration, coal and gas dependence,
            and generation efficiency.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if not table_has_rows("platinum.electric_power_operations_monthly"):
    st.warning(
        "No rows found in `platinum.electric_power_operations_monthly`. Let the power monthly "
        "pipeline complete at least one run first."
    )
    st.stop()

coverage = get_power_operations_coverage()
min_period = pd.to_datetime(coverage["min_period"], utc=True)
max_period = pd.to_datetime(coverage["max_period"], utc=True)
default_start_date, default_end_date = build_default_date_range(min_period, max_period, lookback_days=365)
all_locations = list_power_locations()
sector_df = list_power_sectors()
sector_df["sector_id"] = sector_df["sector_id"].astype(str)
sector_df["sector_name"] = sector_df["sector_name"].astype(str)
sector_df["option_label"] = sector_df["sector_name"] + " (" + sector_df["sector_id"] + ")"
default_sector_labels = (
    sector_df[sector_df["sector_name"].str.lower() == "electric utility"]["option_label"].tolist()
    or sector_df["option_label"].tolist()
)

with st.sidebar:
    st.title("Utility Strategy Director")
    st.caption("Filter the board-facing power portfolio strategy view below.")
    st.divider()
    selected_range = st.date_input(
        "Date range",
        value=(default_start_date, default_end_date),
        min_value=min_period.date(),
        max_value=max_period.date(),
    )
    selected_sector_labels = st.multiselect(
        "Sectors",
        sector_df["option_label"].tolist(),
        default=default_sector_labels,
        help="Leave blank to include all sectors.",
    )
    show_us_total = st.checkbox("Show U.S. total", value=True)
    selected_locations = st.multiselect(
        "Locations",
        all_locations,
        default=[],
        help="Leave blank to include all locations.",
    )
    st.divider()
    with st.expander("How to read this page"):
        st.markdown(
            """
- KPI strip: current location-level portfolio pressure at a glance.
- Watchlist: where structural strategy attention is needed first.
- Exposure: where transition gaps and gas dependence are concentrated.
- Composition: how the latest location portfolios are built by fuel bucket.
- Focus: one location against the portfolio median plus trend direction.
- Supporting evidence: detailed tables, heatmap, cost coverage, export, and backfill status.
"""
        )

if len(selected_range) != 2:
    st.info("Select a start and end date to load data.")
    st.stop()

selected_sector_ids = sector_df.loc[
    sector_df["option_label"].isin(selected_sector_labels or sector_df["option_label"].tolist()),
    "sector_id",
].tolist()
selected_locations_filter = (
    None if not selected_locations or set(selected_locations) == set(all_locations) else selected_locations
)
start_date, end_date = selected_range
start_ts = f"{start_date}T00:00:00+00:00"
end_ts = f"{end_date}T23:59:59+00:00"

with st.spinner("Loading monthly power portfolio data..."):
    power_df = load_power_operations_monthly(
        start_ts,
        end_ts,
        selected_locations_filter,
        selected_sector_ids,
    )
    latest_power_df = load_latest_power_operations_snapshot(
        start_ts,
        end_ts,
        selected_locations_filter,
        selected_sector_ids,
    )

if power_df.empty or latest_power_df.empty:
    fallback_sector_ids = sector_df["sector_id"].tolist()
    if selected_sector_ids != fallback_sector_ids:
        st.info("No rows matched the selected sectors. Showing all sectors for the available month instead.")
        with st.spinner("Reloading across all sectors..."):
            power_df = load_power_operations_monthly(
                start_ts,
                end_ts,
                selected_locations_filter,
                fallback_sector_ids,
            )
            latest_power_df = load_latest_power_operations_snapshot(
                start_ts,
                end_ts,
                selected_locations_filter,
                fallback_sector_ids,
            )

if power_df.empty or latest_power_df.empty:
    st.warning("No power operations rows were found for the selected filters.")
    st.stop()

power_df["period"] = pd.to_datetime(power_df["period"], utc=True)
latest_power_df["period"] = pd.to_datetime(latest_power_df["period"], utc=True)
power_df = coerce_numeric(power_df, NUMERIC_COLUMNS)
latest_power_df = coerce_numeric(latest_power_df, NUMERIC_COLUMNS)

location_period_df = aggregate_location_period(power_df)
latest_location_df = aggregate_location_period(latest_power_df)

if not show_us_total:
    location_period_df = location_period_df[location_period_df["location"] != "US"]
    latest_location_df = latest_location_df[latest_location_df["location"] != "US"]

if location_period_df.empty or latest_location_df.empty:
    if all_locations == ["US"]:
        st.info("Only the U.S. total is currently available in the platinum power table, so the page is showing that portfolio view.")
        location_period_df = aggregate_location_period(power_df)
        latest_location_df = aggregate_location_period(latest_power_df)
    else:
        st.warning("The selected filters left no location-level rows to analyze.")
        st.stop()

if latest_location_df["location"].nunique() == 1:
    st.info("Current power platinum coverage contains a single location snapshot. The page stays live, but cross-location benchmarking is limited until more locations are loaded.")
single_location_mode = latest_location_df["location"].nunique() == 1

thresholds = build_strategy_thresholds(latest_location_df)
for frame in (location_period_df, latest_location_df):
    frame["strategy_priority"] = frame.apply(
        derive_strategy_priority,
        axis=1,
        thresholds=thresholds,
    )
    frame["primary_driver"] = frame.apply(derive_strategy_driver, axis=1)

latest_location_df["score_renewable"] = normalize_direct(latest_location_df["renewable_share_pct"])
latest_location_df["score_diversity"] = normalize_direct(latest_location_df["fuel_diversity_index"])
latest_location_df["score_concentration"] = normalize_inverse(latest_location_df["largest_fuel_share_pct"])
latest_location_df["score_gas"] = normalize_inverse(latest_location_df["gas_share_pct"])
latest_location_df["score_coal"] = normalize_inverse(latest_location_df["coal_share_pct"])
latest_location_df["score_heat_rate"] = normalize_inverse(latest_location_df["weighted_heat_rate_btu_per_kwh"])
latest_location_df["strategic_health_score"] = (
    latest_location_df["score_renewable"] * 0.25
    + latest_location_df["score_diversity"] * 0.20
    + latest_location_df["score_concentration"] * 0.20
    + latest_location_df["score_gas"] * 0.15
    + latest_location_df["score_coal"] * 0.10
    + latest_location_df["score_heat_rate"] * 0.10
).round(1)
latest_location_df["transition_gap"] = (100.0 - latest_location_df["strategic_health_score"]).round(1)
latest_location_df["best_position"] = latest_location_df.apply(_best_position, axis=1)
latest_location_df = rank_priority_labels(latest_location_df, "strategy_priority")
latest_location_df = latest_location_df.sort_values(
    ["strategy_priority", "transition_gap", "coal_share_pct", "location"],
    ascending=[True, False, False, True],
    kind="stable",
).reset_index(drop=True)

location_score_map = latest_location_df[
    ["location", "strategic_health_score", "transition_gap"]
].set_index("location")
for column in ["strategic_health_score", "transition_gap"]:
    location_period_df[column] = location_period_df["location"].map(location_score_map[column])

priority_history_df = build_priority_history(location_period_df)
latest_period = pd.to_datetime(latest_location_df["period"].max(), utc=True)
focus_location = st.selectbox("Focus location", latest_location_df["location"].tolist(), index=0)
focus_df = location_period_df[location_period_df["location"] == focus_location].copy().sort_values("period")
focus_row = latest_location_df[latest_location_df["location"] == focus_location].iloc[0]

st.caption(
    f"Showing **{start_date}** -> **{end_date}** | **{latest_location_df['location'].nunique()}** locations | "
    f"Latest loaded month: **{latest_period.date()}**"
)

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Locations in scope", f"{latest_location_df['location'].nunique():,}")
k2.metric("Critical locations", f"{int((latest_location_df['strategy_priority'] == 'Critical').sum()):,}")
k3.metric("Avg renewable share", _format_value(latest_location_df["renewable_share_pct"].mean(), suffix="%"))
k4.metric("Highest coal share", _format_value(latest_location_df["coal_share_pct"].max(), suffix="%"))
k5.metric("Worst heat rate", _format_value(latest_location_df["weighted_heat_rate_btu_per_kwh"].max(), decimals=0))
k6.metric("Latest loaded month", _format_value(latest_period))

st.divider()

st.subheader("Watchlist")
st.caption("Ranked locations that need structural strategy attention first.")

watchlist_display = latest_location_df[
    [
        "location",
        "location_name",
        "strategy_priority",
        "primary_driver",
        "strategic_health_score",
        "transition_gap",
        "renewable_share_pct",
        "gas_share_pct",
        "coal_share_pct",
        "fuel_diversity_index",
        "weighted_heat_rate_btu_per_kwh",
    ]
].rename(
    columns={
        "location": "Location",
        "location_name": "Name",
        "strategy_priority": "Priority",
        "primary_driver": "Primary driver",
        "strategic_health_score": "Strategic health score",
        "transition_gap": "Transition gap",
        "renewable_share_pct": "Renewable share (%)",
        "gas_share_pct": "Gas share (%)",
        "coal_share_pct": "Coal share (%)",
        "fuel_diversity_index": "Fuel diversity",
        "weighted_heat_rate_btu_per_kwh": "Heat rate",
    }
)
st.dataframe(
    watchlist_display.style.map(_color_priority, subset=["Priority"]),
    use_container_width=True,
    hide_index=True,
    height=min(40 + 35 * len(watchlist_display), 430),
)

st.divider()

st.subheader("Where is exposure concentrated?")
exposure_left, exposure_right = st.columns(2)

if single_location_mode:
    sector_snapshot_df = latest_power_df.copy()
    sector_snapshot_df = (
        sector_snapshot_df.groupby(["sector_name"], as_index=False)["generation_mwh"].sum()
        .sort_values("generation_mwh", ascending=False)
    )
    sector_total = sector_snapshot_df["generation_mwh"].sum()
    sector_snapshot_df["generation_share_pct"] = (
        sector_snapshot_df["generation_mwh"] / sector_total * 100.0
    )
    fig_sector = px.bar(
        sector_snapshot_df.head(8).sort_values("generation_share_pct", ascending=True),
        x="generation_share_pct",
        y="sector_name",
        orientation="h",
        labels={"generation_share_pct": "Generation share (%)", "sector_name": ""},
        title="Latest generation mix by sector",
    )
    fig_sector.update_traces(marker_color="#60A5FA")
    exposure_left.plotly_chart(_style_figure(fig_sector, height=430), use_container_width=True)

    sector_fuel_df = latest_power_df.copy()
    sector_fuel_df["fuel_bucket"] = sector_fuel_df.apply(
        lambda row: map_fuel_bucket(row.get("fueltype_id"), row.get("fueltype_name")),
        axis=1,
    )
    sector_fuel_df["is_rollup_fuel"] = sector_fuel_df.apply(
        lambda row: is_rollup_fuel(row.get("fueltype_id"), row.get("fueltype_name")),
        axis=1,
    )
    if (~sector_fuel_df["is_rollup_fuel"]).any():
        sector_fuel_df = sector_fuel_df[~sector_fuel_df["is_rollup_fuel"]]
    sector_fuel_df = (
        sector_fuel_df.groupby(["sector_name", "fuel_bucket"], as_index=False)["generation_mwh"].sum()
    )
    fig_sector_fuel = px.bar(
        sector_fuel_df,
        x="sector_name",
        y="generation_mwh",
        color="fuel_bucket",
        category_orders={"fuel_bucket": FUEL_BUCKET_ORDER},
        labels={
            "sector_name": "Sector",
            "generation_mwh": "Generation (MWh)",
            "fuel_bucket": "Fuel bucket",
        },
        title="Latest sector fuel posture",
    )
    exposure_right.plotly_chart(_style_figure(fig_sector_fuel, height=430), use_container_width=True)
else:
    gap_rank_df = latest_location_df.head(10).sort_values("transition_gap", ascending=True)
    fig_gap = px.bar(
        gap_rank_df,
        x="transition_gap",
        y="location",
        orientation="h",
        color="strategy_priority",
        color_discrete_map=PRIORITY_COLORS,
        labels={"transition_gap": "Transition gap", "location": ""},
        title="Top transition gaps by location",
        hover_data={"primary_driver": True, "coal_share_pct": ":.1f", "gas_share_pct": ":.1f"},
    )
    fig_gap.update_layout(showlegend=False)
    exposure_left.plotly_chart(_style_figure(fig_gap, height=430), use_container_width=True)

    scatter_df = latest_location_df.dropna(
        subset=["renewable_share_pct", "gas_share_pct", "total_generation_mwh"]
    ).copy()
    fig_scatter = px.scatter(
        scatter_df,
        x="renewable_share_pct",
        y="gas_share_pct",
        size="total_generation_mwh",
        color="strategy_priority",
        color_discrete_map=PRIORITY_COLORS,
        hover_name="location",
        hover_data={
            "location_name": True,
            "coal_share_pct": ":.1f",
            "fuel_diversity_index": ":.2f",
            "weighted_heat_rate_btu_per_kwh": ":.0f",
            "transition_gap": ":.1f",
        },
        labels={
            "renewable_share_pct": "Renewable share (%)",
            "gas_share_pct": "Gas share (%)",
            "strategy_priority": "Priority",
        },
        title="Renewable share vs gas dependence",
    )
    exposure_right.plotly_chart(_style_figure(fig_scatter, height=430), use_container_width=True)

st.divider()

st.subheader("Portfolio composition snapshot")
composition_locations = latest_location_df.head(6)["location"].tolist()
composition_df = latest_power_df[latest_power_df["location"].isin(composition_locations)].copy()
composition_df["fuel_bucket"] = composition_df.apply(
    lambda row: map_fuel_bucket(row.get("fueltype_id"), row.get("fueltype_name")),
    axis=1,
)
composition_df["is_rollup_fuel"] = composition_df.apply(
    lambda row: is_rollup_fuel(row.get("fueltype_id"), row.get("fueltype_name")),
    axis=1,
)
if (~composition_df["is_rollup_fuel"]).any():
    composition_df = composition_df[~composition_df["is_rollup_fuel"]]
composition_df = (
    composition_df.groupby(["location", "fuel_bucket"], as_index=False)["generation_mwh"].sum()
)
composition_totals = composition_df.groupby("location")["generation_mwh"].transform("sum")
composition_df["share_pct"] = composition_df["generation_mwh"] / composition_totals * 100.0
fig_mix = px.bar(
    composition_df,
    x="location",
    y="share_pct",
    color="fuel_bucket",
    category_orders={"fuel_bucket": FUEL_BUCKET_ORDER},
    barmode="stack",
    labels={"location": "Location", "share_pct": "Generation share (%)", "fuel_bucket": "Fuel bucket"},
    title="Latest portfolio mix by fuel bucket",
)
st.plotly_chart(_style_figure(fig_mix, height=420), use_container_width=True)

st.divider()

st.subheader(f"Focus location: {focus_location}")
st.caption(
    "Selected location against the latest portfolio median."
    if not single_location_mode
    else "Current location strategy posture for the available platinum snapshot."
)

focus_left, focus_right = st.columns([1.1, 1])
focus_metric_1, focus_metric_2, focus_metric_3, focus_metric_4, focus_metric_5 = focus_right.columns(5)
focus_metric_1.metric("Priority", str(focus_row["strategy_priority"]))
focus_metric_2.metric("Primary driver", str(focus_row["primary_driver"]))
focus_metric_3.metric("Health score", _format_value(focus_row["strategic_health_score"]))
focus_metric_4.metric("Transition gap", _format_value(focus_row["transition_gap"]))
focus_metric_5.metric("Total generation", _format_value(focus_row["total_generation_mwh"], decimals=0))

focus_right.markdown(
    f"""
    <div class="usd-note">
        <strong>{focus_row['location_name'] or focus_row['location']}</strong> is currently
        <strong>{focus_row['strategy_priority']}</strong>. Its main structural drag is
        <strong>{focus_row['primary_driver'].lower()}</strong>. Its strongest relative position is
        <strong>{focus_row['best_position'].lower()}</strong>.
    </div>
    """,
    unsafe_allow_html=True,
)
portfolio_medians = {column: latest_location_df[column].dropna().median() for column in FUEL_SCORE_COLUMNS}
if single_location_mode:
    focus_left.plotly_chart(
        _style_figure(_score_breakdown_chart(focus_row), height=430),
        use_container_width=True,
    )
else:
    focus_left.plotly_chart(_radar_chart(focus_row, portfolio_medians), use_container_width=True)

st.divider()

st.subheader("Momentum")
renewable_label, renewable_delta = trend_label(focus_df["renewable_share_pct"], higher_is_better=True)
gas_label, gas_delta = trend_label(focus_df["gas_share_pct"], higher_is_better=False)
coal_label, coal_delta = trend_label(focus_df["coal_share_pct"], higher_is_better=False)
diversity_label, diversity_delta = trend_label(focus_df["fuel_diversity_index"], higher_is_better=True)
heat_label, heat_delta = trend_label(focus_df["weighted_heat_rate_btu_per_kwh"], higher_is_better=False)

trend_metrics = st.columns(5)
trend_metrics[0].metric("Renewable", renewable_label, _metric_delta(renewable_delta, "%"))
trend_metrics[1].metric("Gas", gas_label, _metric_delta(gas_delta, "%"))
trend_metrics[2].metric("Coal", coal_label, _metric_delta(coal_delta, "%"))
trend_metrics[3].metric("Diversity", diversity_label, _metric_delta(diversity_delta))
trend_metrics[4].metric("Heat rate", heat_label, _metric_delta(heat_delta))

if focus_df["period"].nunique() < 2:
    st.info("Historical trend charts will become useful after more monthly backfill lands. Right now the platinum table only has one month for this location.")
else:
    trend_col1, trend_col2 = st.columns(2)
    trend_col3, trend_col4 = st.columns(2)

    trend_col1.plotly_chart(
        _style_figure(
            px.line(
                focus_df,
                x="period",
                y="renewable_share_pct",
                markers=True,
                labels={"period": "Period", "renewable_share_pct": "Renewable share (%)"},
                title="Renewable share trend",
            ),
            height=320,
        ),
        use_container_width=True,
    )
    trend_col2.plotly_chart(
        _style_figure(
            px.line(
                focus_df,
                x="period",
                y="gas_share_pct",
                markers=True,
                labels={"period": "Period", "gas_share_pct": "Gas share (%)"},
                title="Gas share trend",
            ),
            height=320,
        ),
        use_container_width=True,
    )
    trend_col3.plotly_chart(
        _style_figure(
            px.line(
                focus_df,
                x="period",
                y="coal_share_pct",
                markers=True,
                labels={"period": "Period", "coal_share_pct": "Coal share (%)"},
                title="Coal share trend",
            ),
            height=320,
        ),
        use_container_width=True,
    )
    trend_col4.plotly_chart(
        _style_figure(
            px.line(
                focus_df,
                x="period",
                y="fuel_diversity_index",
                markers=True,
                labels={"period": "Period", "fuel_diversity_index": "Fuel diversity"},
                title="Fuel diversity trend",
            ),
            height=320,
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        _style_figure(
            px.line(
                focus_df,
                x="period",
                y="weighted_heat_rate_btu_per_kwh",
                markers=True,
                labels={"period": "Period", "weighted_heat_rate_btu_per_kwh": "Heat rate"},
                title="Heat rate trend",
            ),
            height=320,
        ),
        use_container_width=True,
    )

st.divider()

with st.expander("Supporting evidence", expanded=False):
    evidence_left, evidence_right = st.columns([1.2, 1])

    detail_display = latest_location_df[
        [
            "location",
            "location_name",
            "strategy_priority",
            "primary_driver",
            "reported_cost_coverage_pct",
            "reported_avg_fuel_cost_usd",
            "renewable_share_pct",
            "gas_share_pct",
            "coal_share_pct",
            "nuclear_share_pct",
            "largest_fuel_share_pct",
            "fuel_diversity_index",
            "weighted_heat_rate_btu_per_kwh",
        ]
    ].rename(
        columns={
            "location": "Location",
            "location_name": "Name",
            "strategy_priority": "Priority",
            "primary_driver": "Primary driver",
            "reported_cost_coverage_pct": "Cost coverage (%)",
            "reported_avg_fuel_cost_usd": "Avg fuel cost",
            "renewable_share_pct": "Renewable share (%)",
            "gas_share_pct": "Gas share (%)",
            "coal_share_pct": "Coal share (%)",
            "nuclear_share_pct": "Nuclear share (%)",
            "largest_fuel_share_pct": "Largest fuel share (%)",
            "fuel_diversity_index": "Fuel diversity",
            "weighted_heat_rate_btu_per_kwh": "Heat rate",
        }
    )
    evidence_left.dataframe(
        detail_display.style.map(_color_priority, subset=["Priority"]),
        use_container_width=True,
        hide_index=True,
        height=420,
    )

    heatmap_df = latest_location_df[
        ["location"] + FUEL_SCORE_COLUMNS
    ].rename(columns={"location": "Location", **SCORE_LABELS})
    fig_heatmap = go.Figure(
        data=go.Heatmap(
            z=heatmap_df[list(SCORE_LABELS.values())].values,
            x=list(SCORE_LABELS.values()),
            y=heatmap_df["Location"],
            colorscale=[[0, "#7f1d1d"], [0.5, "#f59e0b"], [1, "#10b981"]],
            zmin=0,
            zmax=100,
            colorbar=dict(title="Score"),
            hovertemplate="Location: %{y}<br>Metric: %{x}<br>Score: %{z:.1f}<extra></extra>",
        )
    )
    evidence_right.plotly_chart(_style_figure(fig_heatmap, height=420), use_container_width=True)

    cost_detail_df = latest_power_df[latest_power_df["cost_usd"].notna()].copy()
    cost_detail_df["is_rollup_fuel"] = cost_detail_df.apply(
        lambda row: is_rollup_fuel(row.get("fueltype_id"), row.get("fueltype_name")),
        axis=1,
    )
    if (~cost_detail_df["is_rollup_fuel"]).any():
        cost_detail_df = cost_detail_df[~cost_detail_df["is_rollup_fuel"]]
    if not cost_detail_df.empty:
        cost_detail_df["fuel_bucket"] = cost_detail_df.apply(
            lambda row: map_fuel_bucket(row.get("fueltype_id"), row.get("fueltype_name")),
            axis=1,
        )
        st.subheader("Latest fuel cost detail")
        st.dataframe(
            cost_detail_df[
                [
                    "period",
                    "location",
                    "location_name",
                    "sector_name",
                    "fueltype_name",
                    "fuel_bucket",
                    "generation_mwh",
                    "cost_usd",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.download_button(
        label=f"Download {focus_location} strategy history as CSV",
        data=focus_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{focus_location.lower()}_utility_strategy_history.csv",
        mime="text/csv",
    )

    st.subheader("Operational status")
    status_df = get_backfill_status()
    if status_df.empty:
        st.info("No backfill jobs have been queued yet.")
    else:
        st.dataframe(status_df, use_container_width=True, hide_index=True)
