"""Utility Strategy Director dashboard.

Roman Urdu:
Ye page Utility Strategy Director ke liye design kiya gaya hai.
Iska focus short-term operations nahin, balki long-term strategic position hai.

Hum is page par ye dekhte hain:
- kaun si utility carbon exposure mein high hai
- kis ki renewable position weak hai
- kis ki gas dependence zyada hai
- kis ki clean coverage weak hai
- kis ki fuel diversity weak hai
- kis utility ko strategic attention sab se pehle chahiye

Ye page Resource Planning Lead se alag hai kyun ke ye future positioning
aur transition readiness par focus karta hai.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from data_access import get_backfill_status, table_has_rows
from data_access_shared import _safe_read_sql
from ui_utils import build_default_date_range, coerce_numeric, safe_quantile

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUMERIC_COLUMNS = [
    "daily_demand_mwh",
    "renewable_share_pct",
    "carbon_intensity_kg_per_mwh",
    "fuel_diversity_index",
    "peak_hour_gas_share_pct",
    "clean_coverage_ratio",
]

PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]

PRIORITY_COLORS = {
    "Critical": "#F59E0B",  # orange
    "Elevated": "#2DD4BF",  # aqua
    "Stable": "#F8FAFC",  # white
}

COLOR_ORANGE = "#F59E0B"
COLOR_WHITE = "#F8FAFC"
COLOR_AQUA = "#2DD4BF"
COLOR_RED = "#F97316"
COLOR_BLUE = "#60A5FA"
COLOR_GRID = "rgba(248,250,252,0.10)"
COLOR_BG = "#0B1220"
COLOR_MUTED = "#CBD5E1"
COLOR_GREEN = "#14B8A6"
COLOR_AMBER = "#FBBF24"


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def load_utility_strategy_daily(
    start_date: str,
    end_date: str,
    respondents: list[str] | None,
) -> pd.DataFrame:
    """Load long-term strategy dataset."""

    query = """
        select
            rp.date,
            rp.respondent,
            rp.respondent_name,
            rp.daily_demand_mwh,
            rp.renewable_share_pct,
            rp.carbon_intensity_kg_per_mwh,
            rp.fuel_diversity_index,
            rp.peak_hour_gas_share_pct,
            rp.clean_coverage_ratio,
            rp.updated_at
        from platinum.resource_planning_daily rp
        where rp.date >= %s
          and rp.date <= %s
          and (%s is null or rp.respondent = any(%s))
        order by rp.date, rp.respondent
    """

    params = [
        start_date,
        end_date,
        respondents if respondents else None,
        respondents if respondents else None,
    ]

    return _safe_read_sql(query, params=params)


@st.cache_data(ttl=300)
def load_latest_utility_snapshot(
    start_date: str,
    end_date: str,
    respondents: list[str] | None,
) -> pd.DataFrame:
    """Load latest strategy snapshot only."""

    query = """
        with filtered as (
            select
                rp.date,
                rp.respondent,
                rp.respondent_name,
                rp.daily_demand_mwh,
                rp.renewable_share_pct,
                rp.carbon_intensity_kg_per_mwh,
                rp.fuel_diversity_index,
                rp.peak_hour_gas_share_pct,
                rp.clean_coverage_ratio,
                rp.updated_at
            from platinum.resource_planning_daily rp
            where rp.date >= %s
              and rp.date <= %s
              and (%s is null or rp.respondent = any(%s))
        ),
        latest_date as (
            select max(date) as date
            from filtered
        )
        select f.*
        from filtered f
        join latest_date ld
          on f.date = ld.date
        order by f.respondent
    """

    params = [
        start_date,
        end_date,
        respondents if respondents else None,
        respondents if respondents else None,
    ]

    return _safe_read_sql(query, params=params)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def style_figure(fig):
    fig.update_layout(
        plot_bgcolor=COLOR_BG,
        paper_bgcolor=COLOR_BG,
        font_color=COLOR_WHITE,
        margin=dict(l=10, r=10, t=55, b=10),
    )
    fig.update_xaxes(showgrid=True, gridcolor=COLOR_GRID)
    fig.update_yaxes(showgrid=True, gridcolor=COLOR_GRID)
    return fig


def _color_strategy_priority(val: str) -> str:
    color = PRIORITY_COLORS.get(val, "")
    if val == "Stable":
        return f"background-color: {color}; color: #0B1220; font-weight: 600;"
    return f"background-color: {color}; color: white; font-weight: 600;"


def _derive_strategy_priority(
    row: pd.Series, thresholds: dict[str, float | None]
) -> str:
    """Classify a respondent into strategy priority."""

    carbon = pd.to_numeric(row.get("carbon_intensity_kg_per_mwh"), errors="coerce")
    renewable = pd.to_numeric(row.get("renewable_share_pct"), errors="coerce")
    gas = pd.to_numeric(row.get("peak_hour_gas_share_pct"), errors="coerce")
    clean = pd.to_numeric(row.get("clean_coverage_ratio"), errors="coerce")
    diversity = pd.to_numeric(row.get("fuel_diversity_index"), errors="coerce")

    critical = (
        (thresholds["carbon_p90"] is not None and carbon >= thresholds["carbon_p90"])
        or (
            thresholds["gas_p90"] is not None
            and gas >= thresholds["gas_p90"]
            and thresholds["renewable_p10"] is not None
            and renewable <= thresholds["renewable_p10"]
        )
        or (
            thresholds["clean_p10"] is not None
            and clean <= thresholds["clean_p10"]
            and thresholds["diversity_p10"] is not None
            and diversity <= thresholds["diversity_p10"]
        )
    )

    if critical:
        return "Critical"

    elevated = (
        (thresholds["carbon_p75"] is not None and carbon >= thresholds["carbon_p75"])
        or (
            thresholds["renewable_p25"] is not None
            and renewable <= thresholds["renewable_p25"]
        )
        or (thresholds["clean_p25"] is not None and clean <= thresholds["clean_p25"])
        or (
            thresholds["diversity_p25"] is not None
            and diversity <= thresholds["diversity_p25"]
        )
    )

    if elevated:
        return "Elevated"

    return "Stable"


def _priority_sort_rank(label: str) -> int:
    mapping = {"Critical": 0, "Elevated": 1, "Stable": 2}
    return mapping.get(label, 99)


def _safe_min(series: pd.Series):
    s = series.dropna()
    return s.min() if not s.empty else None


def _safe_max(series: pd.Series):
    s = series.dropna()
    return s.max() if not s.empty else None


def _normalize_inverse(series: pd.Series) -> pd.Series:
    """
    Low is good, high is bad.
    Example: carbon, gas dependence.
    Final score: higher = better.
    """
    s = pd.to_numeric(series, errors="coerce")
    min_val = s.min()
    max_val = s.max()
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return pd.Series([50] * len(s), index=s.index)
    normalized = ((s - min_val) / (max_val - min_val) * 100).round(1)
    return (100 - normalized).round(1)


def _normalize_direct(series: pd.Series) -> pd.Series:
    """
    High is good, low is bad.
    Example: renewable, clean coverage, diversity.
    Final score: higher = better.
    """
    s = pd.to_numeric(series, errors="coerce")
    min_val = s.min()
    max_val = s.max()
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return pd.Series([50] * len(s), index=s.index)
    return ((s - min_val) / (max_val - min_val) * 100).round(1)


def _trend_delta_label(
    values: pd.Series,
    higher_is_better: bool,
) -> tuple[str, float]:

    s = pd.to_numeric(values, errors="coerce").dropna()

    if len(s) < 2:
        return "Insufficient trend history", np.nan

    delta = float(s.iloc[-1] - s.iloc[0])

    tolerance = max(abs(s.mean()) * 0.01, 0.01)

    if abs(delta) <= tolerance:
        return "Stable", delta

    if higher_is_better:
        return ("Improving", delta) if delta > 0 else ("Worsening", delta)

    return ("Improving", delta) if delta < 0 else ("Worsening", delta)


def _trend_explanation(metric_name: str, label: str) -> str:
    explanations = {
        "carbon": {
            "Improving": "Carbon intensity is moving down, which means the utility is becoming cleaner over time.",
            "Worsening": "Carbon intensity is moving up, which means the utility is becoming more carbon intensive over time.",
            "Stable": "Carbon intensity is relatively flat, which means there is no major change in carbon exposure.",
            "Insufficient trend history": "There is not enough historical data to determine a clear carbon direction yet.",
        },
        "renewable": {
            "Improving": "Renewable share is moving up, which means the utility is increasing clean energy usage.",
            "Worsening": "Renewable share is moving down, which means the utility is losing renewable positioning.",
            "Stable": "Renewable share is relatively flat, which means clean energy adoption is not materially changing.",
            "Insufficient trend history": "There is not enough historical data to determine a clear renewable direction yet.",
        },
        "gas": {
            "Improving": "Peak gas dependence is moving down, which means the utility is becoming less reliant on gas during stress periods.",
            "Worsening": "Peak gas dependence is moving up, which means the utility is becoming more dependent on gas during stress periods.",
            "Stable": "Peak gas dependence is relatively flat, which means there is no major change in gas reliance.",
            "Insufficient trend history": "There is not enough historical data to determine a clear gas dependence direction yet.",
        },
        "clean": {
            "Improving": "Clean coverage is moving up, which means cleaner energy is covering more of demand over time.",
            "Worsening": "Clean coverage is moving down, which means cleaner energy is covering less of demand over time.",
            "Stable": "Clean coverage is relatively flat, which means there is no major change in clean support.",
            "Insufficient trend history": "There is not enough historical data to determine a clear clean coverage direction yet.",
        },
    }
    return explanations.get(metric_name, {}).get(label, "")


def _metric_delta_text(delta: float, suffix: str = "") -> str:
    if pd.isna(delta):
        return "n/a"
    return f"{delta:+.2f}{suffix}"


RISK_LABELS = {
    "score_carbon": "Carbon exposure",
    "score_renewable": "Renewable position",
    "score_gas": "Peak gas reliance",
    "score_clean": "Clean coverage",
    "score_diversity": "Fuel diversity",
}


def _dominant_signal(row: pd.Series, mode: str) -> str:
    score_columns = list(RISK_LABELS.keys())
    numeric_scores = pd.to_numeric(row[score_columns], errors="coerce")
    if numeric_scores.isna().all():
        return "Insufficient data"
    target_column = numeric_scores.idxmin() if mode == "risk" else numeric_scores.idxmax()
    return RISK_LABELS[target_column]


def _brief_card(title: str, value: str, note: str) -> str:
    return f"""
    <div class="usd-brief-card">
        <div class="usd-brief-title">{title}</div>
        <div class="usd-brief-value">{value}</div>
        <div class="usd-brief-note">{note}</div>
    </div>
    """


def _build_radar_figure(focus_row: pd.Series, median_scores: dict[str, float]) -> go.Figure:
    categories = list(RISK_LABELS.values())
    focus_values = [float(focus_row[col]) for col in RISK_LABELS]
    median_values = [float(median_scores[col]) for col in RISK_LABELS]
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
            line=dict(color=COLOR_BLUE, width=2),
            fillcolor="rgba(96,165,250,0.18)",
        )
    )
    fig.add_trace(
        go.Scatterpolar(
            r=focus_values,
            theta=categories,
            fill="toself",
            name=str(focus_row["respondent"]),
            line=dict(color=COLOR_ORANGE, width=2),
            fillcolor="rgba(245,158,11,0.26)",
        )
    )
    fig.update_layout(
        polar=dict(
            bgcolor=COLOR_BG,
            radialaxis=dict(range=[0, 100], showgrid=True, gridcolor=COLOR_GRID),
            angularaxis=dict(gridcolor=COLOR_GRID),
        ),
        plot_bgcolor=COLOR_BG,
        paper_bgcolor=COLOR_BG,
        font_color=COLOR_WHITE,
        margin=dict(l=10, r=10, t=55, b=10),
        title="Selected utility vs portfolio median",
        legend=dict(orientation="h", y=1.1),
    )
    return fig


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Utility Strategy Director",
    page_icon="⚡",
    layout="wide",
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
    .usd-kicker {
        color: #7dd3fc;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-size: 0.72rem;
        margin-bottom: 0.45rem;
    }
    .usd-title {
        font-size: 2rem;
        font-weight: 700;
        color: #f8fafc;
        margin-bottom: 0.55rem;
    }
    .usd-copy {
        color: #cbd5e1;
        max-width: 58rem;
        line-height: 1.5;
    }
    .usd-brief-card {
        border: 1px solid rgba(148,163,184,0.20);
        border-radius: 16px;
        padding: 0.95rem 1rem;
        background: rgba(15,23,42,0.72);
        min-height: 132px;
    }
    .usd-brief-title {
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 0.72rem;
        margin-bottom: 0.5rem;
    }
    .usd-brief-value {
        color: #f8fafc;
        font-size: 1.7rem;
        font-weight: 700;
        margin-bottom: 0.35rem;
    }
    .usd-brief-note {
        color: #cbd5e1;
        font-size: 0.9rem;
        line-height: 1.4;
    }
    .usd-focus-note {
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 16px;
        padding: 0.95rem 1rem;
        background: rgba(15,23,42,0.72);
        color: #e2e8f0;
    }
    [data-testid="stMetricValue"] { font-size: 1.35rem; font-weight: 650; }
    [data-testid="stMetricLabel"] { font-size: 0.84rem; color: #cbd5e1; }
    div[data-testid="stExpander"] summary { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown(
    """
    <div class="usd-hero">
        <div class="usd-kicker">Transition readiness brief</div>
        <div class="usd-title">Utility Strategy Director</div>
        <div class="usd-copy">
            This page is intentionally different from the operational and planning views. It is a
            board-facing read on long-term transition posture: who is structurally exposed, who is
            improving, and where portfolio resilience is still thin.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("What this page tells you", expanded=True):
    st.markdown("""
- **Which utilities need strategic attention first**
- **Which utilities are more exposed to future transition risk**
- **Which utilities are weaker on renewable position or clean coverage**
- **Which utilities rely more heavily on gas at peak demand**
- **Whether a selected utility is improving or worsening over time**
""")

# ---------------------------------------------------------------------------
# Guard checks
# ---------------------------------------------------------------------------

if not table_has_rows("platinum.resource_planning_daily"):
    st.warning(
        "No rows found in platinum.resource_planning_daily. "
        "Please allow the planning pipeline to complete at least one run."
    )
    st.stop()

coverage_query = """
    select
        min(date) as min_date,
        max(date) as max_date,
        count(*) as row_count,
        count(distinct respondent) as respondent_count
    from platinum.resource_planning_daily
"""

coverage_df = _safe_read_sql(coverage_query)

if coverage_df.empty:
    st.warning("Planning coverage query returned no rows.")
    st.stop()

coverage = coverage_df.iloc[0]
min_date = pd.to_datetime(coverage["min_date"], errors="coerce")
max_date = pd.to_datetime(coverage["max_date"], errors="coerce")

if pd.isna(min_date) or pd.isna(max_date):
    st.warning("Valid planning coverage dates are not available.")
    st.stop()

default_start_date, default_end_date = build_default_date_range(
    min_date,
    max_date,
    lookback_days=30,
)

respondent_query = """
    select distinct respondent
    from platinum.resource_planning_daily
    where respondent is not null
    order by respondent
"""
respondent_df = _safe_read_sql(respondent_query)
respondents = respondent_df["respondent"].dropna().tolist()

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

filter_col1, filter_col2 = st.columns([1, 1])

selected_range = filter_col1.date_input(
    "Date range",
    value=(default_start_date, default_end_date),
    min_value=min_date.date(),
    max_value=max_date.date(),
)

selected_respondents = filter_col2.multiselect(
    "Respondents",
    respondents,
    default=respondents,
)

if not isinstance(selected_range, tuple) or len(selected_range) != 2:
    st.info("Please select a valid start and end date.")
    st.stop()

start_date, end_date = selected_range
filtered_respondents = selected_respondents or None

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

strategy_df = load_utility_strategy_daily(
    str(start_date),
    str(end_date),
    filtered_respondents,
)

latest_snapshot_df = load_latest_utility_snapshot(
    str(start_date),
    str(end_date),
    filtered_respondents,
)

if strategy_df.empty:
    st.warning("No strategy data was found for the selected filters.")
    st.stop()

if latest_snapshot_df.empty:
    st.warning("The latest strategy snapshot is empty for the selected filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Clean data
# ---------------------------------------------------------------------------

strategy_df["date"] = pd.to_datetime(strategy_df["date"], errors="coerce")
latest_snapshot_df["date"] = pd.to_datetime(latest_snapshot_df["date"], errors="coerce")

strategy_df = coerce_numeric(strategy_df, NUMERIC_COLUMNS)
latest_snapshot_df = coerce_numeric(latest_snapshot_df, NUMERIC_COLUMNS)

thresholds = {
    "carbon_p90": safe_quantile(
        latest_snapshot_df["carbon_intensity_kg_per_mwh"], 0.90
    ),
    "gas_p90": safe_quantile(latest_snapshot_df["peak_hour_gas_share_pct"], 0.90),
    "renewable_p10": safe_quantile(latest_snapshot_df["renewable_share_pct"], 0.10),
    "carbon_p75": safe_quantile(
        latest_snapshot_df["carbon_intensity_kg_per_mwh"], 0.75
    ),
    "renewable_p25": safe_quantile(latest_snapshot_df["renewable_share_pct"], 0.25),
    "clean_p25": safe_quantile(latest_snapshot_df["clean_coverage_ratio"], 0.25),
    "clean_p10": safe_quantile(latest_snapshot_df["clean_coverage_ratio"], 0.10),
    "diversity_p10": safe_quantile(latest_snapshot_df["fuel_diversity_index"], 0.10),
    "diversity_p25": safe_quantile(latest_snapshot_df["fuel_diversity_index"], 0.25),
}

latest_snapshot_df["strategy_priority"] = latest_snapshot_df.apply(
    _derive_strategy_priority,
    axis=1,
    thresholds=thresholds,
)

latest_snapshot_df["priority_rank"] = latest_snapshot_df["strategy_priority"].map(
    _priority_sort_rank
)

latest_snapshot_df["score_carbon"] = _normalize_inverse(
    latest_snapshot_df["carbon_intensity_kg_per_mwh"]
)
latest_snapshot_df["score_renewable"] = _normalize_direct(
    latest_snapshot_df["renewable_share_pct"]
)
latest_snapshot_df["score_gas"] = _normalize_inverse(
    latest_snapshot_df["peak_hour_gas_share_pct"]
)
latest_snapshot_df["score_clean"] = _normalize_direct(
    latest_snapshot_df["clean_coverage_ratio"]
)
latest_snapshot_df["score_diversity"] = _normalize_direct(
    latest_snapshot_df["fuel_diversity_index"]
)

latest_snapshot_df["strategic_health_score"] = (
    latest_snapshot_df["score_carbon"] * 0.25
    + latest_snapshot_df["score_renewable"] * 0.25
    + latest_snapshot_df["score_gas"] * 0.20
    + latest_snapshot_df["score_clean"] * 0.20
    + latest_snapshot_df["score_diversity"] * 0.10
).round(1)
latest_snapshot_df["transition_gap"] = (
    100 - latest_snapshot_df["strategic_health_score"]
).round(1)
latest_snapshot_df["dominant_risk"] = latest_snapshot_df.apply(
    _dominant_signal,
    axis=1,
    mode="risk",
)
latest_snapshot_df["best_position"] = latest_snapshot_df.apply(
    _dominant_signal,
    axis=1,
    mode="strength",
)

latest_snapshot_df = latest_snapshot_df.sort_values(
    ["priority_rank", "transition_gap", "carbon_intensity_kg_per_mwh"],
    ascending=[True, False, False],
    kind="stable",
)

latest_date = latest_snapshot_df["date"].max()

focus_options = latest_snapshot_df["respondent"].dropna().tolist()

if not focus_options:
    st.warning("No respondents are available for focus analysis.")
    st.stop()

focus_respondent = st.selectbox("Focus utility", focus_options, index=0)

focus_df = (
    strategy_df[strategy_df["respondent"] == focus_respondent]
    .copy()
    .sort_values("date")
)

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

kpi1.metric(
    "Latest strategic snapshot",
    str(latest_date.date()) if pd.notna(latest_date) else "n/a",
)
kpi2.metric("Utilities in scope", f"{latest_snapshot_df['respondent'].nunique():,}")

max_carbon = _safe_max(latest_snapshot_df["carbon_intensity_kg_per_mwh"])
min_renewable = _safe_min(latest_snapshot_df["renewable_share_pct"])
avg_score = latest_snapshot_df["strategic_health_score"].dropna().mean()

kpi3.metric(
    "Highest carbon intensity", f"{max_carbon:.1f}" if max_carbon is not None else "n/a"
)
kpi4.metric(
    "Lowest renewable share",
    f"{min_renewable:.1f}%" if min_renewable is not None else "n/a",
)
kpi5.metric(
    "Average strategic health", f"{avg_score:.1f}" if pd.notna(avg_score) else "n/a"
)

st.caption(
    "This page focuses on long-term positioning rather than short-term operations. "
    "It helps identify which utilities are strategically stronger and which need attention."
)

st.divider()

# ---------------------------------------------------------------------------
# Strategic Watchlist
# ---------------------------------------------------------------------------

st.subheader("Strategic Risk Watchlist")

st.caption(
    "This watchlist highlights utilities that may need the most long-term attention, "
    "based on carbon exposure, renewable position, gas dependence, clean coverage, and fuel diversity."
)

watchlist_display = latest_snapshot_df[
    [
        "respondent",
        "respondent_name",
        "strategy_priority",
        "dominant_risk",
        "transition_gap",
        "strategic_health_score",
        "renewable_share_pct",
        "carbon_intensity_kg_per_mwh",
        "peak_hour_gas_share_pct",
        "clean_coverage_ratio",
        "fuel_diversity_index",
    ]
].rename(
    columns={
        "respondent": "Utility",
        "respondent_name": "Name",
        "strategy_priority": "Priority",
        "dominant_risk": "Largest drag",
        "transition_gap": "Transition gap",
        "strategic_health_score": "Strategic health score",
        "renewable_share_pct": "Renewable share (%)",
        "carbon_intensity_kg_per_mwh": "Carbon intensity (kg/MWh)",
        "peak_hour_gas_share_pct": "Peak gas dependence (%)",
        "clean_coverage_ratio": "Clean coverage ratio",
        "fuel_diversity_index": "Fuel diversity",
    }
)

st.dataframe(
    watchlist_display.style.map(_color_strategy_priority, subset=["Priority"]),
    use_container_width=True,
    hide_index=True,
)

brief_col1, brief_col2, brief_col3, brief_col4 = st.columns(4)
brief_col1.markdown(
    _brief_card(
        "Critical utilities",
        f"{int((latest_snapshot_df['strategy_priority'] == 'Critical').sum()):,}",
        "Utilities already carrying multiple structural weaknesses.",
    ),
    unsafe_allow_html=True,
)
brief_col2.markdown(
    _brief_card(
        "Elevated utilities",
        f"{int((latest_snapshot_df['strategy_priority'] == 'Elevated').sum()):,}",
        "Utilities that need active transition management before they worsen.",
    ),
    unsafe_allow_html=True,
)
brief_col3.markdown(
    _brief_card(
        "Largest transition gap",
        f"{latest_snapshot_df['transition_gap'].dropna().max():.1f}"
        if latest_snapshot_df["transition_gap"].notna().any()
        else "n/a",
        "Higher means the utility is farther from a strong portfolio position.",
    ),
    unsafe_allow_html=True,
)
brief_col4.markdown(
    _brief_card(
        "Average health score",
        f"{latest_snapshot_df['strategic_health_score'].dropna().mean():.1f}"
        if latest_snapshot_df["strategic_health_score"].notna().any()
        else "n/a",
        "Higher means stronger long-term transition posture.",
    ),
    unsafe_allow_html=True,
)

st.divider()

# ---------------------------------------------------------------------------
# Strategic visuals
# ---------------------------------------------------------------------------

st.subheader("Strategic Positioning Overview")

viz_col1, viz_col2 = st.columns([1.45, 1])

scatter_df = latest_snapshot_df.dropna(
    subset=["renewable_share_pct", "carbon_intensity_kg_per_mwh", "daily_demand_mwh"]
).copy()

if scatter_df.empty:
    viz_col1.info("No positioning data is available.")
else:
    fig_scatter = px.scatter(
        scatter_df,
        x="renewable_share_pct",
        y="carbon_intensity_kg_per_mwh",
        size="daily_demand_mwh",
        color="strategy_priority",
        color_discrete_map=PRIORITY_COLORS,
        hover_name="respondent",
        hover_data={
            "respondent_name": True,
            "renewable_share_pct": ":.1f",
            "carbon_intensity_kg_per_mwh": ":.1f",
            "peak_hour_gas_share_pct": ":.1f",
            "clean_coverage_ratio": ":.2f",
            "daily_demand_mwh": ":,.0f",
        },
        labels={
            "renewable_share_pct": "Renewable share (%)",
            "carbon_intensity_kg_per_mwh": "Carbon intensity (kg/MWh)",
            "strategy_priority": "Priority",
        },
        title="Risk Positioning: Renewable Share vs Carbon Intensity",
    )

    renewable_ref = latest_snapshot_df["renewable_share_pct"].median()
    carbon_ref = latest_snapshot_df["carbon_intensity_kg_per_mwh"].median()

    if pd.notna(renewable_ref):
        fig_scatter.add_vline(
            x=renewable_ref,
            line_dash="dash",
            line_color=COLOR_MUTED,
            opacity=0.6,
        )
    if pd.notna(carbon_ref):
        fig_scatter.add_hline(
            y=carbon_ref,
            line_dash="dash",
            line_color=COLOR_MUTED,
            opacity=0.6,
        )

    fig_scatter = style_figure(fig_scatter)
    viz_col1.plotly_chart(fig_scatter, use_container_width=True)

gap_rank_df = (
    latest_snapshot_df.dropna(subset=["transition_gap"])
    .head(8)
    .sort_values("transition_gap", ascending=True)
)

if gap_rank_df.empty:
    viz_col2.info("No transition-gap ranking data is available.")
else:
    fig_gap = px.bar(
        gap_rank_df,
        x="transition_gap",
        y="respondent",
        orientation="h",
        color="strategy_priority",
        color_discrete_map=PRIORITY_COLORS,
        labels={"transition_gap": "Transition gap", "respondent": ""},
        title="Largest current transition gaps",
        hover_data={"dominant_risk": True, "best_position": True},
    )
    fig_gap.update_layout(showlegend=False)
    fig_gap = style_figure(fig_gap)
    viz_col2.plotly_chart(fig_gap, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Dynamic Heatmap
# ---------------------------------------------------------------------------

st.subheader("Strategic Strength and Risk Heatmap")

st.caption(
    "Green indicates strong positioning, red indicates weaker positioning. "
    "The color scale is dynamically adjusted for clarity."
)

heatmap_source = latest_snapshot_df[
    [
        "respondent",
        "strategy_priority",
        "strategic_health_score",
        "score_carbon",
        "score_renewable",
        "score_gas",
        "score_clean",
        "score_diversity",
    ]
].copy()

heatmap_source = heatmap_source.rename(
    columns={
        "respondent": "Utility",
        "score_carbon": "Carbon position",
        "score_renewable": "Renewable position",
        "score_gas": "Gas resilience",
        "score_clean": "Clean coverage",
        "score_diversity": "Fuel diversity",
    }
)

heatmap_matrix = heatmap_source.set_index("Utility")[
    [
        "Carbon position",
        "Renewable position",
        "Gas resilience",
        "Clean coverage",
        "Fuel diversity",
    ]
]

heatmap_values = heatmap_matrix.to_numpy(dtype=float)
valid_values = heatmap_values[~np.isnan(heatmap_values)]

if valid_values.size == 0:
    st.info("No heatmap data is available for the selected filters.")
else:
    zmin = float(np.nanpercentile(valid_values, 5))
    zmax = float(np.nanpercentile(valid_values, 95))

    if zmin == zmax:
        zmin = max(0.0, zmin - 1.0)
        zmax = min(100.0, zmax + 1.0)

    flat_cols = [
        col
        for col in heatmap_matrix.columns
        if heatmap_matrix[col].nunique(dropna=True) <= 1
    ]

    if flat_cols:
        st.warning(f"No variation detected in: {', '.join(flat_cols)}")

    fig_heatmap = go.Figure(
        data=go.Heatmap(
            z=heatmap_matrix.values,
            x=heatmap_matrix.columns,
            y=heatmap_matrix.index,
            colorscale=[
                [0, "#7f1d1d"],
                [0.5, "#f59e0b"],
                [1, "#10b981"],
            ],
            zmin=zmin,
            zmax=zmax,
            colorbar=dict(title="Score"),
            hovertemplate=(
                "Utility: %{y}<br>" "Metric: %{x}<br>" "Score: %{z:.1f}<extra></extra>"
            ),
        )
    )

    fig_heatmap.update_layout(
        plot_bgcolor=COLOR_BG,
        paper_bgcolor=COLOR_BG,
        font_color=COLOR_WHITE,
        margin=dict(l=10, r=10, t=55, b=10),
    )

    st.plotly_chart(fig_heatmap, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Focus respondent summary
# ---------------------------------------------------------------------------

st.subheader(f"Board Packet: {focus_respondent}")

focus_latest = latest_snapshot_df[
    latest_snapshot_df["respondent"] == focus_respondent
].copy()

if focus_latest.empty:
    st.info("No latest summary is available for the selected utility.")
else:
    focus_row = focus_latest.iloc[0]

    score_medians = {
        key: latest_snapshot_df[key].dropna().median()
        for key in RISK_LABELS
    }
    weakest_area = _dominant_signal(focus_row, "risk")
    strongest_area = _dominant_signal(focus_row, "strength")

    summary_left, summary_right = st.columns([1.1, 1])
    summary_left.plotly_chart(
        _build_radar_figure(focus_row, score_medians),
        use_container_width=True,
    )

    summary_right.markdown(
        f"""
        <div class="usd-focus-note">
            <strong>{focus_row['respondent_name'] or focus_row['respondent']}</strong> is currently
            classified as <strong>{focus_row['strategy_priority']}</strong>. Its biggest structural drag
            is <strong>{weakest_area.lower()}</strong>, while its strongest relative position is
            <strong>{strongest_area.lower()}</strong>.
        </div>
        """,
        unsafe_allow_html=True,
    )

    summary_col1, summary_col2, summary_col3, summary_col4 = summary_right.columns(4)
    summary_col1.metric("Priority", str(focus_row["strategy_priority"]))
    summary_col2.metric("Health score", f"{focus_row['strategic_health_score']:.1f}")
    summary_col3.metric("Transition gap", f"{focus_row['transition_gap']:.1f}")
    summary_col4.metric("Renewable share", f"{focus_row['renewable_share_pct']:.1f}%")

    st.caption(
        "This summary shows the selected utility’s current long-term position based on the latest strategic snapshot."
    )

st.divider()

# ---------------------------------------------------------------------------
# Trend section
# ---------------------------------------------------------------------------

st.subheader(f"How {focus_respondent} is moving over time")

st.caption(
    "These trends show whether the selected utility is moving in a stronger or weaker strategic direction."
)

carbon_df = focus_df.dropna(subset=["carbon_intensity_kg_per_mwh"]).copy()
renewable_df = focus_df.dropna(subset=["renewable_share_pct"]).copy()
gas_df = focus_df.dropna(subset=["peak_hour_gas_share_pct"]).copy()
clean_df = focus_df.dropna(subset=["clean_coverage_ratio"]).copy()

carbon_label, carbon_delta = _trend_delta_label(
    (
        carbon_df["carbon_intensity_kg_per_mwh"]
        if not carbon_df.empty
        else pd.Series(dtype=float)
    ),
    higher_is_better=False,
)

renewable_label, renewable_delta = _trend_delta_label(
    (
        renewable_df["renewable_share_pct"]
        if not renewable_df.empty
        else pd.Series(dtype=float)
    ),
    higher_is_better=True,
)

gas_label, gas_delta = _trend_delta_label(
    gas_df["peak_hour_gas_share_pct"] if not gas_df.empty else pd.Series(dtype=float),
    higher_is_better=False,
)

clean_label, clean_delta = _trend_delta_label(
    clean_df["clean_coverage_ratio"] if not clean_df.empty else pd.Series(dtype=float),
    higher_is_better=True,
)

trend_status_col1, trend_status_col2, trend_status_col3, trend_status_col4 = st.columns(
    4
)
trend_status_col1.metric(
    "Carbon direction", carbon_label, _metric_delta_text(carbon_delta)
)
trend_status_col2.metric(
    "Renewable direction", renewable_label, _metric_delta_text(renewable_delta, "%")
)
trend_status_col3.metric("Gas direction", gas_label, _metric_delta_text(gas_delta, "%"))
trend_status_col4.metric(
    "Clean direction", clean_label, _metric_delta_text(clean_delta)
)

trend_col1, trend_col2 = st.columns(2)

if carbon_df.empty:
    trend_col1.info("No carbon data is available for this utility.")
else:
    fig_carbon_trend = px.line(
        carbon_df,
        x="date",
        y="carbon_intensity_kg_per_mwh",
        markers=True,
        color_discrete_sequence=[COLOR_ORANGE],
        labels={
            "date": "Date",
            "carbon_intensity_kg_per_mwh": "Carbon intensity (kg/MWh)",
        },
        title=f"Carbon Intensity Trend ({carbon_label})",
    )
    fig_carbon_trend.update_traces(line=dict(width=3), marker=dict(size=8))
    fig_carbon_trend = style_figure(fig_carbon_trend)
    trend_col1.plotly_chart(fig_carbon_trend, use_container_width=True)
    trend_col1.caption(_trend_explanation("carbon", carbon_label))

if renewable_df.empty:
    trend_col2.info("No renewable data is available for this utility.")
else:
    fig_renewable_trend = px.line(
        renewable_df,
        x="date",
        y="renewable_share_pct",
        markers=True,
        color_discrete_sequence=[COLOR_AQUA],
        labels={"date": "Date", "renewable_share_pct": "Renewable share (%)"},
        title=f"Renewable Share Trend ({renewable_label})",
    )
    fig_renewable_trend.update_traces(line=dict(width=3), marker=dict(size=8))
    fig_renewable_trend = style_figure(fig_renewable_trend)
    trend_col2.plotly_chart(fig_renewable_trend, use_container_width=True)
    trend_col2.caption(_trend_explanation("renewable", renewable_label))

trend_col3, trend_col4 = st.columns(2)

if gas_df.empty:
    trend_col3.info("No gas dependence data is available for this utility.")
else:
    fig_gas_trend = px.line(
        gas_df,
        x="date",
        y="peak_hour_gas_share_pct",
        markers=True,
        color_discrete_sequence=[COLOR_ORANGE],
        labels={"date": "Date", "peak_hour_gas_share_pct": "Peak gas dependence (%)"},
        title=f"Peak Gas Dependence Trend ({gas_label})",
    )
    fig_gas_trend.update_traces(line=dict(width=3), marker=dict(size=8))
    fig_gas_trend = style_figure(fig_gas_trend)
    trend_col3.plotly_chart(fig_gas_trend, use_container_width=True)
    trend_col3.caption(_trend_explanation("gas", gas_label))

if clean_df.empty:
    trend_col4.info("No clean coverage data is available for this utility.")
else:
    fig_clean_trend = px.line(
        clean_df,
        x="date",
        y="clean_coverage_ratio",
        markers=True,
        color_discrete_sequence=[COLOR_AQUA],
        labels={"date": "Date", "clean_coverage_ratio": "Clean coverage ratio"},
        title=f"Clean Coverage Trend ({clean_label})",
    )
    fig_clean_trend.update_traces(line=dict(width=3), marker=dict(size=8))
    fig_clean_trend = style_figure(fig_clean_trend)
    trend_col4.plotly_chart(fig_clean_trend, use_container_width=True)
    trend_col4.caption(_trend_explanation("clean", clean_label))

st.divider()

# ---------------------------------------------------------------------------
# Client-friendly explanation block
# ---------------------------------------------------------------------------

with st.expander("How to explain this page to a client", expanded=False):
    st.markdown("""
This page helps show which utilities are stronger or weaker from a long-term strategic perspective.

It looks at:
- carbon exposure
- renewable adoption
- gas dependence during peak demand
- clean coverage
- portfolio diversity

The goal is to help leadership quickly see:
- where future risk may be building
- which utilities need attention first
- which utilities are improving
- and which ones may need stronger transition planning
""")

with st.expander("How to explain trend charts to a client", expanded=False):
    st.markdown("""
### Carbon Intensity Trend
- If the line goes **up**, the utility is becoming more carbon intensive.
- If the line goes **down**, the utility is becoming cleaner.
- If the line is **flat**, carbon exposure is not materially changing.

### Renewable Share Trend
- If the line goes **up**, renewable usage is improving.
- If the line goes **down**, renewable positioning is getting weaker.
- If the line is **flat**, clean energy adoption is not materially changing.

### Peak Gas Dependence Trend
- If the line goes **up**, the utility is becoming more dependent on gas during stress periods.
- If the line goes **down**, gas dependence is improving.
- If the line is **flat**, gas reliance is not materially changing.

### Clean Coverage Trend
- If the line goes **up**, clean energy is covering more demand.
- If the line goes **down**, clean energy support is weakening.
- If the line is **flat**, clean coverage is not materially changing.

### Best overall story
A strong utility is not just one that looks good today — it is one that is moving in the right direction over time.
""")

# ---------------------------------------------------------------------------
# Operational status
# ---------------------------------------------------------------------------

st.subheader("Operational Status")

with st.expander("Backfill Status", expanded=False):
    status_df = get_backfill_status()
    if status_df.empty:
        st.info("No backfill jobs have been queued yet.")
    else:
        st.dataframe(status_df, use_container_width=True, hide_index=True)
