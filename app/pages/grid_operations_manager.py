"""Grid Operations Manager dashboard.

This page reads the grid-operations serving tables from Postgres through
`data_access.py`, derives watchlist priorities in pandas, and renders the
charts used by teammates to triage short-term operational issues.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
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
from ui_utils import build_default_date_range, coerce_numeric, convert_timestamp_series, format_timestamp, get_timezone_options, rank_priority_labels

NUMERIC_COLUMNS = [
    "actual_demand_mwh",
    "day_ahead_forecast_mwh",
    "forecast_error_mwh",
    "forecast_error_zscore",
    "demand_ramp_mwh",
    "demand_ramp_zscore",
    "total_generation_mwh",
    "renewable_share_pct",
    "fossil_share_pct",
    "gas_share_pct",
    "generation_gap_mwh",
    "coverage_ratio",
    "metric_value",
    "threshold_value",
]

PRIORITY_ORDER = ["Critical", "Elevated", "Stable"]


def _derive_priority_label(row: pd.Series) -> str:
    """Classify one respondent into the watchlist priority used on this page."""

    if bool(row.get("has_high_alert")) or pd.to_numeric(row.get("coverage_ratio"), errors="coerce") < 0.8:
        return "Critical"
    if (
        bool(row.get("has_medium_alert"))
        or abs(pd.to_numeric(row.get("forecast_error_zscore"), errors="coerce")) >= 2.5
        or abs(pd.to_numeric(row.get("demand_ramp_zscore"), errors="coerce")) >= 2.5
    ):
        return "Elevated"
    return "Stable"


def _severity_rank(value: str) -> int:
    """Rank alert severity values so high-severity counts are easy to summarize."""

    ranks = {"high": 2, "medium": 1}
    return ranks.get(str(value).lower(), 0)


st.set_page_config(page_title="Grid Operations Manager", layout="wide")
st.title("Grid Operations Manager")
st.caption("Use this page to identify what needs attention now, where it is happening, and what trend explains it.")

with st.expander("How to read this page"):
    st.markdown(
        """
- Start with the KPI row and attention summary to identify respondents needing action now.
- Use the latest-state rankings to see where forecast miss or coverage stress is concentrated.
- Use the focus trends only after a respondent is on the watchlist.
- Approximate maps are supporting context, not primary operating evidence.
"""
    )

if not table_has_rows("platinum.grid_operations_hourly"):
    st.warning("No grid operations mart rows found yet. Let the `platinum_grid_operations_hourly` DAG run first.")
    st.stop()

coverage = get_grid_operations_coverage()
max_period = pd.to_datetime(coverage["max_period"], utc=True)
min_period = pd.to_datetime(coverage["min_period"], utc=True)
default_start_date, default_end_date = build_default_date_range(min_period, max_period, lookback_days=7)
respondents = list_respondents("platinum.grid_operations_hourly")

col_a, col_b, col_c, col_d = st.columns([2, 2, 1, 1])
selected_range = col_a.date_input("UTC date range", value=(default_start_date, default_end_date), min_value=min_period.date(), max_value=max_period.date())
selected_respondents = col_b.multiselect("Respondents", respondents, default=respondents)
display_timezone = col_c.selectbox("Display timezone", get_timezone_options(), index=0)
focus_placeholder = col_d.empty()

if len(selected_range) != 2:
    st.stop()

# Load the exact filtered warehouse slices the page will turn into KPIs, charts,
# and the ranked watchlist shown to teammates.
start_date, end_date = selected_range
start_ts = f"{start_date}T00:00:00+00:00"
end_ts = f"{end_date}T23:59:59+00:00"
filtered_respondents = selected_respondents or None

ops_df = load_grid_operations_hourly(start_ts, end_ts, filtered_respondents)
watchlist_df = load_grid_watchlist(start_ts, end_ts, filtered_respondents)
latest_snapshot_df = load_latest_grid_operations_snapshot(start_ts, end_ts, filtered_respondents)
latest_alerts_df = load_latest_grid_alerts(start_ts, end_ts, filtered_respondents)
alerts_history_df = load_grid_operations_alerts(start_ts, end_ts, filtered_respondents)

if ops_df.empty or watchlist_df.empty:
    st.warning("No grid operations rows found for the selected filters.")
    st.stop()

ops_df["period"] = pd.to_datetime(ops_df["period"], utc=True)
ops_df["period_display"] = convert_timestamp_series(ops_df["period"], display_timezone)
ops_df = coerce_numeric(ops_df, NUMERIC_COLUMNS)

watchlist_df["period"] = pd.to_datetime(watchlist_df["period"], utc=True)
watchlist_df = coerce_numeric(
    watchlist_df,
    ["actual_demand_mwh", "forecast_error_mwh", "forecast_error_zscore", "demand_ramp_zscore", "coverage_ratio", "active_alert_count"],
)
watchlist_df["priority_label"] = watchlist_df.apply(_derive_priority_label, axis=1)
watchlist_df = rank_priority_labels(watchlist_df, "priority_label", PRIORITY_ORDER)
watchlist_df = watchlist_df.sort_values(
    ["priority_label", "has_high_alert", "active_alert_count", "coverage_ratio"],
    ascending=[True, False, False, True],
    kind="stable",
)

latest_snapshot_df["period"] = pd.to_datetime(latest_snapshot_df["period"], utc=True)
latest_snapshot_df["period_display"] = convert_timestamp_series(latest_snapshot_df["period"], display_timezone)
latest_snapshot_df = coerce_numeric(latest_snapshot_df, NUMERIC_COLUMNS)

if not latest_alerts_df.empty:
    latest_alerts_df["period"] = pd.to_datetime(latest_alerts_df["period"], utc=True)
    latest_alerts_df["period_display"] = convert_timestamp_series(latest_alerts_df["period"], display_timezone)
    latest_alerts_df = coerce_numeric(latest_alerts_df, NUMERIC_COLUMNS)

if not alerts_history_df.empty:
    alerts_history_df["period"] = pd.to_datetime(alerts_history_df["period"], utc=True)
    alerts_history_df["period_display"] = convert_timestamp_series(alerts_history_df["period"], display_timezone)
    alerts_history_df["alert_day"] = alerts_history_df["period_display"].dt.date

focus_options = watchlist_df["respondent"].dropna().tolist()
focus_respondent = focus_placeholder.selectbox("Focus respondent", focus_options, index=0)
focus_df = ops_df[ops_df["respondent"] == focus_respondent].copy().sort_values("period")

latest_period = latest_snapshot_df["period"].max()
latest_period_display = format_timestamp(latest_period, display_timezone)
respondents_in_alert = int((watchlist_df["active_alert_count"] > 0).sum())
highest_severity_active_count = int(sum(_severity_rank(severity) == 2 for severity in latest_alerts_df["severity"])) if not latest_alerts_df.empty else 0
lowest_coverage = latest_snapshot_df["coverage_ratio"].dropna().min()

# The KPI row answers "what is happening right now?" before the user reads any
# detailed chart or supporting evidence.
metric1, metric2, metric3, metric4, metric5 = st.columns(5)
metric1.metric("Latest loaded period", latest_period_display)
metric2.metric("Respondents in scope", f"{watchlist_df['respondent'].nunique():,}")
metric3.metric("Respondents in alert", f"{respondents_in_alert:,}")
metric4.metric("High-severity active issues", f"{highest_severity_active_count:,}")
metric5.metric("Lowest coverage ratio", f"{lowest_coverage:.2f}" if pd.notna(lowest_coverage) else "n/a")

st.subheader("What needs attention now?")
st.caption("This watchlist is ranked to surface respondents that appear operationally stressed in the latest snapshot.")
attention_df = watchlist_df[
    [
        "respondent",
        "respondent_name",
        "actual_demand_mwh",
        "forecast_error_mwh",
        "demand_ramp_zscore",
        "coverage_ratio",
        "active_alert_count",
        "priority_label",
    ]
].rename(
    columns={
        "actual_demand_mwh": "latest_actual_demand_mwh",
        "forecast_error_mwh": "latest_forecast_error_mwh",
        "demand_ramp_zscore": "latest_demand_ramp_zscore",
    }
)
st.dataframe(attention_df, use_container_width=True, hide_index=True)

st.subheader("Where is intervention needed?")
st.caption("Use the latest-state rankings to see where current forecast miss and coverage stress are concentrated.")
chart_col1, chart_col2 = st.columns(2)

forecast_rank_df = latest_snapshot_df.dropna(subset=["forecast_error_mwh"]).copy()
forecast_rank_df["abs_forecast_error_mwh"] = forecast_rank_df["forecast_error_mwh"].abs()
forecast_rank_df = forecast_rank_df.sort_values("abs_forecast_error_mwh", ascending=False).head(10)
if forecast_rank_df.empty:
    chart_col1.info("No forecast-backed rows are available in the latest snapshot.")
else:
    chart_col1.plotly_chart(
        px.bar(
            forecast_rank_df.sort_values("abs_forecast_error_mwh", ascending=True),
            x="forecast_error_mwh",
            y="respondent",
            orientation="h",
            labels={"forecast_error_mwh": "Forecast error (MWh)", "respondent": "Respondent"},
            title="Top current forecast misses",
        ),
        use_container_width=True,
    )

coverage_rank_df = latest_snapshot_df.dropna(subset=["coverage_ratio"]).sort_values("coverage_ratio", ascending=True).head(10)
if coverage_rank_df.empty:
    chart_col2.info("No coverage rows are available in the latest snapshot.")
else:
    chart_col2.plotly_chart(
        px.bar(
            coverage_rank_df.sort_values("coverage_ratio", ascending=False),
            x="coverage_ratio",
            y="respondent",
            orientation="h",
            labels={"coverage_ratio": "Coverage ratio", "respondent": "Respondent"},
            title="Lowest current generation coverage",
        ),
        use_container_width=True,
    )

st.subheader(f"What trend explains {focus_respondent}?")
st.caption("These trends provide supporting evidence for the selected respondent over the current time window.")
trend_col1, trend_col2 = st.columns(2)

demand_forecast_df = focus_df[["period_display", "actual_demand_mwh", "day_ahead_forecast_mwh"]].melt(
    id_vars="period_display",
    var_name="series",
    value_name="mwh",
).dropna(subset=["mwh"])
demand_forecast_df["series"] = demand_forecast_df["series"].replace(
    {
        "actual_demand_mwh": "Actual demand",
        "day_ahead_forecast_mwh": "Day-ahead forecast",
    }
)
if demand_forecast_df.empty:
    trend_col1.info("No overlapping actual and forecast series are available for this respondent.")
else:
    trend_col1.plotly_chart(
        px.line(
            demand_forecast_df,
            x="period_display",
            y="mwh",
            color="series",
            labels={"period_display": f"Period ({display_timezone})", "mwh": "MWh", "series": "Series"},
            title="Is this respondent missing forecast materially?",
        ),
        use_container_width=True,
    )

ramp_df = focus_df.dropna(subset=["demand_ramp_mwh", "demand_ramp_zscore"], how="all")
if ramp_df.empty:
    trend_col2.info("No ramp stress rows are available for this respondent.")
else:
    ramp_plot_df = ramp_df[["period_display", "demand_ramp_mwh", "demand_ramp_zscore"]].melt(
        id_vars="period_display",
        var_name="series",
        value_name="value",
    ).dropna(subset=["value"])
    ramp_plot_df["series"] = ramp_plot_df["series"].replace(
        {
            "demand_ramp_mwh": "Ramp magnitude (MWh)",
            "demand_ramp_zscore": "Ramp z-score",
        }
    )
    trend_col2.plotly_chart(
        px.line(
            ramp_plot_df,
            x="period_display",
            y="value",
            color="series",
            labels={"period_display": f"Period ({display_timezone})", "value": "Value", "series": "Series"},
            title="Is ramp stress increasing?",
        ),
        use_container_width=True,
    )

coverage_trend_df = focus_df.dropna(subset=["generation_gap_mwh", "coverage_ratio"], how="all")
if coverage_trend_df.empty:
    st.info("No generation gap or coverage ratio rows are available for this respondent.")
else:
    coverage_plot_df = coverage_trend_df[["period_display", "generation_gap_mwh", "coverage_ratio"]].melt(
        id_vars="period_display",
        var_name="series",
        value_name="value",
    ).dropna(subset=["value"])
    coverage_plot_df["series"] = coverage_plot_df["series"].replace(
        {
            "generation_gap_mwh": "Generation gap (MWh)",
            "coverage_ratio": "Coverage ratio",
        }
    )
    st.plotly_chart(
        px.line(
            coverage_plot_df,
            x="period_display",
            y="value",
            color="series",
            labels={"period_display": f"Period ({display_timezone})", "value": "Value", "series": "Series"},
            title="Is generation support weakening?",
        ),
        use_container_width=True,
    )

st.subheader("What evidence supports action?")
st.caption("Use the latest active alerts table to validate severity, thresholds, and the underlying signal.")
alert_filter_col1, alert_filter_col2 = st.columns(2)
severity_options = ["All"]
alert_type_options = ["All"]
if not latest_alerts_df.empty:
    severity_options += sorted(latest_alerts_df["severity"].dropna().str.title().unique().tolist())
    alert_type_options += sorted(latest_alerts_df["alert_type"].dropna().unique().tolist())
selected_severity = alert_filter_col1.selectbox("Alert severity", severity_options, index=0)
selected_alert_type = alert_filter_col2.selectbox("Alert type", alert_type_options, index=0)

filtered_latest_alerts_df = latest_alerts_df.copy()
if not filtered_latest_alerts_df.empty and selected_severity != "All":
    filtered_latest_alerts_df = filtered_latest_alerts_df[filtered_latest_alerts_df["severity"].str.lower() == selected_severity.lower()]
if not filtered_latest_alerts_df.empty and selected_alert_type != "All":
    filtered_latest_alerts_df = filtered_latest_alerts_df[filtered_latest_alerts_df["alert_type"] == selected_alert_type]

if filtered_latest_alerts_df.empty:
    st.info("No active alerts match the selected filters in the latest snapshot.")
else:
    st.dataframe(
        filtered_latest_alerts_df[
            [
                "period_display",
                "respondent",
                "alert_type",
                "severity",
                "metric_value",
                "threshold_value",
                "message",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

if alerts_history_df.empty:
    st.info("No persisted alert history is available for the selected filters.")
else:
    alert_count_df = alerts_history_df.groupby("alert_day", as_index=False).size().rename(columns={"size": "alert_count"})
    st.plotly_chart(
        px.bar(
            alert_count_df,
            x="alert_day",
            y="alert_count",
            labels={"alert_day": f"Day ({display_timezone})", "alert_count": "Alert count"},
            title="Are alert counts stabilizing or worsening?",
        ),
        use_container_width=True,
    )

with st.expander("Supporting analysis", expanded=False):
    # These charts are secondary context once a respondent is already on the watchlist.
    support_col1, support_col2 = st.columns(2)
    fuel_mix_df = focus_df[["period_display", "renewable_share_pct", "fossil_share_pct", "gas_share_pct"]].melt(
        id_vars="period_display",
        var_name="metric",
        value_name="value",
    ).dropna(subset=["value"])
    fuel_mix_df["metric"] = fuel_mix_df["metric"].replace(
        {
            "renewable_share_pct": "Renewable share %",
            "fossil_share_pct": "Fossil share %",
            "gas_share_pct": "Gas share %",
        }
    )
    if fuel_mix_df.empty:
        support_col1.info("No fuel support mix rows are available for this respondent.")
    else:
        support_col1.plotly_chart(
            px.area(
                fuel_mix_df,
                x="period_display",
                y="value",
                color="metric",
                labels={"period_display": f"Period ({display_timezone})", "value": "Share (%)", "metric": "Fuel share"},
                title="How dependent is current support on fossil and gas generation?",
            ),
            use_container_width=True,
        )

    map_df = latest_snapshot_df.copy()
    map_df["lat"] = map_df["respondent"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lat"))
    map_df["lon"] = map_df["respondent"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("lon"))
    map_df["location_label"] = map_df["respondent"].map(lambda code: RESPONDENT_GEO.get(code, {}).get("label", code))
    map_df = map_df.dropna(subset=["lat", "lon", "actual_demand_mwh"])
    if map_df.empty:
        support_col2.info("No mapped operational rows are available for the latest snapshot.")
    else:
        support_col2.plotly_chart(
            px.scatter_geo(
                map_df,
                lat="lat",
                lon="lon",
                color="coverage_ratio",
                size="actual_demand_mwh",
                hover_name="respondent",
                hover_data={"respondent_name": True, "coverage_ratio": ':.2f', "actual_demand_mwh": ':.0f', "location_label": True},
                scope="usa",
                title="Approximate concentration map",
            ),
            use_container_width=True,
        )
        support_col2.caption("Approximate respondent coordinates only; this map is not primary operating evidence.")

    raw_download_df = focus_df[
        [
            "period_display",
            "respondent",
            "respondent_name",
            "actual_demand_mwh",
            "day_ahead_forecast_mwh",
            "forecast_error_mwh",
            "demand_ramp_mwh",
            "coverage_ratio",
            "generation_gap_mwh",
        ]
    ].copy()
    st.download_button(
        label="Download focus respondent data as CSV",
        data=raw_download_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{focus_respondent.lower()}_grid_operations.csv",
        mime="text/csv",
    )
