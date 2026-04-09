"""Monthly electricity sales trends dashboard."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data_access import (
    gold_table_has_rows,
    load_fuel_mix_price_joined,
    load_sales_coverage,
    load_sales_data,
    load_sales_gold,
    operational_table_has_rows,
    sales_table_has_rows,
)

st.set_page_config(page_title="Monthly Sales Trends", layout="wide", initial_sidebar_state="expanded")

METRIC_LABELS = {
    "sales": "Sales (MWh)",
    "revenue": "Revenue ($)",
    "price": "Avg Price (cents/kWh)",
    "customers": "Customers",
}
METRIC_TICK_FORMAT = {
    "sales": {"ticksuffix": " MWh", "tickprefix": ""},
    "revenue": {"ticksuffix": "", "tickprefix": "$"},
    "price": {"ticksuffix": " c", "tickprefix": ""},
    "customers": {"ticksuffix": "", "tickprefix": ""},
}
SECTOR_COLORS_BY_ABBR = {
    "COM": "#74df84",
    "IND": "#e96379",
    "OTH": "#9c79ef",
    "RES": "#5292b7",
    "TRA": "#f3ad2b",
}


def _load_combined_data(
    start_date: str | None = None,
    end_date: str | None = None,
    sectors: list[str] | None = None,
    states: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    if gold_table_has_rows():
        sales_df = load_sales_gold(start_date=start_date, end_date=end_date, sectors=sectors, states=states)
        return sales_df, sales_df.dropna(subset=["fossil_pct", "renewable_pct"]), "gold"

    sales_df = load_sales_data(start_date=start_date, end_date=end_date, sectors=sectors, states=states)
    if operational_table_has_rows():
        joined_df = load_fuel_mix_price_joined(start_date=start_date, end_date=end_date, states=states)
    else:
        joined_df = pd.DataFrame()
    return sales_df, joined_df, "silver"


def _line_fill(trace) -> None:
    if not trace.line.color or not str(trace.line.color).startswith("#"):
        return
    color = str(trace.line.color)
    r = int(color[1:3], 16)
    g = int(color[3:5], 16)
    b = int(color[5:7], 16)
    trace.update(fill="tozeroy", fillcolor=f"rgba({r},{g},{b},0.15)", line_width=2, marker_size=5)


def _sector_color_map(df: pd.DataFrame) -> dict[str, str]:
    if df.empty or "sector_abbr" not in df.columns or "sector_name" not in df.columns:
        return {}
    pairs = (
        df[["sector_abbr", "sector_name"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["sector_abbr", "sector_name"])
    )
    return {
        row["sector_name"]: SECTOR_COLORS_BY_ABBR.get(row["sector_abbr"], "#0d9488")
        for _, row in pairs.iterrows()
    }


st.title("Monthly Electricity Sales Trends")
st.caption(
    "Track monthly retail sales, revenue, price, and customer counts by state and sector. "
    "When the monthly gold mart is available, fuel-mix overlays come from the joined gold path; "
    "otherwise the page falls back to silver sales plus operational rollups."
)

if not sales_table_has_rows() and not gold_table_has_rows():
    st.warning("No monthly sales data found yet. Run the monthly sales pipeline first.")
    st.stop()

coverage = load_sales_coverage()
min_date = pd.to_datetime(coverage["min_period"])
max_date = pd.to_datetime(coverage["max_period"])

with st.sidebar:
    st.title("Monthly Sales")
    st.caption("Filter the monthly view before rendering the charts.")
    st.divider()

    selected_range = st.date_input(
        "Date range",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    metric = st.selectbox(
        "Primary metric",
        options=["sales", "revenue", "price", "customers"],
        format_func=lambda value: METRIC_LABELS[value],
    )

if len(selected_range) != 2:
    st.info("Select a start and end date to load monthly data.")
    st.stop()

start_date = pd.Timestamp(selected_range[0]).strftime("%Y-%m-%d")
end_date = pd.Timestamp(selected_range[1]).strftime("%Y-%m-%d")

with st.spinner("Loading monthly sales data from Snowflake..."):
    sales_df, joined_df, data_path = _load_combined_data(start_date, end_date)

if sales_df.empty:
    st.warning("No monthly sales data matches the selected filters.")
    st.stop()

sales_df["period"] = pd.to_datetime(sales_df["period"])
sales_df["year"] = sales_df["period"].dt.year
sales_df["month"] = sales_df["period"].dt.month
sales_df["year_month"] = sales_df["period"].dt.strftime("%Y-%m")
sector_color_map = _sector_color_map(sales_df)

all_sectors = sorted(sales_df["sector_name"].dropna().unique().tolist())
all_states = sorted(sales_df["state_id"].dropna().unique().tolist())

with st.sidebar:
    selected_sectors = st.multiselect("Sectors", all_sectors, default=all_sectors)
    selected_states = st.multiselect("States", all_states, default=all_states)
    st.divider()
    st.caption("Data refreshes every hour after the monthly pipeline writes the current partition.")
    with st.expander("How to read this page"):
        st.markdown(
            """
- KPI strip: current business summary for the filtered monthly window.
- Monthly trends: sector movement over time for the selected metric.
- Share views: sector composition and state concentration.
- Seasonal and YoY sections: timing and structural change over time.
- Fuel mix overlays: state-level price versus generation mix when operational data is available.
"""
        )

filtered_df = sales_df[
    sales_df["sector_name"].isin(selected_sectors) & sales_df["state_id"].isin(selected_states)
].copy()

if filtered_df.empty:
    st.warning("No monthly sales data matches the selected filters.")
    st.stop()

metric_label = METRIC_LABELS[metric]
tick_fmt = METRIC_TICK_FORMAT[metric]

st.markdown(
    f"Using **{data_path}** monthly sales path. "
    f"Periods in scope: **{filtered_df['period'].min().date()}** to **{filtered_df['period'].max().date()}**."
)

k1, k2, k3, k4 = st.columns(4)
complete_months = filtered_df.groupby("period")["sales"].sum().sort_index()
complete_months = complete_months.iloc[:-1] if len(complete_months) > 1 else complete_months
mom_delta = None
if len(complete_months) >= 2 and complete_months.iloc[-2] != 0:
    mom_delta = (complete_months.iloc[-1] - complete_months.iloc[-2]) / complete_months.iloc[-2] * 100

k1.metric("Total Sales", f"{filtered_df['sales'].sum():,.0f} MWh", None if mom_delta is None else f"{mom_delta:+.1f}% MoM")
k2.metric("Total Revenue", f"${filtered_df['revenue'].sum():,.0f}")
k3.metric("Avg Retail Price", f"{filtered_df['price'].mean():.2f} c/kWh")
k4.metric("Avg Monthly Customers", f"{filtered_df.groupby('period')['customers'].sum().mean():,.0f}")

st.divider()

st.subheader("Monthly Trends by Sector")
sector_trend = (
    filtered_df.groupby(["period", "sector_name"], as_index=False)
    .agg({metric: "mean" if metric == "price" else "sum"})
    .sort_values("period")
)
fig_sector = px.line(
    sector_trend,
    x="period",
    y=metric,
    color="sector_name",
    markers=True,
    labels={"period": "Month", metric: metric_label, "sector_name": "Sector"},
    title=f"Monthly {metric_label} by sector",
    template="plotly_white",
    color_discrete_map=sector_color_map or None,
)
for trace in fig_sector.data:
    _line_fill(trace)
fig_sector.update_layout(
    legend_title_text="Sector",
    height=390,
    yaxis=dict(tickprefix=tick_fmt["tickprefix"], ticksuffix=tick_fmt["ticksuffix"]),
)
st.plotly_chart(fig_sector, use_container_width=True)

share_left, share_right = st.columns(2)

sector_totals = (
    filtered_df.groupby("sector_name", as_index=False)
    .agg({metric: "mean" if metric == "price" else "sum"})
    .sort_values(metric, ascending=False)
)
fig_pie = px.pie(
    sector_totals,
    names="sector_name",
    values=metric,
    hole=0.42,
    title=f"{metric_label} share by sector",
    template="plotly_white",
    color="sector_name",
    color_discrete_map=sector_color_map or None,
)
fig_pie.update_traces(
    textposition="outside",
    textinfo="percent+label",
    hovertemplate="<b>%{label}</b><br>"
    + tick_fmt["tickprefix"]
    + "%{value:,.1f}"
    + tick_fmt["ticksuffix"]
    + "<extra></extra>",
)
fig_pie.update_layout(showlegend=False, height=380)
share_left.plotly_chart(fig_pie, use_container_width=True)

sector_share = (
    filtered_df.groupby(["period", "sector_name"], as_index=False)
    .agg(sales=("sales", "sum"))
    .sort_values("period")
)
fig_share = px.area(
    sector_share,
    x="period",
    y="sales",
    color="sector_name",
    groupnorm="percent",
    labels={"period": "Month", "sales": "Share (%)", "sector_name": "Sector"},
    title="Sector share of total sales",
    template="plotly_white",
    color_discrete_map=sector_color_map or None,
)
fig_share.update_layout(height=380, yaxis=dict(ticksuffix="%"))
share_right.plotly_chart(fig_share, use_container_width=True)

st.subheader("Annual Sector Share")
st.caption("Annual share of total sales by sector highlights slower structural shifts beyond the monthly swings.")
annual_sector_share = (
    filtered_df.groupby(["year", "sector_name"], as_index=False)
    .agg(sales=("sales", "sum"))
    .sort_values(["year", "sector_name"])
)
annual_sector_share["share_pct"] = (
    annual_sector_share["sales"] / annual_sector_share.groupby("year")["sales"].transform("sum") * 100
)
fig_annual_share = px.bar(
    annual_sector_share,
    x="year",
    y="share_pct",
    color="sector_name",
    barmode="group",
    labels={"year": "Year", "share_pct": "Share (%)", "sector_name": "Sector"},
    title="Annual sector share of total sales",
    template="plotly_white",
    color_discrete_map=sector_color_map or None,
)
fig_annual_share.update_layout(height=380, yaxis=dict(ticksuffix="%"))
st.plotly_chart(fig_annual_share, use_container_width=True)

state_totals = (
    filtered_df.groupby("state_id", as_index=False)
    .agg({metric: "mean" if metric == "price" else "sum"})
    .sort_values("state_id")
)
st.subheader("Total by State")
fig_state = px.bar(
    state_totals,
    x="state_id",
    y=metric,
    color=metric,
    color_continuous_scale="Teal",
    labels={"state_id": "State", metric: metric_label},
    title=f"Total {metric_label} by state",
    template="plotly_white",
)
fig_state.update_layout(
    xaxis=dict(categoryorder="category ascending"),
    xaxis_tickangle=-40,
    coloraxis_showscale=False,
    height=380,
    yaxis=dict(tickprefix=tick_fmt["tickprefix"], ticksuffix=tick_fmt["ticksuffix"]),
)
st.plotly_chart(fig_state, use_container_width=True)

st.subheader("Seasonal Heatmap")
complete_years = filtered_df.groupby("year")["month"].nunique()
complete_years = complete_years[complete_years == 12].index.tolist()
heat_df = filtered_df[filtered_df["year"].isin(complete_years)].copy()
if not heat_df.empty:
    heat_source = (
        heat_df.groupby(["year", "month"], as_index=False)
        .agg({metric: np.mean if metric == "price" else np.sum})
        .pivot(index="month", columns="year", values=metric)
    )
    heat_source.index = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    fig_heat = px.imshow(
        heat_source,
        labels={"x": "Year", "y": "Month", "color": metric_label},
        title=f"Seasonal heatmap - {metric_label}",
        template="plotly_white",
        aspect="auto",
        color_continuous_scale="YlOrRd",
    )
    fig_heat.update_layout(height=380)
    fig_heat.update_traces(
        hovertemplate="Month: %{y}<br>Year: %{x}<br>"
        + tick_fmt["tickprefix"]
        + "%{z:,.1f}"
        + tick_fmt["ticksuffix"]
        + "<extra></extra>"
    )
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.info("Seasonal heatmap requires at least one complete year of monthly data.")

st.subheader("Year-over-Year Growth")
if metric != "price" and len(complete_years) >= 2:
    yoy_source = filtered_df[filtered_df["year"].isin(complete_years)]
    yoy = yoy_source.groupby(["year", "sector_name"], as_index=False).agg({metric: "sum"})
    yoy_pct = (
        yoy.pivot(index="year", columns="sector_name", values=metric)
        .pct_change()
        .mul(100)
        .dropna(how="all")
        .reset_index()
        .melt(id_vars="year", var_name="sector_name", value_name="yoy_pct")
        .dropna(subset=["yoy_pct"])
    )
    if yoy_pct.empty:
        st.info("Year-over-year growth requires at least two complete years of monthly data.")
    else:
        fig_yoy = px.bar(
            yoy_pct,
            x="year",
            y="yoy_pct",
            color="sector_name",
            barmode="group",
            labels={"year": "Year", "yoy_pct": "YoY Change (%)", "sector_name": "Sector"},
            title="Year-over-year growth by sector",
            template="plotly_white",
            color_discrete_map=sector_color_map or None,
        )
        fig_yoy.add_hline(y=0, line_dash="dash", line_color="#6b7280")
        fig_yoy.update_layout(height=360)
        st.plotly_chart(fig_yoy, use_container_width=True)
else:
    st.info("Year-over-year growth is available for sales, revenue, and customers when two complete years are present.")

st.subheader("Price vs Sales Volume")
price_sales = (
    filtered_df.groupby("period", as_index=False)
    .agg(sales=("sales", "sum"), price=("price", "mean"))
    .sort_values("period")
)
fig_dual = go.Figure()
fig_dual.add_trace(
    go.Bar(
        x=price_sales["period"],
        y=price_sales["sales"],
        name="Sales (MWh)",
        marker_color="rgba(13,148,136,0.35)",
    )
)
fig_dual.add_trace(
    go.Scatter(
        x=price_sales["period"],
        y=price_sales["price"],
        name="Avg Price (c/kWh)",
        mode="lines+markers",
        line=dict(color="#f97316", width=2),
        marker=dict(size=4),
        yaxis="y2",
    )
)
fig_dual.update_layout(
    template="plotly_white",
    title="Avg retail price vs total sales volume",
    xaxis_title="Month",
    yaxis=dict(title="Sales (MWh)", ticksuffix=" MWh"),
    yaxis2=dict(title="Avg Price (c/kWh)", ticksuffix=" c", overlaying="y", side="right"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=380,
)
st.plotly_chart(fig_dual, use_container_width=True)

st.subheader("Fuel Mix vs Retail Price by State")
st.caption(
    "Each point is one state in one month. The x-axis shows fossil generation share, while the y-axis "
    "shows average retail price. When the monthly gold mart exists, these overlays come directly from the "
    "joined monthly mart; otherwise they are derived from silver rollups."
)

if joined_df.empty:
    st.info(
        "Fuel mix overlays are not available yet. The sales view still works, but operational monthly data "
        "or the monthly gold mart is missing."
    )
else:
    scatter_y = "price" if data_path == "gold" else "avg_price"
    state_label = "state_description"
    scatter_df = joined_df.dropna(subset=["fossil_pct", scatter_y]).copy()
    if scatter_df.empty:
        st.info("No overlapping fuel mix data is available for the selected period.")
    else:
        scatter_fig = px.scatter(
            scatter_df,
            x="fossil_pct",
            y=scatter_y,
            color="renewable_pct",
            hover_name=state_label,
            hover_data={"period": "|%b %Y", "fossil_pct": ":.1f", "renewable_pct": ":.1f", scatter_y: ":.2f"},
            labels={
                "fossil_pct": "Fossil share (%)",
                scatter_y: "Avg retail price (c/kWh)",
                "renewable_pct": "Renewable share (%)",
            },
            title="Fossil generation share vs retail price",
            template="plotly_white",
            opacity=0.72,
        )
        slope, intercept = np.polyfit(scatter_df["fossil_pct"], scatter_df[scatter_y], 1)
        x_range = pd.Series([scatter_df["fossil_pct"].min(), scatter_df["fossil_pct"].max()])
        scatter_fig.add_trace(
            go.Scatter(
                x=x_range,
                y=slope * x_range + intercept,
                mode="lines",
                line=dict(color="#6b7280", dash="dash"),
                name="Trend",
            )
        )
        scatter_fig.update_layout(height=440)
        st.plotly_chart(scatter_fig, use_container_width=True)

    st.subheader("Retail Price by Renewable Quartile")
    quartile_source = joined_df.dropna(subset=["renewable_pct", scatter_y]).copy()
    if quartile_source.empty:
        st.info("Renewable quartile trend is not available for the selected period.")
    else:
        state_avg = quartile_source.groupby("state_id", as_index=False).agg(avg_renewable_pct=("renewable_pct", "mean"))
        if len(state_avg) < 4:
            st.info("Renewable quartile trend needs at least four states with overlapping fuel-mix data.")
        else:
            state_avg["quartile"] = pd.qcut(
                state_avg["avg_renewable_pct"],
                q=4,
                labels=["Q1 Low renewable", "Q2", "Q3", "Q4 High renewable"],
                duplicates="drop",
            )
            quartile_source = quartile_source.merge(state_avg[["state_id", "quartile"]], on="state_id", how="left")
            quartile_trend = (
                quartile_source.dropna(subset=["quartile"])
                .groupby(["period", "quartile"], as_index=False)
                .agg(avg_price=(scatter_y, "mean"))
                .sort_values("period")
            )
            quartile_fig = px.line(
                quartile_trend,
                x="period",
                y="avg_price",
                color="quartile",
                markers=True,
                labels={"period": "Month", "avg_price": "Avg retail price (c/kWh)", "quartile": "Renewable tier"},
                title="Retail price by renewable generation quartile",
                template="plotly_white",
                color_discrete_sequence=["#ef4444", "#f97316", "#22c55e", "#0d9488"],
            )
            for trace in quartile_fig.data:
                _line_fill(trace)
            quartile_fig.update_layout(
                height=420,
                yaxis=dict(ticksuffix=" c"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(quartile_fig, use_container_width=True)
