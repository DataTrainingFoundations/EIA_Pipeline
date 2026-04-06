"""Monthly electricity sales trends dashboard."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data_access import (
    gold_table_has_rows,
    list_sales_sectors,
    list_sales_states,
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
    "price": {"ticksuffix": " ¢", "tickprefix": ""},
    "customers": {"ticksuffix": "", "tickprefix": ""},
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


st.title("Monthly Electricity Sales Trends")
st.caption(
    "Track monthly electricity sales, revenue, and price trends by state and sector. "
    "When the monthly gold mart is available, fuel-mix overlays are included directly."
)

if not sales_table_has_rows() and not gold_table_has_rows():
    st.warning("No monthly sales data found yet. Run the monthly sales pipeline first.")
    st.stop()

coverage = load_sales_coverage()
min_date = pd.to_datetime(coverage["min_period"])
max_date = pd.to_datetime(coverage["max_period"])
all_sectors = list_sales_sectors()
all_states = list_sales_states()

with st.sidebar:
    st.title("Monthly Sales")
    selected_range = st.date_input(
        "Date range",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )
    selected_sectors = st.multiselect("Sectors", all_sectors, default=all_sectors)
    selected_states = st.multiselect("States", all_states, default=all_states)
    metric = st.selectbox(
        "Primary metric",
        options=["sales", "revenue", "price", "customers"],
        format_func=lambda value: METRIC_LABELS[value],
    )
    with st.expander("How to read this page"):
        st.markdown(
            """
- KPI strip: current business summary for the filtered monthly window.
- Sector trends and share views: how demand and revenue composition shifts over time.
- State totals and seasonal heatmap: where and when the market is strongest.
- Fuel mix overlays: price versus fossil share and renewable quartile trends when operational data is available.
"""
        )

if len(selected_range) != 2:
    st.info("Select a start and end date to load monthly data.")
    st.stop()

start_date = pd.Timestamp(selected_range[0]).strftime("%Y-%m-%d")
end_date = pd.Timestamp(selected_range[1]).strftime("%Y-%m-%d")
selected_sectors = selected_sectors or None
selected_states = selected_states or None

with st.spinner("Loading monthly sales data from Snowflake..."):
    sales_df, joined_df, data_path = _load_combined_data(start_date, end_date, selected_sectors, selected_states)

if sales_df.empty:
    st.warning("No monthly sales data matches the selected filters.")
    st.stop()

sales_df["year"] = sales_df["period"].dt.year
sales_df["month"] = sales_df["period"].dt.month

st.markdown(
    f"Using **{data_path}** monthly sales path. "
    f"Periods in scope: **{sales_df['period'].min().date()}** to **{sales_df['period'].max().date()}**."
)

k1, k2, k3, k4 = st.columns(4)
complete_months = sales_df.groupby("period")["sales"].sum().sort_index()
complete_months = complete_months.iloc[:-1] if len(complete_months) > 1 else complete_months
mom_delta = None
if len(complete_months) >= 2 and complete_months.iloc[-2] != 0:
    mom_delta = (complete_months.iloc[-1] - complete_months.iloc[-2]) / complete_months.iloc[-2] * 100

k1.metric("Total Sales", f"{sales_df['sales'].sum():,.0f} MWh", None if mom_delta is None else f"{mom_delta:+.1f}% MoM")
k2.metric("Total Revenue", f"${sales_df['revenue'].sum():,.0f}")
k3.metric("Avg Retail Price", f"{sales_df['price'].mean():.2f} ¢/kWh")
k4.metric("Avg Monthly Customers", f"{sales_df.groupby('period')['customers'].sum().mean():,.0f}")

st.subheader("Monthly trends by sector")
sector_trend = (
    sales_df.groupby(["period", "sector_name"], as_index=False)
    .agg({metric: "mean" if metric == "price" else "sum"})
    .sort_values("period")
)
fig_sector = px.line(
    sector_trend,
    x="period",
    y=metric,
    color="sector_name",
    markers=True,
    labels={"period": "Month", metric: METRIC_LABELS[metric], "sector_name": "Sector"},
    title=f"Monthly {METRIC_LABELS[metric]} by sector",
    template="plotly_white",
)
st.plotly_chart(fig_sector, use_container_width=True)

share_left, share_right = st.columns(2)

sector_share = (
    sales_df.groupby(["period", "sector_name"], as_index=False)
    .agg(sales=("sales", "sum"))
    .sort_values("period")
)
share_right.plotly_chart(
    px.area(
        sector_share,
        x="period",
        y="sales",
        color="sector_name",
        groupnorm="percent",
        labels={"period": "Month", "sales": "Share (%)", "sector_name": "Sector"},
        title="Sector share of total sales",
        template="plotly_white",
    ),
    use_container_width=True,
)

sector_totals = (
    sales_df.groupby("sector_name", as_index=False)
    .agg({metric: "mean" if metric == "price" else "sum"})
    .sort_values(metric, ascending=False)
)
share_left.plotly_chart(
    px.pie(
        sector_totals,
        names="sector_name",
        values=metric,
        hole=0.45,
        title=f"{METRIC_LABELS[metric]} share by sector",
        template="plotly_white",
    ),
    use_container_width=True,
)

state_totals = (
    sales_df.groupby("state_description", as_index=False)
    .agg({metric: "mean" if metric == "price" else "sum"})
    .sort_values(metric, ascending=False)
)
st.subheader("Total by state")
st.plotly_chart(
    px.bar(
        state_totals,
        x="state_description",
        y=metric,
        color=metric,
        color_continuous_scale="Teal",
        labels={"state_description": "State", metric: METRIC_LABELS[metric]},
        title=f"Total {METRIC_LABELS[metric]} by state",
        template="plotly_white",
    ),
    use_container_width=True,
)

st.subheader("Seasonal heatmap")
complete_years = sales_df.groupby("year")["month"].nunique()
complete_years = complete_years[complete_years == 12].index.tolist()
heat_df = sales_df[sales_df["year"].isin(complete_years)].copy()
if not heat_df.empty:
    heat_source = (
        heat_df.groupby(["year", "month"], as_index=False)
        .agg({metric: np.mean if metric == "price" else np.sum})
        .pivot(index="month", columns="year", values=metric)
    )
    heat_source.index = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    st.plotly_chart(
        px.imshow(
            heat_source,
            labels={"x": "Year", "y": "Month", "color": METRIC_LABELS[metric]},
            title=f"Seasonal heatmap - {METRIC_LABELS[metric]}",
            template="plotly_white",
            aspect="auto",
            color_continuous_scale="YlOrRd",
        ),
        use_container_width=True,
    )
else:
    st.info("Seasonal heatmap requires at least one complete year of monthly data.")

st.subheader("Year-over-year growth")
if metric != "price" and len(complete_years) >= 2:
    yoy_source = sales_df[sales_df["year"].isin(complete_years)]
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
        )
        fig_yoy.add_hline(y=0, line_dash="dash", line_color="#6b7280")
        st.plotly_chart(fig_yoy, use_container_width=True)
else:
    st.info("Year-over-year growth is available for sales, revenue, and customers when two complete years are present.")

st.subheader("Price vs sales volume")
price_sales = (
    sales_df.groupby("period", as_index=False)
    .agg(sales=("sales", "sum"), price=("price", "mean"))
    .sort_values("period")
)
fig_dual = go.Figure()
fig_dual.add_trace(go.Bar(x=price_sales["period"], y=price_sales["sales"], name="Sales (MWh)", marker_color="rgba(13,148,136,0.35)"))
fig_dual.add_trace(
    go.Scatter(
        x=price_sales["period"],
        y=price_sales["price"],
        name="Avg Price (¢/kWh)",
        mode="lines+markers",
        line=dict(color="#f97316", width=2),
        yaxis="y2",
    )
)
fig_dual.update_layout(
    template="plotly_white",
    title="Avg retail price vs total sales volume",
    yaxis=dict(title="Sales (MWh)", ticksuffix=" MWh"),
    yaxis2=dict(title="Avg Price (¢/kWh)", ticksuffix=" ¢", overlaying="y", side="right"),
    legend=dict(orientation="h", y=1.02),
)
st.plotly_chart(fig_dual, use_container_width=True)

st.subheader("Fuel mix overlays")
if joined_df.empty:
    st.info("Fuel mix overlays are not available yet. The sales view still works, but operational monthly data or the monthly gold mart is missing.")
else:
    overlay_left, overlay_right = st.columns(2)
    scatter_y = "price" if data_path == "gold" else "avg_price"
    scatter_fig = px.scatter(
        joined_df.dropna(subset=["fossil_pct", scatter_y]),
        x="fossil_pct",
        y=scatter_y,
        color="renewable_pct",
        hover_name="state_description",
        labels={
            "fossil_pct": "Fossil share (%)",
            scatter_y: "Avg retail price (¢/kWh)",
            "renewable_pct": "Renewable share (%)",
        },
        title="Fossil generation share vs retail price",
        template="plotly_white",
        opacity=0.72,
    )
    if not joined_df.dropna(subset=["fossil_pct", scatter_y]).empty:
        line_source = joined_df.dropna(subset=["fossil_pct", scatter_y])
        slope, intercept = np.polyfit(line_source["fossil_pct"], line_source[scatter_y], 1)
        x_range = pd.Series([line_source["fossil_pct"].min(), line_source["fossil_pct"].max()])
        scatter_fig.add_trace(
            go.Scatter(x=x_range, y=slope * x_range + intercept, mode="lines", line=dict(color="#6b7280", dash="dash"), name="Trend")
        )
    overlay_left.plotly_chart(scatter_fig, use_container_width=True)

    quartile_source = joined_df.dropna(subset=["renewable_pct", scatter_y]).copy()
    quartile_source["state_key"] = quartile_source["state_id"]
    state_avg = quartile_source.groupby("state_key", as_index=False).agg(avg_renewable_pct=("renewable_pct", "mean"))
    if len(state_avg) < 4:
        overlay_right.info("Renewable quartile trend needs at least four states with overlapping fuel-mix data.")
    else:
        state_avg["quartile"] = pd.qcut(
            state_avg["avg_renewable_pct"],
            q=4,
            labels=["Q1 Low renewable", "Q2", "Q3", "Q4 High renewable"],
            duplicates="drop",
        )
        quartile_source = quartile_source.merge(state_avg[["state_key", "quartile"]], on="state_key", how="left")
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
            labels={"period": "Month", "avg_price": "Avg retail price (¢/kWh)", "quartile": "Renewable tier"},
            title="Retail price by renewable generation quartile",
            template="plotly_white",
        )
        overlay_right.plotly_chart(quartile_fig, use_container_width=True)
