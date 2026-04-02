"""Monthly Sales Trends — EIA Analytics dashboard page (Gold table version)."""
# pylint: disable=import-error
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from data_access_sales import load_sales_coverage

from data_access_operational_sales import (
    load_sales_gold,
    gold_table_has_rows,
)

st.set_page_config(page_title="Monthly Sales Trends · EIA Analytics (Gold)", layout="wide")

# ── Page header ───────────────────────────────────────────────────────────────
st.title("Monthly Electricity Sales Trends (Gold Table)")
st.caption(
    "Retail electricity sales by sector, state, and customer class. "
    "Data sourced from EIA retail sales reporting (Gold layer)."
)

# ── Early exit guard ──────────────────────────────────────────────────────────
if not gold_table_has_rows():
    st.warning("No Gold sales data found. Check the pipeline.")
    st.stop()

# ── Data loading ──────────────────────────────────────────────────────────────
with st.spinner("Loading Gold sales data…"):
    df = load_sales_gold()

if df.empty:
    st.warning("No Gold sales data available. Check the data pipeline.")
    st.stop()

# Derived fields
df["YEAR"] = df["PERIOD"].dt.year
df["MONTH"] = df["PERIOD"].dt.month
df["YEAR_MONTH"] = df["PERIOD"].dt.strftime("%Y-%m")

# ── Sidebar Filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    coverage = load_sales_coverage()  # unchanged
    min_date = coverage["min_period"].to_pydatetime()
    max_date = coverage["max_period"].to_pydatetime()

    date_range = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    all_sectors = sorted(df["SECTORNAME"].unique())
    selected_sectors = st.multiselect(
        "Sectors",
        options=all_sectors,
        default=all_sectors,
    )

    all_states = sorted(df["STATEDESCRIPTION"].unique())
    selected_states = st.multiselect(
        "States",
        options=all_states,
        default=all_states,
    )

    metric = st.selectbox(
        "Primary metric",
        options=["SALES", "REVENUE", "PRICE", "CUSTOMERS"],
        format_func=lambda m: {
            "SALES": "Sales (MWh)",
            "REVENUE": "Revenue (million $)",
            "PRICE": "Avg Price (cents/kWh)",
            "CUSTOMERS": "Customers",
        }[m],
    )

    st.divider()
    st.caption("Data refreshes every hour at :15 past.")

# ── Apply filters ─────────────────────────────────────────────────────────────
if len(date_range) == 2:
    start_date = pd.Timestamp(date_range[0])
    end_date = pd.Timestamp(date_range[1])
else:
    start_date = df["PERIOD"].min()
    end_date = df["PERIOD"].max()

mask = (
    df["PERIOD"].between(start_date, end_date)
    & df["SECTORNAME"].isin(selected_sectors)
    & df["STATEDESCRIPTION"].isin(selected_states)
)
fdf = df[mask].copy()

if fdf.empty:
    st.warning("No data matches the selected filters. Adjust the sidebar.")
    st.stop()

# Metric labels and tick formatting
METRIC_LABELS = {
    "SALES": "Sales (MWh)",
    "REVENUE": "Revenue (million $)",
    "PRICE": "Avg Price (cents/kWh)",
    "CUSTOMERS": "Customers",
}
metric_label = METRIC_LABELS[metric]

METRIC_TICK_FORMAT = {
    "SALES":     {"ticksuffix": " MWh", "tickprefix": ""},
    "REVENUE":   {"ticksuffix": " M",   "tickprefix": "$"},
    "PRICE":     {"ticksuffix": " ¢",   "tickprefix": ""},
    "CUSTOMERS": {"ticksuffix": "",      "tickprefix": ""},
}
tick_fmt = METRIC_TICK_FORMAT[metric]

# ── KPI row ───────────────────────────────────────────────────────────────────
st.subheader("Summary")
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

total_sales = fdf["SALES"].sum()
total_revenue = fdf["REVENUE"].sum()
avg_price = fdf["PRICE"].mean()
total_customers = fdf.groupby("PERIOD")["CUSTOMERS"].sum().mean()

complete_months = fdf.groupby("PERIOD")["SALES"].sum().sort_index().iloc[:-1]
mom_delta = (complete_months.iloc[-1] - complete_months.iloc[-2]) / complete_months.iloc[-2] * 100 \
    if len(complete_months) >= 2 else None
delta_str = f"{mom_delta:+.1f}% MoM" if mom_delta is not None else None

kpi1.metric("Total Sales", f"{total_sales:,.0f} M kWh", delta=delta_str)
kpi2.metric("Total Revenue", f"${total_revenue:,.0f} M")
kpi3.metric("Avg Retail Price", f"{avg_price:.2f} ¢/kWh")
kpi4.metric("Avg Monthly Customers", f"{total_customers:,.0f}")

st.divider()

# ── Monthly Trends by Sector ─────────────────────────────────────────────────
st.subheader("Monthly Trends by Sector")
if metric != "PRICE":
    monthly_sector = fdf.groupby(["PERIOD", "SECTORNAME"])[metric].sum().reset_index()
else:
    monthly_sector = fdf.groupby(["PERIOD", "SECTORNAME"])[metric].mean().reset_index()

fig_line = px.line(
    monthly_sector,
    x="PERIOD",
    y=metric,
    color="SECTORNAME",
    markers=True,
    title=f"Monthly {metric_label} by Sector",
    labels={"PERIOD": "Month", metric: metric_label, "SECTORNAME": "Sector"},
    template="plotly_white",
)
# Fill under lines
for trace in fig_line.data:
    r, g, b = int(trace.line.color[1:3], 16), int(trace.line.color[3:5], 16), int(trace.line.color[5:7], 16)
    trace.update(fill="tozeroy", fillcolor=f"rgba({r},{g},{b},0.15)", line_width=2, marker_size=5)
fig_line.update_layout(legend_title_text="Sector", height=380,
                       yaxis=dict(tickprefix=tick_fmt["tickprefix"], ticksuffix=tick_fmt["ticksuffix"]))
st.plotly_chart(fig_line, use_container_width=True)

# ── Market Share Pie Chart ───────────────────────────────────────────────────
st.subheader(f"{metric_label} Share by Sector")
agg_fn = "mean" if metric == "PRICE" else "sum"
sector_totals = fdf.groupby("SECTORNAME")[metric].agg(agg_fn).reset_index()
fig_pie = px.pie(
    sector_totals,
    names="SECTORNAME",
    values=metric,
    title=f"{metric_label} Share",
    template="plotly_white",
    hole=0.42,
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_pie.update_traces(
    textposition="outside",
    textinfo="percent+label",
    hovertemplate="<b>%{label}</b><br>" + tick_fmt["tickprefix"] + "%{value:,.1f}" + tick_fmt["ticksuffix"] + "<extra></extra>"
)
fig_pie.update_layout(showlegend=False, height=380)
st.plotly_chart(fig_pie, use_container_width=True)

# ── Total by State ──────────────────────────────────────────────────────────
st.subheader("Total by State")
state_totals = fdf.groupby("STATEDESCRIPTION")[metric].agg(agg_fn).reset_index().sort_values("STATEDESCRIPTION")
fig_bar = px.bar(
    state_totals,
    x="STATEDESCRIPTION",
    y=metric,
    title=f"Total {metric_label} by State",
    labels={"STATEDESCRIPTION": "State", metric: metric_label},
    template="plotly_white",
    color=metric,
    color_continuous_scale="Teal",
)
fig_bar.update_layout(xaxis=dict(categoryorder="category ascending"), xaxis_tickangle=-40,
                      coloraxis_showscale=False, height=380,
                      yaxis=dict(tickprefix=tick_fmt["tickprefix"], ticksuffix=tick_fmt["ticksuffix"]))
st.plotly_chart(fig_bar, use_container_width=True)

# ── Seasonal Heatmap ────────────────────────────────────────────────────────
st.subheader("Seasonal Heatmap")
heat_agg_fn = np.mean if metric == "PRICE" else np.sum
complete_years = fdf.groupby("YEAR")["MONTH"].nunique().pipe(lambda s: s[s == 12]).index
heat_fdf = fdf[fdf["YEAR"].isin(complete_years)]
heat_data = heat_fdf.groupby(["YEAR","MONTH"])[metric].agg(heat_agg_fn).reset_index().pivot(index="MONTH", columns="YEAR", values=metric)
month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
heat_data.index = [month_names[m-1] for m in heat_data.index]
fig_heat = px.imshow(
    heat_data,
    title=f"Seasonal Heatmap — {metric_label}",
    labels={"x": "Year", "y": "Month", "color": metric_label},
    template="plotly_white",
    color_continuous_scale="YlOrRd",
    aspect="auto",
)
fig_heat.update_layout(height=380)
fig_heat.update_traces(
    hovertemplate="Month: %{y}<br>Year: %{x}<br>" + tick_fmt["tickprefix"] + "%{z:,.1f}" + tick_fmt["ticksuffix"] + "<extra></extra>"
)
st.plotly_chart(fig_heat, use_container_width=True)

# ── Year-over-Year Growth ───────────────────────────────────────────────────
st.subheader("Year-over-Year Growth")
if metric != "PRICE":
    yoy_fdf = fdf[fdf["YEAR"].isin(complete_years)]
    yoy = yoy_fdf.groupby(["YEAR", "SECTORNAME"])[metric].sum().reset_index()
    yoy_pivot = yoy.pivot(index="YEAR", columns="SECTORNAME", values=metric)
    yoy_pct = (yoy_pivot.pct_change()*100).dropna(how="all").reset_index().melt(id_vars="YEAR", var_name="SECTORNAME", value_name="YoY_pct").dropna(subset=["YoY_pct"])
    if yoy_pct.empty:
        st.info("YoY growth requires at least 2 complete years of data.")
    else:
        fig_yoy = px.bar(
            yoy_pct, x="YEAR", y="YoY_pct", color="SECTORNAME",
            barmode="group",
            title="Year-over-Year Growth (%) by Sector",
            labels={"YEAR": "Year", "YoY_pct": "YoY Change (%)", "SECTORNAME": "Sector"},
            template="plotly_white",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_yoy.add_hline(y=0, line_dash="dash", line_color="grey", line_width=1)
        fig_yoy.update_layout(height=360)
        st.plotly_chart(fig_yoy, use_container_width=True)
else:
    st.info("YoY growth chart is shown for volume/revenue metrics. Switch the primary metric.")

# ── Price vs Sales Volume ────────────────────────────────────────────────────
st.subheader("Price vs Sales Volume Over Time")
price_sales = fdf.groupby("PERIOD").agg(SALES=("SALES","sum"), PRICE=("PRICE","mean")).reset_index().sort_values("PERIOD")
fig_dual = go.Figure()
fig_dual.add_trace(go.Bar(x=price_sales["PERIOD"], y=price_sales["SALES"], name="Sales (MWh)", marker_color="rgba(13,148,136,0.4)", yaxis="y1"))
fig_dual.add_trace(go.Scatter(x=price_sales["PERIOD"], y=price_sales["PRICE"], name="Avg Price (¢/kWh)", mode="lines+markers", line=dict(color="#f97316", width=2), marker=dict(size=4), yaxis="y2"))
fig_dual.update_layout(
    title="Avg Retail Price vs Total Sales Volume",
    xaxis_title="Month",
    yaxis=dict(title="Sales (MWh)", ticksuffix=" MWh"),
    yaxis2=dict(title="Avg Price (¢/kWh)", ticksuffix=" ¢", overlaying="y", side="right"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    template="plotly_white",
    height=380,
)
st.plotly_chart(fig_dual, use_container_width=True)

# ── Sector Share Shift ───────────────────────────────────────────────────────
st.subheader("Sector Share of Total Sales Over Time")
sector_totals = fdf.groupby(["PERIOD","SECTORNAME"])["SALES"].sum().reset_index()
period_totals = sector_totals.groupby("PERIOD")["SALES"].transform("sum")
sector_totals["SHARE_PCT"] = sector_totals["SALES"]/period_totals*100
fig_share = px.area(
    sector_totals.sort_values("PERIOD"),
    x="PERIOD",
    y="SHARE_PCT",
    color="SECTORNAME",
    title="Sector share of total sales (%)",
    labels={"PERIOD":"Month","SHARE_PCT":"Share (%)","SECTORNAME":"Sector"},
    template="plotly_white",
    groupnorm="percent",
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_share.update_layout(legend_title_text="Sector", height=380, yaxis=dict(ticksuffix="%"))
st.plotly_chart(fig_share, use_container_width=True)

# ── Fuel Mix / Renewable Quartile Visuals ───────────────────────────────────
st.subheader("Fuel Mix vs Retail Price by State")
st.caption("Each point is one state in one month. X-axis is the share of electricity generated from fossil fuels (gas + coal). Y-axis is the average retail price consumers pay.")

jdf = fdf.dropna(subset=["FOSSIL_PCT","AVG_PRICE","RENEWABLE_PCT"])  # Gold already joined
if jdf.empty:
    st.info("No overlapping fuel mix data for this date range.")
else:
    fig_scatter = px.scatter(
        jdf,
        x="FOSSIL_PCT",
        y="PRICE",
        color="RENEWABLE_PCT",
        hover_name="STATEDESCRIPTION",
        hover_data={"PERIOD":"|%b %Y","FOSSIL_PCT":":.1f","RENEWABLE_PCT":":.1f","PRICE":":.2f"},
        color_continuous_scale="RdYlGn",
        labels={"FOSSIL_PCT":"Fossil share (%)","PRICE":"Avg retail price (¢/kWh)","RENEWABLE_PCT":"Renewable share (%)"},
        title="Fossil generation share vs retail price — all states, all months",
        template="plotly_white",
        opacity=0.7,
    )
    z = np.polyfit(jdf["FOSSIL_PCT"].dropna(), jdf["PRICE"].dropna(), 1)
    x_range = pd.Series([jdf["FOSSIL_PCT"].min(), jdf["FOSSIL_PCT"].max()])
    fig_scatter.add_trace(go.Scatter(x=x_range, y=z[0]*x_range+z[1], mode="lines", line=dict(color="grey", dash="dash", width=1.5), name="Trend"))
    fig_scatter.update_layout(height=440, coloraxis_colorbar=dict(title="Renewable %"))
    st.plotly_chart(fig_scatter, use_container_width=True)

st.subheader("Renewable Penetration vs Retail Price Over Time")
st.caption("Lines show how each renewable quartile's average retail price evolves.")
state_avg_renewable = jdf.groupby("LOCATION")["RENEWABLE_PCT"].mean().reset_index().rename(columns={"RENEWABLE_PCT":"AVG_RENEWABLE_PCT"})
state_avg_renewable["QUARTILE"] = pd.qcut(state_avg_renewable["AVG_RENEWABLE_PCT"], q=4, labels=["Q1 Low renewable","Q2","Q3","Q4 High renewable"])
jdf_q = jdf.merge(state_avg_renewable[["LOCATION","QUARTILE"]], on="LOCATION", how="left")
quartile_trend = jdf_q.groupby(["PERIOD","QUARTILE"])["PRICE"].mean().reset_index().sort_values("PERIOD")

fig_quartile = px.line(
    quartile_trend,
    x="PERIOD",
    y="PRICE",
    color="QUARTILE",
    markers=True,
    title="Avg retail price by renewable generation quartile",
    labels={"PERIOD":"Month","PRICE":"Avg retail price (¢/kWh)","QUARTILE":"Renewable tier"},
    template="plotly_white",
    color_discrete_sequence=["#ef4444","#f97316","#22c55e","#0d9488"],
)
for trace in fig_quartile.data:
    r,g,b = int(trace.line.color[1:3],16), int(trace.line.color[3:5],16), int(trace.line.color[5:7],16)
    trace.update(fill="tozeroy", fillcolor=f"rgba({r},{g},{b},0.08)", line_width=2, marker_size=4)
fig_quartile.update_layout(height=420, yaxis=dict(ticksuffix=" ¢"), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
st.plotly_chart(fig_quartile, use_container_width=True)