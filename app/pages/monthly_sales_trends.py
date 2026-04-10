"""Monthly Sales Trends — EIA Analytics dashboard page (Gold table version)."""
# pylint: disable=import-error
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from data_access_sales import (
    load_sales_gold,
    load_gold_coverage,
    gold_table_has_rows,
)
SECTOR_COLORS = {
    "COM": "#74df84",  # teal
    "IND": "#e96379",  # rose
    "OTH": "#9c79ef",  # purple
    "RES": "#5292b7",  # blue
    "TRA": "#f3ad2b",  # orange
}

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

# ── Expand Sidebar and Adjust Styling ─────────────────────────────
st.markdown(
    """
    <style>
    /* ── Sidebar width ── */
    /* When expanded */
    .css-1d391kg {  /* wrapper class for sidebar in Streamlit 1.25+ */
        width: 320px;
    }
    /* When collapsed */
    .css-1d391kg[aria-expanded="false"] {
        width: 60px;
    }

    /* ── KPI Metrics Row ── */
    .stMetric {
        min-width: 180px !important;   /* prevents numbers/deltas from being cut */
    }
    .stMetric > div > div {
        white-space: nowrap !important;  /* prevents wrapping */
    }

    /* ── General tweaks ── */
    .css-1v3fvcr { max-width: 100% !important; } /* force container width for charts */
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Sidebar Filters ─────────────────────────────
with st.sidebar:
    st.header("Filters")

    coverage = load_gold_coverage()
    min_date = coverage["min_period"].to_pydatetime()
    max_date = coverage["max_period"].to_pydatetime()

    # Date picker now has full sidebar width

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
    date_range = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
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

# ── KPI row ─────────────────────────────
st.subheader("Summary")
kpi1, kpi2, kpi3, kpi4 = st.columns([2, 1.5, 1.5, 2])

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

# ── Monthly Trends by Sector ─────────────────────────────────────────────────
st.subheader("Monthly Trends by Sector")
if metric != "PRICE":
    monthly_sector = fdf.groupby(["PERIOD", "SECTORNAME"])[metric].sum().reset_index()
else:
    monthly_sector = fdf.groupby(["PERIOD", "SECTORNAME"])[metric].mean().reset_index()

# Sort sectors by total value so smallest is drawn on top
sector_order = (
    monthly_sector.groupby("SECTORNAME")[metric]
    .sum()
    .sort_values(ascending=False)
    .index.tolist()
)
monthly_sector["SECTORNAME"] = pd.Categorical(
    monthly_sector["SECTORNAME"], categories=sector_order, ordered=True
)
monthly_sector = monthly_sector.sort_values(["PERIOD", "SECTORNAME"])

fig_line = px.line(
    monthly_sector,
    x="PERIOD",
    y=metric,
    color="SECTORNAME",
    color_discrete_map=SECTOR_COLORS,
    markers=True,
    title=f"Monthly {metric_label} by Sector",
    labels={"PERIOD": "Month", metric: metric_label, "SECTORNAME": "Sector"},
    template="plotly_white",
    category_orders={"SECTORNAME": sector_order},
)

# First trace fills to zero, rest fill to next trace
for i, trace in enumerate(fig_line.data):
    hex_color = SECTOR_COLORS.get(trace.name, "#888888")
    r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
    fill_type = "tozeroy" if i == len(fig_line.data) - 1 else "tonexty"
    trace.update(
        fill="tozeroy",
        fillcolor=f"rgba({r},{g},{b},0.45)",
        line_width=2,
        marker_size=5,
    )

fig_line.update_layout(
    legend_title_text="Sector",
    height=380,
    yaxis=dict(
        tickprefix=tick_fmt["tickprefix"],
        ticksuffix=tick_fmt["ticksuffix"],
    ),
)
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
    color="SECTORNAME",
    color_discrete_map=SECTOR_COLORS,
)
fig_pie.update_traces(
    textposition="outside",
    textinfo="percent+label",
    hovertemplate="<b>%{label}</b><br>" + tick_fmt["tickprefix"] + "%{value:,.1f}" + tick_fmt["ticksuffix"] + "<extra></extra>"
)
fig_pie.update_layout(showlegend=False, height=380)
st.plotly_chart(fig_pie, use_container_width=True)

# ── Sector Share by Year ───────────────────────────────────────────────────
st.subheader("Sector Share of Total Sales by Year")
st.caption("Annual share of total electricity sales per sector. Small shifts reflect long-term structural changes in consumption patterns.")

annual_share = (
    fdf.groupby(["YEAR", "SECTORNAME"])["SALES"]
    .sum()
    .reset_index()
)
annual_total = annual_share.groupby("YEAR")["SALES"].transform("sum")
annual_share["SHARE_PCT"] = annual_share["SALES"] / annual_total * 100

fig_annual_share = px.bar(
    annual_share.sort_values("YEAR"),
    x="YEAR",
    y="SHARE_PCT",
    color="SECTORNAME",
    barmode="group",
    title="Annual Sector Share of Total Sales (%)",
    labels={"YEAR": "Year", "SHARE_PCT": "Share (%)", "SECTORNAME": "Sector"},
    template="plotly_white",
    color_discrete_map=SECTOR_COLORS,
)
fig_annual_share.update_layout(
    legend_title_text="Sector",
    height=380,
    yaxis=dict(ticksuffix="%"),
)
st.plotly_chart(fig_annual_share, use_container_width=True)

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
            color_discrete_map=SECTOR_COLORS,
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
