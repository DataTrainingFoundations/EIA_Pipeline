"""Monthly Sales Trends — EIA Analytics dashboard page."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Monthly Sales Trends · EIA Analytics", layout="wide")

# ── Page header ───────────────────────────────────────────────────────────────
st.title("Monthly Electricity Sales Trends")
st.caption(
    "Retail electricity sales by sector, state, and customer class. "
    "Data sourced from EIA retail sales reporting."
)


# ── Data loading (stub — replace with real connection logic) ──────────────────
@st.cache_data(ttl=3600)
def load_sales_data() -> pd.DataFrame:
    """
    TODO: Replace this stub with your actual database/Snowflake query.

    Expected columns after cleaning:
        PERIOD           datetime  — monthly period (e.g. 2024-01-01)
        STATEDESCRIPTION str       — full state name
        STATEID          str       — two-letter state abbreviation
        SECTORNAME       str       — e.g. "residential", "commercial", "industrial"
        CUSTOMERS        float     — number of customers
        PRICE            float     — average retail price (cents/kWh)
        REVENUE          float     — revenue (million $)
        SALES            float     — sales (million kWh)

    Example stub generates synthetic data so the layout renders immediately.
    """
    rng = np.random.default_rng(42)

    sectors = ["residential", "commercial", "industrial", "transportation"]
    states = {
        "California": "CA", "Texas": "TX", "Florida": "FL",
        "New York": "NY", "Illinois": "IL", "Pennsylvania": "PA",
        "Ohio": "OH", "Georgia": "GA", "Michigan": "MI", "Washington": "WA",
    }
    periods = pd.date_range("2022-01-01", periods=36, freq="MS")

    rows = []
    for period in periods:
        for state, abbr in states.items():
            for sector in sectors:
                base_sales = {
                    "residential": 800,
                    "commercial": 500,
                    "industrial": 300,
                    "transportation": 20,
                }[sector]
                month_mult = 1 + 0.25 * np.sin((period.month - 1) * np.pi / 6)
                sales = base_sales * month_mult * rng.uniform(0.9, 1.1)
                revenue = sales * rng.uniform(0.10, 0.14)
                price = (revenue / sales) * 100
                customers = sales * rng.uniform(0.8, 1.2) * 1000

                rows.append(dict(
                    PERIOD=period,
                    STATEDESCRIPTION=state,
                    STATEID=abbr,
                    SECTORNAME=sector,
                    SALES=round(sales, 2),
                    REVENUE=round(revenue, 2),
                    PRICE=round(price, 4),
                    CUSTOMERS=round(customers),
                ))

    return pd.DataFrame(rows)


with st.spinner("Loading sales data…"):
    df = load_sales_data()

if df.empty:
    st.warning("No sales data available. Check the data pipeline.")
    st.stop()

# Derived fields
df["YEAR"] = df["PERIOD"].dt.year
df["MONTH"] = df["PERIOD"].dt.month
df["YEAR_MONTH"] = df["PERIOD"].dt.strftime("%Y-%m")

# ── Sidebar Filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    min_date = df["PERIOD"].min().to_pydatetime()
    max_date = df["PERIOD"].max().to_pydatetime()

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
            "SALES": "Sales (million kWh)",
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

METRIC_LABELS = {
    "SALES": "Sales (million kWh)",
    "REVENUE": "Revenue (million $)",
    "PRICE": "Avg Price (cents/kWh)",
    "CUSTOMERS": "Customers",
}
metric_label = METRIC_LABELS[metric]

METRIC_TICK_FORMAT = {
    "SALES":     {"ticksuffix": " M kWh", "tickprefix": ""},
    "REVENUE":   {"ticksuffix": " M",     "tickprefix": "$"},
    "PRICE":     {"ticksuffix": " ¢",     "tickprefix": ""},
    "CUSTOMERS": {"ticksuffix": "",       "tickprefix": ""},
}
tick_fmt = METRIC_TICK_FORMAT[metric]

# ── KPI row ───────────────────────────────────────────────────────────────────
st.subheader("Summary")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

total_sales = fdf["SALES"].sum()
total_revenue = fdf["REVENUE"].sum()
avg_price = fdf["PRICE"].mean()
total_customers = fdf["CUSTOMERS"].sum()

monthly_total = fdf.groupby("PERIOD")["SALES"].sum().sort_index()
if len(monthly_total) >= 2:
    mom_delta = (
        (monthly_total.iloc[-1] - monthly_total.iloc[-2])
        / monthly_total.iloc[-2] * 100
    )
    delta_str = f"{mom_delta:+.1f}% MoM"
else:
    delta_str = None

kpi1.metric("Total Sales", f"{total_sales:,.0f} M kWh", delta=delta_str)
kpi2.metric("Total Revenue", f"${total_revenue:,.0f} M")
kpi3.metric("Avg Retail Price", f"{avg_price:.2f} ¢/kWh")
kpi4.metric("Total Customers", f"{total_customers:,.0f}")

st.divider()

# ── Row 1: Monthly trend line + Sector pie ────────────────────────────────────
st.subheader("Trends & Mix")
col_line, col_pie = st.columns([3, 1])

with col_line:
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
    fig_line.update_traces(
        line_width=2,
        marker_size=5,
        fill="tozeroy",
        fillcolor="rgba(0,0,0,0.08)",
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

with col_pie:
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
        hovertemplate=(
            "<b>%{label}</b><br>"
            + tick_fmt["tickprefix"]
            + "%{value:,.1f}"
            + tick_fmt["ticksuffix"]
            + "<extra></extra>"
        ),
    )
    fig_pie.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig_pie, use_container_width=True)

# ── Row 2: State bar + Seasonal heatmap ──────────────────────────────────────
st.subheader("State & Seasonal Breakdown")
col_bar, col_heat = st.columns(2)

with col_bar:
    agg_fn = "mean" if metric == "PRICE" else "sum"
    state_totals = (
        fdf.groupby("STATEDESCRIPTION")[metric]
        .agg(agg_fn)
        .sort_values(ascending=False)
        .reset_index()
    )

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
    fig_bar.update_layout(
        xaxis_tickangle=-40,
        coloraxis_showscale=False,
        height=380,
        yaxis=dict(
            tickprefix=tick_fmt["tickprefix"],
            ticksuffix=tick_fmt["ticksuffix"],
        ),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col_heat:
    heat_agg_fn = np.mean if metric == "PRICE" else np.sum
    heat_data = (
        fdf.groupby(["YEAR", "MONTH"])[metric]
        .agg(heat_agg_fn)
        .reset_index()
        .pivot(index="MONTH", columns="YEAR", values=metric)
    )

    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    heat_data.index = [month_names[m - 1] for m in heat_data.index]

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
        hovertemplate=(
            "Month: %{y}<br>Year: %{x}<br>"
            + tick_fmt["tickprefix"]
            + "%{z:,.1f}"
            + tick_fmt["ticksuffix"]
            + "<extra></extra>"
        ),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

# ── Row 3: YoY growth bars + Cumulative area ─────────────────────────────────
st.subheader("Year-over-Year & Cumulative")
col_yoy, col_area = st.columns(2)

with col_yoy:
    if metric != "PRICE":
        yoy = fdf.groupby(["YEAR", "SECTORNAME"])[metric].sum().reset_index()
        yoy_pivot = yoy.pivot(index="YEAR", columns="SECTORNAME", values=metric)
        yoy_pct = (
            yoy_pivot.pct_change() * 100
        ).dropna().reset_index().melt(
            id_vars="YEAR",
            var_name="SECTORNAME",
            value_name="YoY_pct",
        )

        fig_yoy = px.bar(
            yoy_pct,
            x="YEAR",
            y="YoY_pct",
            color="SECTORNAME",
            barmode="group",
            title="Year-over-Year Growth (%) by Sector",
            labels={
                "YEAR": "Year",
                "YoY_pct": "YoY Change (%)",
                "SECTORNAME": "Sector",
            },
            template="plotly_white",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_yoy.add_hline(y=0, line_dash="dash", line_color="grey", line_width=1)
        fig_yoy.update_layout(height=360)
        st.plotly_chart(fig_yoy, use_container_width=True)
    else:
        st.info("YoY growth chart is shown for volume/revenue metrics. Switch the primary metric.")

with col_area:
    if metric != "PRICE":
        cum_data = fdf.groupby("PERIOD")[metric].sum().reset_index()
    else:
        cum_data = fdf.groupby("PERIOD")[metric].mean().reset_index()

    cum_data = cum_data.sort_values("PERIOD")
    cum_data["CUMULATIVE"] = cum_data[metric].cumsum()

    fig_area = go.Figure()
    fig_area.add_trace(go.Scatter(
        x=cum_data["PERIOD"],
        y=cum_data["CUMULATIVE"],
        fill="tozeroy",
        mode="lines",
        line_color="#0d9488",
        fillcolor="rgba(13,148,136,0.15)",
        name=f"Cumulative {metric_label}",
    ))
    fig_area.update_layout(
        title=f"Cumulative {metric_label} Over Time",
        xaxis_title="Month",
        yaxis_title=f"Cumulative {metric_label}",
        yaxis=dict(
            tickprefix=tick_fmt["tickprefix"],
            ticksuffix=tick_fmt["ticksuffix"],
        ),
        template="plotly_white",
        height=360,
    )
    st.plotly_chart(fig_area, use_container_width=True)

# ── Row 4: Detailed data table ────────────────────────────────────────────────
with st.expander("View underlying data", expanded=False):
    display_cols = ["PERIOD", "STATEDESCRIPTION", "SECTORNAME",
                    "SALES", "REVENUE", "PRICE", "CUSTOMERS"]
    st.dataframe(
        fdf[display_cols]
        .sort_values(["PERIOD", "STATEDESCRIPTION", "SECTORNAME"])
        .reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"{len(fdf):,} rows matching current filters.")