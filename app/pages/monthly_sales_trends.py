"""Monthly Sales Trends — EIA Analytics dashboard page."""
import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lower

st.set_page_config(page_title="Monthly Sales Trends · EIA Analytics", layout="wide")

# ── Page header ───────────────────────────────────────────────────────────────
st.title("Monthly Electricity Sales Trends")
st.caption(
    "Retail electricity sales by sector, state, and customer class. "
    "Data sourced from EIA retail sales reporting."
)


# ── Data loading (stub — replace with real connection logic) ──────────────────
load_dotenv()

@st.cache_data(ttl=3600)
def load_sales_data() -> pd.DataFrame:
    """
    # DEBUG
    raw = session.table("ELECTRICITY_RETAIL_SALES_RAW")
    print("Row count unfiltered:", raw.count())
    print("Columns:", raw.columns)
    print("Unique SECTORID:", raw.select("SECTORID").distinct().collect())
    print("Unique STATEID sample:", raw.select("STATEID").distinct().collect()[:5])
    print("Period range:", raw.select("PERIOD").distinct().collect())
    state_ids = [row["STATEID"] for row in raw.select("STATEID").distinct().collect()]
    print("All STATEIDs:", sorted(state_ids))
    """
    session = Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()

    VALID_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID',
    'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO',
    'MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA',
    'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
    }

    result_df = (
    session.table("ELECTRICITY_RETAIL_SALES_RAW")
    .filter(col("SECTORID") != "ALL")
    .filter(col("STATEID").isin(list(VALID_STATES)))
    .select("PERIOD", "STATEID", "STATEDESCRIPTION", "SECTORNAME",
            "CUSTOMERS", "PRICE", "REVENUE", "SALES")
    .to_pandas()
    )
    print("Row count after filter:", len(result_df))
    session.close()

    numeric_cols = ["CUSTOMERS", "PRICE", "REVENUE", "SALES"]
    result_df[numeric_cols] = result_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    result_df["SECTORNAME"] = result_df["SECTORNAME"].str.lower()
    result_df["PERIOD"] = pd.to_datetime(result_df["PERIOD"])

    return result_df

@st.cache_data(ttl=3600)
def load_local_sales_data() -> pd.DataFrame:
    """
    FOR LOCAL TESTING: 
    Load Local Sales Data:
    Load data from local csv
    Write to Df
    Use dataframe to pull pre-defined columns for visualizations
    """
    result_df = pd.read_csv(
        "/Users/ezra/Documents/Python/EIA_Pipeline/ELECTRICITY_SALES_MONTHLY_BRONZE.csv"
    )
    result_df = result_df[result_df["STATEID"] != "US"]
    result_df = result_df[result_df["SECTORNAME"].str.lower() != "all sectors"]
    numeric_cols = ["CUSTOMERS", "PRICE", "REVENUE", "SALES"]
    result_df[numeric_cols] = result_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    result_df.columns = result_df.columns.str.upper()
    result_df["SECTORNAME"] = result_df["SECTORNAME"].str.lower()
    result_df["PERIOD"] = pd.to_datetime(result_df["PERIOD"])

    return result_df

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

# Only use complete months (exclude the most recent incomplete month)
complete_months = fdf.groupby("PERIOD")["SALES"].sum().sort_index()

# Drop the last month as it may be incomplete
complete_months = complete_months.iloc[:-1]

if len(complete_months) >= 2:
    mom_delta = (
        (complete_months.iloc[-1] - complete_months.iloc[-2])
        / complete_months.iloc[-2] * 100
    )
    delta_str = f"{mom_delta:+.1f}% MoM"
else:
    delta_str = None

kpi1.metric("Total Sales", f"{total_sales:,.0f} M kWh", delta=delta_str)
kpi2.metric("Total Revenue", f"${total_revenue:,.0f} M")
kpi3.metric("Avg Retail Price", f"{avg_price:.2f} ¢/kWh")
kpi4.metric("Avg Monthly Customers", f"{total_customers:,.0f}")

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
        current_year = pd.Timestamp.today().year
        yoy_fdf = fdf[fdf["YEAR"] < current_year]
        yoy = yoy_fdf.groupby(["YEAR", "SECTORNAME"])[metric].sum().reset_index()
        yoy_pivot = yoy.pivot(index="YEAR", columns="SECTORNAME", values=metric)
        yoy_pct = (
            yoy_pivot.pct_change() * 100
        ).dropna().reset_index().melt(
            id_vars="YEAR", var_name="SECTORNAME", value_name="YoY_pct"
        )
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
