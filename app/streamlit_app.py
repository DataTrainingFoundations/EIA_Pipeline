"""Home page for the EIA Streamlit application."""

import streamlit as st

from data_access import (
    get_connection,
    get_generation_coverage,
    get_demand_coverage,
    get_daily_generation_coverage,
    #get_sales_coverage,
    table_has_rows,
)

st.set_page_config(page_title="EIA Analytics", layout="wide")
st.title("EIA Electricity Analytics")
st.caption("Near-real-time electricity generation, demand, and retail sales dashboards powered by the EIA Open Data API.")

try:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select now()")
            server_time = cur.fetchone()[0]
    st.success(f"PostgreSQL connected. Server time: {server_time}")
except Exception as exc:
    st.error(f"Database connection failed: {exc}")
    st.stop()

st.subheader("Dashboards")
col1, col2, col3, col4, col5 = st.columns(5)

col1.markdown("""
**Generation Mix Monitor**
- Hourly fuel mix by BA
- Renewable vs fossil share
- Latest-hour rankings
""")
col2.markdown("""
**Demand & Forecast Tracker**
- Demand vs day-ahead forecast
- Forecast miss watchlist
- Daily peak trends
""")
col3.markdown("""
**Energy Trader**
- Gas share + demand surprise signals
- Bullish / Bearish BA watchlist
- RE intermittency risk
""")
col4.markdown("""
**Utility Sales Manager**
- Monthly retail sales by sector
- State growth rankings & map
- Pricing trends
""")
col5.markdown("""
**Executive Dashboard**
- Cross-domain KPI summary
- Generation + sales alignment
- YoY growth map
""")

if hasattr(st, "page_link"):
    link1, link2, link3, link4, link5 = st.columns(5)
    link1.page_link("pages/generation_mix_monitor.py",  label="Generation Mix")
    link2.page_link("pages/demand_forecast_tracker.py", label="Demand & Forecast")
    link3.page_link("pages/energy_trader.py",           label="Energy Trader")
    link4.page_link("pages/utility_sales_manager.py",   label="Utility Sales")
    link5.page_link("pages/executive_dashboard.py",     label="Executive Dashboard")

st.subheader("Data Coverage")
c1, c2, c3, c4 = st.columns(4)

if table_has_rows("fact_generation_hourly"):
    cov = get_generation_coverage()
    c1.metric("Generation rows", f"{int(cov['row_count']):,}")
    c1.caption(f"{cov['ba_count']} BAs · {cov['fuel_count']} fuels")

if table_has_rows("fact_demand_hourly"):
    cov = get_demand_coverage()
    c2.metric("Demand rows", f"{int(cov['row_count']):,}")
    c2.caption(f"{cov['ba_count']} BAs")

if table_has_rows("agg_daily_generation"):
    cov = get_daily_generation_coverage()
    c3.metric("Daily gen rows", f"{int(cov['row_count']):,}")

if table_has_rows("fact_retail_sales_monthly"):
    cov = get_sales_coverage()
    c4.metric("Sales rows", f"{int(cov['row_count']):,}")
    c4.caption(f"{cov['state_count']} states · {cov['sector_count']} sectors")

st.info(
    "Generation & demand refresh hourly. "
    "Retail sales refresh on the 15th of each month (EIA publishes with ~6-week lag)."
)
