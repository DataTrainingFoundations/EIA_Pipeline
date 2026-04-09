"""EIA Analytics home page."""

from __future__ import annotations

import streamlit as st

from data_access import (
    get_connection,
    get_daily_generation_coverage,
    get_demand_coverage,
    get_generation_coverage,
    get_generation_zero_value_summary,
    get_gold_coverage_summary,
    get_monthly_sales_coverage,
    get_pipeline_state_summary,
    get_raw_coverage_summary,
    qualified_table,
    SILVER_ELECTRICITY_RETAIL_SALES,
    table_has_rows,
)

st.set_page_config(page_title="EIA Analytics", layout="wide")
st.markdown(
    """
    <style>
    .hero-copy { color: #4b5563; max-width: 54rem; }
    .dashboard-card {
        border: 1px solid #d1d5db;
        border-radius: 0.85rem;
        padding: 1rem 1.1rem;
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        min-height: 12rem;
    }
    .dashboard-card h3 { margin-top: 0; margin-bottom: 0.4rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("EIA Electricity Analytics")
st.markdown(
    """
<div class="hero-copy">
Gold serves a dashboard-friendly star schema: hourly facts for generation and demand,
small shared dimensions, daily aggregates for trend monitoring, and a monthly sales mart
for longer-horizon state and sector analysis.
</div>
""",
    unsafe_allow_html=True,
)

try:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select current_timestamp()")
            server_time = cur.fetchone()[0]
    st.success(f"Snowflake connected. Server time: {server_time}")
except Exception as exc:
    st.error(f"Database connection failed: {exc}")
    st.stop()

theme_mode = st.context.theme.type

card_left, card_mid, card_right = st.columns(3)
if theme_mode == "dark":
    with card_left:
        st.markdown(
            """
    <div class="dashboard-card", style="background: lightslategray;">
        <h3>Generation Mix Monitor</h3>
        <p>Track hourly generation by fuel type, rank fossil-heavy regions, and inspect one BA's mix over time.</p>
        <p><strong>Uses:</strong> FACT_GENERATION_HOURLY, AGG_DAILY_GENERATION, DIM_FUEL_TYPE</p>
    </div>
    """,
            unsafe_allow_html=True,
        )
        if hasattr(st, "page_link"):
            st.page_link("pages/generation_mix_monitor.py", label="Open Generation Mix Monitor")

    with card_mid:
        st.markdown(
            """
    <div class="dashboard-card", style="background: lightslategray;">
        <h3>Demand & Forecast Tracker</h3>
        <p>Monitor latest forecast misses, track daily peaks, and drill into demand volatility for one BA.</p>
        <p><strong>Uses:</strong> FACT_DEMAND_HOURLY, AGG_DAILY_DEMAND_PEAK, DIM_BALANCING_AUTHORITY</p>
    </div>
    """,
            unsafe_allow_html=True,
        )
        if hasattr(st, "page_link"):
            st.page_link("pages/demand_forecast_tracker.py", label="Open Demand & Forecast Tracker")

    with card_right:
        st.markdown(
            """
    <div class="dashboard-card", style="background: lightslategray;">
        <h3>Monthly Sales Trends</h3>
        <p>Track monthly retail sales, revenue, price, and customers by state and sector, with fuel-mix overlays when available.</p>
        <p><strong>Uses:</strong> GOLD_ELECTRICITY_OPERATIONAL_SALES, SILVER_ELECTRICITY_RETAIL_SALES</p>
    </div>
    """,
            unsafe_allow_html=True,
        )
        if hasattr(st, "page_link"):
            st.page_link("pages/monthly_sales_trends.py", label="Open Monthly Sales Trends")
elif theme_mode == "light":
    with card_left:
        st.markdown(
            """
    <div class="dashboard-card">
        <h3>Generation Mix Monitor</h3>
        <p>Track hourly generation by fuel type, rank fossil-heavy regions, and inspect one BA's mix over time.</p>
        <p><strong>Uses:</strong> FACT_GENERATION_HOURLY, AGG_DAILY_GENERATION, DIM_FUEL_TYPE</p>
    </div>
    """,
            unsafe_allow_html=True,
        )
        if hasattr(st, "page_link"):
            st.page_link("pages/generation_mix_monitor.py", label="Open Generation Mix Monitor")

    with card_mid:
        st.markdown(
            """
    <div class="dashboard-card">
        <h3>Demand & Forecast Tracker</h3>
        <p>Monitor latest forecast misses, track daily peaks, and drill into demand volatility for one BA.</p>
        <p><strong>Uses:</strong> FACT_DEMAND_HOURLY, AGG_DAILY_DEMAND_PEAK, DIM_BALANCING_AUTHORITY</p>
    </div>
    """,
            unsafe_allow_html=True,
        )
        if hasattr(st, "page_link"):
            st.page_link("pages/demand_forecast_tracker.py", label="Open Demand & Forecast Tracker")

    with card_right:
        st.markdown(
            """
    <div class="dashboard-card"">
        <h3>Monthly Sales Trends</h3>
        <p>Track monthly retail sales, revenue, price, and customers by state and sector, with fuel-mix overlays when available.</p>
        <p><strong>Uses:</strong> GOLD_ELECTRICITY_OPERATIONAL_SALES, SILVER_ELECTRICITY_RETAIL_SALES</p>
    </div>
    """,
            unsafe_allow_html=True,
        )
        if hasattr(st, "page_link"):
            st.page_link("pages/monthly_sales_trends.py", label="Open Monthly Sales Trends")


st.subheader("Data coverage")
cov_left, cov_mid, cov_right, cov_far = st.columns(4)

if table_has_rows("FACT_GENERATION_HOURLY"):
    cov = get_generation_coverage()
    cov_left.metric("Generation rows", f"{int(cov['row_count']):,}")
    cov_left.caption(f"{cov['ba_count']} balancing authorities")
    cov_left.caption(f"{cov['fuel_count']} fuel types")
    cov_left.caption(f"Latest period: {cov['max_period']}")
else:
    cov_left.warning("No generation data yet.")

if table_has_rows("FACT_DEMAND_HOURLY"):
    cov = get_demand_coverage()
    cov_mid.metric("Demand rows", f"{int(cov['row_count']):,}")
    cov_mid.caption(f"{cov['ba_count']} balancing authorities")
    cov_mid.caption(f"Latest period: {cov['max_period']}")
else:
    cov_mid.warning("No demand data yet.")

if table_has_rows("AGG_DAILY_GENERATION"):
    cov = get_daily_generation_coverage()
    cov_right.metric("Daily aggregate rows", f"{int(cov['row_count']):,}")
    cov_right.caption(f"Coverage: {cov['min_date']} to {cov['max_date']}")
else:
    cov_right.warning("No daily generation aggregates yet.")

if table_has_rows(qualified_table("SILVER", SILVER_ELECTRICITY_RETAIL_SALES)):
    cov = get_monthly_sales_coverage()
    cov_far.metric("Monthly sales rows", f"{int(cov['row_count']):,}")
    cov_far.caption(f"{cov['state_count']} states")
    cov_far.caption(f"{cov['sector_count']} sectors")
    cov_far.caption(f"Latest period: {cov['max_period']}")
else:
    cov_far.warning("No monthly sales data yet.")

st.info(
    "Ingest and transform now run as separate Airflow DAGs for hourly and monthly cadence groups. "
    "Bootstrap can chain follow-up runs automatically until RAW and published history catch up."
)

st.subheader("Bootstrap diagnostics")
diag_left, diag_right = st.columns(2)
with diag_left:
    try:
        st.caption("Pipeline state")
        st.dataframe(get_pipeline_state_summary(), use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(f"Pipeline state unavailable: {exc}")
with diag_right:
    try:
        st.caption("RAW coverage")
        st.dataframe(get_raw_coverage_summary(), use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(f"RAW coverage unavailable: {exc}")

try:
    st.caption("Published GOLD coverage")
    st.dataframe(get_gold_coverage_summary(), use_container_width=True, hide_index=True)
except Exception as exc:
    st.warning(f"GOLD coverage unavailable: {exc}")

try:
    zero_summary = get_generation_zero_value_summary()
    if zero_summary:
        st.caption("Recent generation zero-value rows")
        st.dataframe(zero_summary, use_container_width=True, hide_index=True)
except Exception as exc:
    st.warning(f"Generation zero-value diagnostics unavailable: {exc}")
