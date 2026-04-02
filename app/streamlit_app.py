"""EIA Analytics home page."""

from __future__ import annotations

import streamlit as st

from data_access import (
    get_connection,
    get_daily_generation_coverage,
    get_demand_coverage,
    get_generation_coverage,
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
small shared dimensions, and daily aggregates for trend monitoring. Use the pages below
to inspect the latest balancing-authority conditions and the broader fuel mix.
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

card_left, card_right = st.columns(2)
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

with card_right:
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

st.subheader("Data coverage")
cov_left, cov_mid, cov_right = st.columns(3)

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

st.info(
    "The normal refresh chain is eia_ingest -> eia_silver -> eia_gold. "
    "For historical loads, trigger eia_ingest in Airflow with start_date and end_date."
)
