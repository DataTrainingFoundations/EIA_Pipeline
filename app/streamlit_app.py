"""EIA Analytics — home page."""

import streamlit as st

from data_access import (
    get_connection,
    get_generation_coverage,
    get_demand_coverage,
    get_daily_generation_coverage,
    table_has_rows,
)

st.set_page_config(page_title="EIA Analytics", layout="wide")
st.title("EIA Electricity Analytics")
st.caption(
    "Near-real-time electricity generation and demand dashboards "
    "powered by the EIA Open Data API."
)

# ── Connection check ──────────────────────────────────────────────────────────
try:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select now()")
            server_time = cur.fetchone()[0]
    st.success(f"PostgreSQL connected — server time: {server_time}")
except Exception as exc:
    st.error(f"Database connection failed: {exc}")
    st.stop()

# ── Persona cards ─────────────────────────────────────────────────────────────
st.subheader("Dashboards")
col1, col2 = st.columns(2)

col1.markdown("""
**Generation Mix Monitor**
Track hourly electricity generation by fuel type across balancing authorities.
Surfaces which BAs are most dependent on fossil fuels right now and shows
renewable vs fossil share trends.
""")

col2.markdown("""
**Demand & Forecast Tracker**
Monitor hourly electricity demand vs day-ahead forecast per balancing authority.
Surfaces regions with the largest forecast misses and tracks daily peak demand trends.
""")

if hasattr(st, "page_link"):
    link1, link2 = st.columns(2)
    link1.page_link("pages/generation_mix_monitor.py",  label="→ Generation Mix Monitor")
    link2.page_link("pages/demand_forecast_tracker.py", label="→ Demand & Forecast Tracker")

# ── Data coverage ─────────────────────────────────────────────────────────────
st.subheader("Data coverage")
c1, c2, c3 = st.columns(3)

if table_has_rows("fact_generation_hourly"):
    cov = get_generation_coverage()
    c1.metric("Generation rows",   f"{int(cov['row_count']):,}")
    c1.caption(f"{cov['ba_count']} BAs · {cov['fuel_count']} fuel types")
    c1.caption(f"Latest: {cov['max_period']}")
else:
    c1.warning("No generation data yet")

if table_has_rows("fact_demand_hourly"):
    cov = get_demand_coverage()
    c2.metric("Demand rows",       f"{int(cov['row_count']):,}")
    c2.caption(f"{cov['ba_count']} BAs")
    c2.caption(f"Latest: {cov['max_period']}")
else:
    c2.warning("No demand data yet")

if table_has_rows("agg_daily_generation"):
    cov = get_daily_generation_coverage()
    c3.metric("Daily agg rows",    f"{int(cov['row_count']):,}")
    c3.caption(f"{cov['min_date']} → {cov['max_date']}")
else:
    c3.warning("No daily agg data yet")

st.info(
    "Data refreshes every hour at :15 past. "
    "To load historical data, trigger the **eia_backfill_pipeline** DAG "
    "from the Airflow UI with a start_date and end_date."
)
