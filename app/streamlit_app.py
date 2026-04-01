"""EIA Analytics — home page."""

import streamlit as st
from snowflake.snowpark import Session

from data_access import (
    get_session,
    get_coverage,
    get_daily_generation_coverage,
    table_has_rows,
)

st.set_page_config(page_title="EIA Analytics", layout="wide")
st.title("EIA Electricity Analytics")
st.caption(
    "Near-real-time electricity generation and demand dashboards "
    "powered by the EIA Open Data API."
)

# ── Connection check ───────────────────────────────────────────────────────────
try:
    session = get_session()
    result  = session.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()
    session.close()
    server_time = result[0]["TS"]
    st.success(f"Snowflake connected — server time: {server_time}")
except Exception as exc:
    st.error(f"Snowflake connection failed: {exc}")
    st.stop()

# ── Dashboard cards ────────────────────────────────────────────────────────────
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

# ── Data coverage ──────────────────────────────────────────────────────────────
st.subheader("Data coverage")
c1, c2, c3 = st.columns(3)

if table_has_rows():
    cov = get_coverage()
    c1.metric("Total hourly rows",   f"{int(cov['row_count']):,}")
    c1.caption(f"{cov['ba_count']} BAs · {cov['fuel_count']} fuel types")
    c1.caption(f"Latest: {cov['max_period']}")
    c1.caption(f"Since:  {cov['min_period']}")
else:
    c1.warning("No data yet — run eia_ingest to populate.")

if table_has_rows("AGG_DAILY_GENERATION"):
    cov = get_daily_generation_coverage()
    c2.metric("Daily generation rows", f"{int(cov['row_count']):,}")
    c2.caption(f"{cov['min_date']} → {cov['max_date']}")
else:
    c2.warning("No daily aggregation data yet.")

# Third card — pipeline info
c3.info(
    "Data refreshes every hour at :15 past.\n\n"
    "To backfill historical data, trigger the **eia_ingest** DAG "
    "from Airflow with `{\"start_date\": \"YYYY-MM-DD\", \"end_date\": \"YYYY-MM-DD\"}`."
)