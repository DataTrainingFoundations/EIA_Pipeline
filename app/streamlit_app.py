"""Home page for the business-facing Streamlit application.

This page confirms the warehouse connection, summarizes the available persona
views, and shows high-level pipeline freshness pulled from the serving tables.
"""

import streamlit as st

from data_access import (
    get_backfill_status,
    get_connection,
    get_grid_operations_coverage,
    get_planning_coverage,
    get_summary_coverage,
    table_has_rows,
)

st.set_page_config(page_title="EIA Analytics", layout="wide")
st.title("EIA Decision Support")
st.caption("Action-focused business dashboards for near-term operations and resource planning decisions.")

try:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select now()")
            server_time = cur.fetchone()[0]
    st.success(f"Platform connected. PostgreSQL server time: {server_time}")
except Exception as exc:  # pragma: no cover
    st.error(f"Database connection failed: {exc}")
    st.stop()

st.subheader("Supported Views")
persona_col1, persona_col2 = st.columns(2)
persona_col1.markdown(
    """
**Grid Operations Manager**

- Identify where immediate load risk or coverage stress needs attention
- Triage forecast miss, ramp stress, and active alerts
- Use trends as evidence after the current watchlist is clear
"""
)
persona_col2.markdown(
    """
**Resource Planning Lead**

- Identify where structural planning attention is needed
- Compare carbon intensity, renewable share, fuel diversity, and gas dependence
- Use recent trends to explain why a region is on the watchlist
"""
)

link_col1, link_col2 = st.columns(2)
if hasattr(st, "page_link"):
    link_col1.page_link("pages/grid_operations_manager.py", label="Open Grid Operations Manager")
    link_col2.page_link("pages/resource_planning_lead.py", label="Open Resource Planning Lead")
else:  # pragma: no cover
    link_col1.caption("Use the sidebar to open Grid Operations Manager.")
    link_col2.caption("Use the sidebar to open Resource Planning Lead.")

col1, col2, col3 = st.columns(3)
if table_has_rows():
    coverage = get_summary_coverage()
    col1.metric("Core Daily Rows", f"{int(coverage['row_count']):,}")
if table_has_rows("platinum.grid_operations_hourly"):
    ops_coverage = get_grid_operations_coverage()
    col2.metric("Ops Hourly Rows", f"{int(ops_coverage['row_count']):,}")
if table_has_rows("platinum.resource_planning_daily"):
    planning_coverage = get_planning_coverage()
    col3.metric("Planning Daily Rows", f"{int(planning_coverage['row_count']):,}")

st.subheader("Data Freshness")
freshness_col1, freshness_col2, freshness_col3 = st.columns(3)
if table_has_rows():
    freshness_col1.markdown(
        f"**Core Daily Coverage**  \n{coverage['min_date']}  \nto  \n{coverage['max_date']}"
    )
if table_has_rows("platinum.grid_operations_hourly"):
    freshness_col2.markdown(
        f"**Ops Hourly Coverage**  \n{ops_coverage['min_period']}  \nto  \n{ops_coverage['max_period']}"
    )
if table_has_rows("platinum.resource_planning_daily"):
    freshness_col3.markdown(
        f"**Planning Daily Coverage**  \n{planning_coverage['min_date']}  \nto  \n{planning_coverage['max_date']}"
    )

st.info(
    "Primary navigation is now centered on the two persona dashboards. Pipeline status remains available below for operational monitoring."
)

with st.expander("Operational Status", expanded=False):
    status_df = get_backfill_status()
    if status_df.empty:
        st.info("No backfill jobs have been queued yet.")
    else:
        st.dataframe(status_df, use_container_width=True)
