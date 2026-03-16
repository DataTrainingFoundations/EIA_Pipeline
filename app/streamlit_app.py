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
st.caption(
    "Action-focused business dashboards for near-term operations and resource planning decisions."
)

# Database connection
try:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select now()")
            server_time = cur.fetchone()[0]
    st.success(f"Platform connected. PostgreSQL server time: {server_time}")
except Exception as exc:  # pragma: no cover
    st.error(f"Database connection failed: {exc}")
    st.stop()

tabs = st.tabs(["Grid Operations Manager", "Utility Strategy Director", "Resource Planning Lead"])

# GRID OPERATIONS MANAGER TAB
with tabs[0]:
    st.subheader("Grid Operations Manager")
    st.markdown(
        """
        - Identify where immediate load risk or coverage stress needs attention
        - Triage forecast miss, ramp stress, and active alerts
        - Use trends as evidence after the current watchlist is clear
        """
    )
    if hasattr(st, "page_link"):
        st.page_link("pages/grid_operations_manager.py", label="Open Grid Operations Manager")
    else:
        st.caption("Use the sidebar to open Grid Operations Manager.")

    col1, col2, col3 = st.columns(3)
    if table_has_rows():
        coverage = get_summary_coverage()
        col1.metric("Core Daily Rows", f"{int(coverage['row_count']):,}")
        freshness_col1 = col1
        freshness_col1.markdown(
            f"**Core Daily Coverage**  \n{coverage['min_date']}  \nto  \n{coverage['max_date']}"
        )
    if table_has_rows("platinum.grid_operations_hourly"):
        ops_coverage = get_grid_operations_coverage()
        col2.metric("Ops Hourly Rows", f"{int(ops_coverage['row_count']):,}")
        freshness_col2 = col2
        freshness_col2.markdown(
            f"**Ops Hourly Coverage**  \n{ops_coverage['min_period']}  \nto  \n{ops_coverage['max_period']}"
        )

# UTILITY STRATEGY DIRECTOR TAB
with tabs[1]:
    st.subheader("Utility Strategy Director")
    st.markdown(
        """
        - Evaluate portfolio-level risk and opportunities
        - Review fuel diversity, capacity, and grid resilience metrics
        - Monitor long-term trends to guide strategic planning
        """
    )
    if hasattr(st, "page_link"):
        st.page_link("pages/utility_strategy_director.py", label="Open Utility Strategy Director")
    else:
        st.caption("Use the sidebar to open Utility Strategy Director.")


# RESOURCE PLANNING LEAD TAB
with tabs[2]:
    st.subheader("Resource Planning Lead")
    st.markdown(
        """
        - Identify where structural planning attention is needed
        - Compare carbon intensity, renewable share, fuel diversity, and gas dependence
        - Use recent trends to explain why a region is on the watchlist
        """
    )
    if hasattr(st, "page_link"):
        st.page_link("pages/resource_planning_lead.py", label="Open Resource Planning Lead")
    else:
        st.caption("Use the sidebar to open Resource Planning Lead.")

    if table_has_rows("platinum.resource_planning_daily"):
        planning_coverage = get_planning_coverage()
        col = st.columns(1)[0]
        col.metric("Planning Daily Rows", f"{int(planning_coverage['row_count']):,}")
        col.markdown(
            f"**Planning Daily Coverage**  \n{planning_coverage['min_date']}  \nto  \n{planning_coverage['max_date']}"
        )

st.subheader("Operational Status")
with st.expander("Backfill Status", expanded=False):
    status_df = get_backfill_status()
    if status_df.empty:
        st.info("No backfill jobs have been queued yet.")
    else:
        st.dataframe(status_df, use_container_width=True)