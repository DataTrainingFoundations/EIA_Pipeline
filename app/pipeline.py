import streamlit as st

from data_access import get_backfill_status, get_summary_coverage, table_has_rows

st.set_page_config(page_title="Pipeline", layout="wide")
st.title("Pipeline")

status_df = get_backfill_status()
if status_df.empty:
    st.info("No backfill jobs have been queued yet.")
else:
    st.subheader("Backfill Queue")
    st.dataframe(status_df, use_container_width=True)

if table_has_rows():
    coverage = get_summary_coverage()
    col1, col2, col3 = st.columns(3)
    col1.metric("Rows", f"{int(coverage['row_count']):,}")
    col2.metric("Coverage Start", str(coverage['min_date']))
    col3.metric("Coverage End", str(coverage['max_date']))
else:
    st.warning("No serving rows available yet.")
