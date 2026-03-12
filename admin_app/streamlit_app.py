from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="EIA Admin Console", layout="wide")

st.title("EIA Admin Console")
st.caption("Read-only operational checks for API parity, storage layers, Airflow state, and task logs.")

st.warning("This app is read-only. It does not trigger DAGs, mutate data, or write pipeline state.")

st.markdown(
    """
Use the pages in the sidebar:

- `Overview` for platform health and default comparison snapshots
- `Data Comparisons` for Bronze, Silver, Gold, and Platinum parity checks
- `Airflow Status` for DAG and task state
- `Log Explorer` for raw Airflow task logs and error excerpts
"""
)
