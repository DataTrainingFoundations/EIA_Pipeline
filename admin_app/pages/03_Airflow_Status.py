from __future__ import annotations

import pandas as pd
import streamlit as st

from airflow_store import AirflowStore
from config import load_config

config = load_config()
airflow = AirflowStore(config)


@st.cache_data(ttl=300)
def load_dag_health() -> pd.DataFrame:
    records = airflow.get_dag_health()
    return pd.DataFrame(
        {
            "dag_id": [record.dag_id for record in records],
            "is_paused": [record.is_paused for record in records],
            "latest_run_state": [record.latest_run_state for record in records],
            "latest_run_start": [record.latest_run_start for record in records],
            "latest_run_end": [record.latest_run_end for record in records],
            "is_overdue": [record.is_overdue for record in records],
        }
    )


@st.cache_data(ttl=300)
def load_recent_dag_runs() -> pd.DataFrame:
    return airflow.get_recent_dag_runs()


@st.cache_data(ttl=300)
def load_failed_tasks() -> pd.DataFrame:
    return airflow.get_recent_failed_tasks()


st.title("Airflow Status")
st.caption("Read-only DAG and task visibility with queue and coverage summaries.")

dag_health_df = load_dag_health()
if not dag_health_df.empty:
    st.subheader("DAG Health")
    st.dataframe(dag_health_df, use_container_width=True)
else:
    st.info("No DAG health records were found.")

st.subheader("Recent DAG Runs")
st.dataframe(load_recent_dag_runs(), use_container_width=True)

failed_tasks_df = load_failed_tasks()
st.subheader("Recent Failed Tasks")
if failed_tasks_df.empty:
    st.success("No failed tasks are currently recorded.")
else:
    st.dataframe(failed_tasks_df, use_container_width=True)
    task_options = {
        f"{row.dag_id} | {row.task_id} | {row.run_id}": row
        for row in failed_tasks_df.itertuples(index=False)
    }
    selected_label = st.selectbox("Prepare a failed task for Log Explorer", list(task_options))
    if st.button("Open in Log Explorer"):
        selected = task_options[selected_label]
        st.session_state["log_explorer_selection"] = {
            "dag_id": selected.dag_id,
            "run_id": selected.run_id,
            "task_id": selected.task_id,
            "try_number": 1,
        }
        if hasattr(st, "switch_page"):
            st.switch_page("pages/04_Log_Explorer.py")

st.subheader("Backfill Queue Summary")
st.dataframe(airflow.get_backfill_summary(), use_container_width=True)

st.subheader("Bronze Coverage Summary")
bronze_coverage_df = airflow.get_bronze_coverage_summary()
if bronze_coverage_df.empty:
    st.info("No bronze coverage summary rows exist yet.")
else:
    st.dataframe(bronze_coverage_df, use_container_width=True)
