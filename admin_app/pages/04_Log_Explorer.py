from __future__ import annotations

import streamlit as st

from airflow_store import AirflowStore
from config import load_config

config = load_config()
airflow = AirflowStore(config)


@st.cache_data(ttl=300)
def load_log_catalog():
    return airflow.build_log_catalog()


st.title("Log Explorer")
st.caption("Browse raw Airflow task attempt logs from the shared read-only log mount.")

catalog_df = load_log_catalog()
if catalog_df.empty:
    st.info("No Airflow logs are available in the shared log directory yet.")
    st.stop()

selection = st.session_state.get("log_explorer_selection", {})

dag_options = sorted(catalog_df["dag_id"].unique().tolist())
default_dag_index = dag_options.index(selection["dag_id"]) if selection.get("dag_id") in dag_options else 0
dag_id = st.selectbox("DAG", dag_options, index=default_dag_index)

filtered_runs = catalog_df[catalog_df["dag_id"] == dag_id]
run_options = sorted(filtered_runs["run_id"].unique().tolist(), reverse=True)
default_run_index = run_options.index(selection["run_id"]) if selection.get("run_id") in run_options else 0
run_id = st.selectbox("Run", run_options, index=default_run_index)

filtered_tasks = filtered_runs[filtered_runs["run_id"] == run_id]
task_options = sorted(filtered_tasks["task_id"].unique().tolist())
default_task_index = task_options.index(selection["task_id"]) if selection.get("task_id") in task_options else 0
task_id = st.selectbox("Task", task_options, index=default_task_index)

filtered_attempts = filtered_tasks[filtered_tasks["task_id"] == task_id]
attempt_options = sorted(filtered_attempts["try_number"].unique().tolist())
default_attempt_index = attempt_options.index(selection["try_number"]) if selection.get("try_number") in attempt_options else 0
try_number = st.selectbox("Attempt", attempt_options, index=default_attempt_index)

log_record = airflow.read_task_log(dag_id, run_id, task_id, int(try_number))
if log_record is None:
    st.error("The selected log file no longer exists.")
    st.stop()

st.code(log_record.log_path, language="text")
if log_record.error_excerpt:
    st.subheader("Probable Error Excerpt")
    st.code(log_record.error_excerpt, language="text")

st.subheader("Log Tail")
st.code(log_record.log_tail, language="text")
