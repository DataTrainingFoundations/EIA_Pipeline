from __future__ import annotations

import pandas as pd
import streamlit as st

from airflow_store import AirflowStore
from comparison_service import run_comparison, supported_dataset_ids
from config import load_config
from models import ComparisonRequest
from warehouse_store import WarehouseStore

config = load_config()
warehouse = WarehouseStore(config)
airflow = AirflowStore(config)


@st.cache_data(ttl=300)
def load_counts() -> pd.DataFrame:
    return warehouse.get_platform_counts()


@st.cache_data(ttl=60)
def load_airflow_health() -> pd.DataFrame:
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


@st.cache_data(ttl=60)
def load_recent_failed_tasks() -> pd.DataFrame:
    return airflow.get_recent_failed_tasks()


def _snapshot_targets() -> list[tuple[str, str]]:
    targets: list[tuple[str, str]] = []
    for stage in ("bronze", "silver", "gold"):
        targets.extend((stage, dataset_id) for dataset_id in supported_dataset_ids(stage))
    targets.extend(("platinum", dataset_id) for dataset_id in supported_dataset_ids("platinum"))
    return targets


def _run_snapshot() -> pd.DataFrame:
    start_utc, end_utc = config.default_window()
    targets = _snapshot_targets()
    progress_bar = st.progress(0.0)
    status_box = st.status("Running default snapshot", expanded=True)
    rows: list[dict[str, object]] = []

    for index, (stage, dataset_id) in enumerate(targets, start=1):
        status_box.write(f"[{index}/{len(targets)}] {stage} / {dataset_id}")
        result = run_comparison(
            config,
            ComparisonRequest(dataset_id=dataset_id, stage=stage, start_utc=start_utc, end_utc=end_utc),
        )
        rows.append(
            {
                "stage": stage,
                "dataset_id": dataset_id,
                "status": result.summary.status,
                "expected_count": result.summary.expected_count,
                "actual_count": result.summary.actual_count,
                "missing_count": result.summary.missing_count,
                "extra_count": result.summary.extra_count,
            }
        )
        progress_bar.progress(index / len(targets))

    status_box.update(label="Default snapshot complete", state="complete", expanded=False)
    return pd.DataFrame.from_records(rows)


st.title("Overview")
st.caption("Operational health and a manual 7-day comparison snapshot.")
st.info("Default snapshot is now run on demand so the page does not block indefinitely on initial load.")

counts_df = load_counts()
dag_health_df = load_airflow_health()
recent_failed_tasks_df = load_recent_failed_tasks()

service_col1, service_col2, service_col3 = st.columns(3)
service_col1.metric("Tracked DAGs", int(len(dag_health_df)))
service_col2.metric("Failed tasks", int(len(recent_failed_tasks_df)))
service_col3.metric("Log files", int(len(airflow.build_log_catalog())))

st.subheader("Platinum Table Counts")
st.dataframe(counts_df, use_container_width=True)

st.subheader("Default Comparison Snapshot")
if st.button("Run / refresh default snapshot"):
    st.session_state["overview_snapshot_df"] = _run_snapshot()

snapshot_df = st.session_state.get("overview_snapshot_df")
if snapshot_df is None:
    st.info("No snapshot has been run yet.")
elif snapshot_df.empty:
    st.info("Snapshot completed with no rows.")
else:
    stage_counts = snapshot_df.groupby("stage")["actual_count"].sum().to_dict()
    stage_col1, stage_col2, stage_col3, stage_col4 = st.columns(4)
    stage_col1.metric("Bronze rows in window", f"{int(stage_counts.get('bronze', 0)):,}")
    stage_col2.metric("Silver rows in window", f"{int(stage_counts.get('silver', 0)):,}")
    stage_col3.metric("Gold rows in window", f"{int(stage_counts.get('gold', 0)):,}")
    stage_col4.metric("Platinum rows in window", f"{int(stage_counts.get('platinum', 0)):,}")
    st.dataframe(snapshot_df, use_container_width=True)

st.subheader("Airflow Health")
if dag_health_df.empty:
    st.info("No Airflow DAG metadata was found.")
else:
    st.dataframe(dag_health_df, use_container_width=True)

st.subheader("Recent Failed Tasks")
if recent_failed_tasks_df.empty:
    st.success("No failed Airflow tasks are currently recorded.")
else:
    st.dataframe(recent_failed_tasks_df, use_container_width=True)
