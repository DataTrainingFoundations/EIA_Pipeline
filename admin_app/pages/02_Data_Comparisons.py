from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

import pandas as pd
import streamlit as st

from comparison_service import run_comparison, supported_dataset_ids
from config import load_config
from models import ComparisonRequest

config = load_config()


def _run_with_progress(dataset_id: str, stage: str, start_utc: datetime, end_utc: datetime, respondent_filter: str):
    progress_messages: list[str] = []
    progress_bar = st.progress(0.0)
    status_box = st.status(f"Running {stage} comparison for {dataset_id}", expanded=True)

    def progress_cb(message: str) -> None:
        progress_messages.append(message)
        progress_bar.progress(min(len(progress_messages) / 4, 1.0))
        status_box.write(message)

    result = run_comparison(
        config,
        ComparisonRequest(
            dataset_id=dataset_id,
            stage=stage,
            start_utc=start_utc,
            end_utc=end_utc,
            respondent_filter=respondent_filter or None,
        ),
        progress_cb=progress_cb,
    )
    progress_bar.progress(1.0)
    status_box.update(label=f"{stage} comparison complete", state="complete", expanded=False)
    return result


def _render_stage(stage: str) -> None:
    if st.button(f"Clear results for {stage}", key=f"clear_{stage}"):
        st.session_state.pop(f"comparison_result_{stage}", None)

    default_start, default_end = config.default_window()
    with st.form(f"comparison_filters_{stage}"):
        dataset_id = st.selectbox("Dataset / target", supported_dataset_ids(stage), key=f"dataset_{stage}")
        start_date = st.date_input("Start date", value=default_start.date(), key=f"start_{stage}")
        end_date = st.date_input("End date", value=(default_end - timedelta(days=1)).date(), key=f"end_{stage}")
        respondent_filter = st.text_input("Respondent filter", key=f"respondent_{stage}")
        submitted = st.form_submit_button("Run comparison")

    if submitted:
        start_utc = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
        end_utc = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
        st.session_state[f"comparison_result_{stage}"] = _run_with_progress(
            dataset_id,
            stage,
            start_utc,
            end_utc,
            respondent_filter,
        )

    result = st.session_state.get(f"comparison_result_{stage}")
    if result is None:
        st.info("Run a comparison to load results for this stage.")
        return

    metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)
    metric_col1.metric("Status", result.summary.status)
    metric_col2.metric("Expected rows", f"{int(result.summary.expected_count):,}")
    metric_col3.metric("Actual rows", f"{int(result.summary.actual_count):,}")
    metric_col4.metric("Missing keys", f"{int(result.summary.missing_count):,}")
    metric_col5.metric("Extra keys", f"{int(result.summary.extra_count):,}")

    if not result.delta_df.empty:
        st.subheader("Count Deltas by Period")
        st.dataframe(result.delta_df, use_container_width=True)

    discrepancy_df = pd.concat([result.missing_df, result.extra_df], ignore_index=True)
    st.subheader("Discrepancies")
    if discrepancy_df.empty:
        st.success("No missing or extra keys were found for the selected window.")
    else:
        st.dataframe(discrepancy_df, use_container_width=True)
        st.download_button(
            "Download discrepancies CSV",
            data=discrepancy_df.to_csv(index=False).encode("utf-8"),
            file_name=f"{stage}_{result.summary.dataset_id.replace('.', '_')}_discrepancies.csv",
            mime="text/csv",
        )


st.title("Data Comparisons")
st.caption("Compare live EIA API keys against Bronze, Silver, Gold, and Platinum outputs.")

bronze_tab, silver_tab, gold_tab, platinum_tab = st.tabs(["Bronze", "Silver", "Gold", "Platinum"])
with bronze_tab:
    _render_stage("bronze")
with silver_tab:
    _render_stage("silver")
with gold_tab:
    _render_stage("gold")
with platinum_tab:
    _render_stage("platinum")
