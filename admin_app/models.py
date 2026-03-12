from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass(frozen=True)
class ComparisonRequest:
    dataset_id: str
    stage: str
    start_utc: datetime
    end_utc: datetime
    respondent_filter: str | None = None


@dataclass(frozen=True)
class ComparisonSummary:
    dataset_id: str
    stage: str
    expected_count: int
    actual_count: int
    missing_count: int
    extra_count: int
    status: str


@dataclass(frozen=True)
class MissingRecord:
    dataset_id: str
    stage: str
    grain_key: str
    period_start_utc: datetime | None
    respondent: str | None
    dimension_value: str | None
    reason: str


@dataclass(frozen=True)
class DagHealthRecord:
    dag_id: str
    is_paused: bool
    latest_run_state: str | None
    latest_run_start: datetime | None
    latest_run_end: datetime | None
    is_overdue: bool


@dataclass(frozen=True)
class TaskLogRecord:
    dag_id: str
    run_id: str
    task_id: str
    try_number: int
    log_path: str
    log_tail: str
    error_excerpt: str | None


@dataclass(frozen=True)
class ComparisonResult:
    summary: ComparisonSummary
    missing_df: pd.DataFrame
    extra_df: pd.DataFrame
    delta_df: pd.DataFrame
    expected_df: pd.DataFrame
    actual_df: pd.DataFrame
