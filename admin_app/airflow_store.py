from __future__ import annotations

import re
from contextlib import closing
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2

from config import AppConfig
from models import DagHealthRecord, TaskLogRecord

LOG_PATH_PATTERN = re.compile(
    r"dag_id=(?P<dag_id>.+?)[/\\]run_id=(?P<run_id>.+?)[/\\]task_id=(?P<task_id>.+?)[/\\]attempt=(?P<try_number>\d+)\.log$"
)


def expected_lag_for_dag(dag_id: str) -> timedelta:
    if dag_id == "electricity_fuel_type_data_incremental":
        return timedelta(hours=26)
    if dag_id.endswith("_incremental"):
        return timedelta(hours=2)
    if dag_id.endswith("_backfill"):
        return timedelta(minutes=20)
    if dag_id.endswith("_bronze_hourly_verification"):
        return timedelta(hours=2)
    if dag_id.endswith("_bronze_hourly_repair"):
        return timedelta(hours=2)
    if dag_id.startswith("platinum_"):
        return timedelta(hours=2)
    return timedelta(hours=6)


def extract_error_excerpt(log_text: str, max_lines: int = 12) -> str | None:
    lines = [line for line in log_text.splitlines() if line.strip()]
    if not lines:
        return None
    markers = ("Traceback", "ERROR", "Exception", "AnalysisException", "AirflowException")
    matching_indexes = [index for index, line in enumerate(lines) if any(marker in line for marker in markers)]
    if not matching_indexes:
        return "\n".join(lines[-max_lines:])
    start_index = max(matching_indexes[-1] - 2, 0)
    return "\n".join(lines[start_index : start_index + max_lines])


class AirflowStore:
    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def _connection(self):
        return psycopg2.connect(
            host=self._config.postgres_host,
            port=self._config.postgres_port,
            dbname=self._config.postgres_db,
            user=self._config.postgres_user,
            password=self._config.postgres_password,
        )

    def read_sql(self, query: str, params: list[object] | tuple[object, ...] | None = None) -> pd.DataFrame:
        with closing(self._connection()) as conn:
            with closing(conn.cursor()) as cur:
                if params is None:
                    cur.execute(query)
                else:
                    cur.execute(query, tuple(params))
                rows = cur.fetchall() if cur.description else []
                columns = [description[0] for description in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=columns)

    def get_dag_health(self) -> list[DagHealthRecord]:
        query = """
        with ranked_runs as (
            select
                dag_id,
                state,
                start_date,
                end_date,
                row_number() over (partition by dag_id order by coalesce(start_date, execution_date) desc) as rn
            from dag_run
        )
        select
            dag.dag_id,
            dag.is_paused,
            ranked_runs.state as latest_run_state,
            ranked_runs.start_date as latest_run_start,
            ranked_runs.end_date as latest_run_end
        from dag
        left join ranked_runs
            on dag.dag_id = ranked_runs.dag_id
           and ranked_runs.rn = 1
        where dag.dag_id like 'electricity_%%' or dag.dag_id like 'platinum_%%'
        order by dag.dag_id
        """
        df = self.read_sql(query)
        now_utc = datetime.now(timezone.utc)
        records: list[DagHealthRecord] = []
        for row in df.to_dict(orient="records"):
            latest_start = pd.to_datetime(row.get("latest_run_start"), utc=True, errors="coerce")
            latest_end = pd.to_datetime(row.get("latest_run_end"), utc=True, errors="coerce")
            is_overdue = False
            if not bool(row["is_paused"]):
                if pd.isna(latest_start):
                    is_overdue = True
                else:
                    is_overdue = (now_utc - latest_start.to_pydatetime()) > expected_lag_for_dag(str(row["dag_id"]))
            records.append(
                DagHealthRecord(
                    dag_id=str(row["dag_id"]),
                    is_paused=bool(row["is_paused"]),
                    latest_run_state=row.get("latest_run_state"),
                    latest_run_start=None if pd.isna(latest_start) else latest_start.to_pydatetime(),
                    latest_run_end=None if pd.isna(latest_end) else latest_end.to_pydatetime(),
                    is_overdue=is_overdue,
                )
            )
        return records

    def get_recent_dag_runs(self, limit: int = 20) -> pd.DataFrame:
        return self.read_sql(
            """
            select dag_id, run_id, state, queued_at, start_date, end_date
            from dag_run
            where dag_id like 'electricity_%%' or dag_id like 'platinum_%%'
            order by coalesce(start_date, queued_at, execution_date) desc
            limit %s
            """,
            [limit],
        )

    def get_recent_failed_tasks(self, limit: int = 20) -> pd.DataFrame:
        return self.read_sql(
            """
            select dag_id, task_id, run_id, state, start_date, end_date
            from task_instance
            where state = 'failed'
              and (dag_id like 'electricity_%%' or dag_id like 'platinum_%%')
            order by coalesce(end_date, start_date) desc
            limit %s
            """,
            [limit],
        )

    def get_backfill_summary(self) -> pd.DataFrame:
        return self.read_sql(
            """
            select dataset_id, status, job_count, min_chunk_start_utc, max_chunk_end_utc, last_updated_at
            from ops.backfill_job_summary
            order by dataset_id, status
            """
        )

    def get_bronze_coverage_summary(self) -> pd.DataFrame:
        return self.read_sql(
            """
            select dataset_id, status, hour_count, min_hour_start_utc, max_hour_start_utc, last_verified_at
            from ops.bronze_hourly_coverage_summary
            order by dataset_id, status
            """
        )

    def build_log_catalog(self) -> pd.DataFrame:
        if not self._config.airflow_log_dir.exists():
            return pd.DataFrame(columns=["dag_id", "run_id", "task_id", "try_number", "log_path"])

        records = []
        for path in self._config.airflow_log_dir.rglob("attempt=*.log"):
            match = LOG_PATH_PATTERN.search(str(path))
            if not match:
                continue
            records.append(
                {
                    "dag_id": match.group("dag_id"),
                    "run_id": match.group("run_id"),
                    "task_id": match.group("task_id"),
                    "try_number": int(match.group("try_number")),
                    "log_path": str(path),
                }
            )
        return pd.DataFrame.from_records(records)

    def read_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> TaskLogRecord | None:
        path = self._config.airflow_log_dir / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}" / f"attempt={try_number}.log"
        if not path.exists():
            return None
        log_text = path.read_text(encoding="utf-8", errors="replace")
        return TaskLogRecord(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            log_path=str(path),
            log_tail="\n".join(log_text.splitlines()[-200:]),
            error_excerpt=extract_error_excerpt(log_text),
        )
