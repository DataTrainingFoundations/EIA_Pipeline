"""Backfill queue management helpers for Airflow dataset DAGs."""

from __future__ import annotations

import logging
import os
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Any

from pipeline_runtime import (
    SUPPORTED_BACKFILL_STEPS,
    _floor_to_step,
    _format_cli_hour,
    _format_cli_timestamp,
    _parse_utc,
    _retreat_step,
    current_airflow_log_fields,
    db_connect,
    format_log_fields,
    get_dataset,
)

logger = logging.getLogger(__name__)
BACKFILL_MAX_ATTEMPTS = int(os.getenv("AIRFLOW_BACKFILL_MAX_ATTEMPTS", "5"))
BACKFILL_STALE_MINUTES = int(os.getenv("AIRFLOW_BACKFILL_STALE_MINUTES", "60"))


def _start_of_calendar_week(value: datetime) -> datetime:
    """Return the Monday 00:00 UTC boundary for one timestamp."""

    utc_value = value.astimezone(timezone.utc)
    start_of_day = utc_value.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_of_day - timedelta(days=start_of_day.weekday())


def _load_existing_backfill_jobs(
    cur, dataset_id: str, history_start: datetime, boundary: datetime
) -> dict[tuple[datetime, datetime], str]:
    """Load existing backfill jobs so enqueue logic can avoid duplicate work."""

    cur.execute(
        """
        select chunk_start_utc, chunk_end_utc, status
        from ops.backfill_jobs
        where dataset_id = %s
          and chunk_start_utc >= %s
          and chunk_end_utc <= %s
        """,
        (dataset_id, history_start, boundary),
    )
    return {
        (
            chunk_start_utc.astimezone(timezone.utc),
            chunk_end_utc.astimezone(timezone.utc),
        ): status
        for chunk_start_utc, chunk_end_utc, status in cur.fetchall()
    }


def recover_stale_backfill_jobs(dataset_id: str) -> int:
    """Mark stale in-progress backfill jobs as failed so they can be retried."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            update ops.backfill_jobs
            set status = 'failed',
                updated_at = now(),
                last_error = %s
            where dataset_id = %s
              and status = 'in_progress'
              and started_at is not null
              and started_at < now() - (%s || ' minutes')::interval
              and attempt_count < %s
            """,
            (
                f"auto-recovered stale backfill job after {BACKFILL_STALE_MINUTES} minutes",
                dataset_id,
                str(BACKFILL_STALE_MINUTES),
                BACKFILL_MAX_ATTEMPTS,
            ),
        )
        recovered = cur.rowcount
        conn.commit()
    if recovered:
        logger.warning(
            "Recovered stale backfill jobs %s",
            format_log_fields(
                **current_airflow_log_fields(),
                dataset_id=dataset_id,
                recovered=recovered,
                stale_minutes=BACKFILL_STALE_MINUTES,
                max_attempts=BACKFILL_MAX_ATTEMPTS,
            ),
        )
    return recovered


def enqueue_backfill_jobs(
    dataset_id: str, max_pending_override: int | None = None
) -> int:
    """Create pending backfill jobs from the newest missing window backward."""

    dataset = get_dataset(dataset_id)
    backfill_config = dataset.get("backfill") or {}
    step = backfill_config.get("step", "day")
    if step not in SUPPORTED_BACKFILL_STEPS:
        raise ValueError(f"Unsupported backfill step '{step}'")

    history_start = _floor_to_step(_parse_utc(backfill_config["start"]), step)
    pending_limit = (
        max_pending_override
        if max_pending_override is not None
        else int(backfill_config.get("max_pending_chunks", 14))
    )
    boundary = _floor_to_step(
        datetime.now(timezone.utc), step if step in {"month", "year"} else "hour"
    )

    inserted = 0
    open_window_count = 0

    recover_stale_backfill_jobs(dataset_id)

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        existing_jobs = _load_existing_backfill_jobs(
            cur, dataset_id, history_start, boundary
        )

        candidate_windows: list[tuple[datetime, datetime]] = []
        if step in {"hour", "day"}:
            current_week_start = _start_of_calendar_week(boundary)
            if boundary > current_week_start:
                candidate_windows.append(
                    (max(history_start, current_week_start), boundary)
                )

            candidate_end = current_week_start
            while (
                len(candidate_windows) < pending_limit and candidate_end > history_start
            ):
                candidate_start = max(history_start, candidate_end - timedelta(days=7))
                if candidate_start >= candidate_end:
                    break
                candidate_windows.append((candidate_start, candidate_end))
                candidate_end = candidate_start
        else:
            candidate_end = boundary
            while (
                len(candidate_windows) < pending_limit and candidate_end > history_start
            ):
                candidate_start = max(history_start, _retreat_step(candidate_end, step))
                if candidate_start >= candidate_end:
                    break
                candidate_windows.append((candidate_start, candidate_end))
                candidate_end = candidate_start

        for candidate_start, candidate_end in candidate_windows:
            status = existing_jobs.get((candidate_start, candidate_end))
            if status == "completed":
                continue

            if status in {"pending", "in_progress", "failed"}:
                open_window_count += 1
            else:
                cur.execute(
                    """
                    insert into ops.backfill_jobs (
                        dataset_id,
                        chunk_start_utc,
                        chunk_end_utc,
                        status,
                        updated_at
                    )
                    values (%s, %s, %s, 'pending', now())
                    on conflict (dataset_id, chunk_start_utc, chunk_end_utc) do nothing
                    """,
                    (dataset_id, candidate_start, candidate_end),
                )
                inserted += cur.rowcount
                open_window_count += 1
            if pending_limit > 0 and open_window_count >= pending_limit:
                break

        conn.commit()
    logger.info(
        "Backfill enqueue complete %s",
        format_log_fields(
            **current_airflow_log_fields(),
            dataset_id=dataset_id,
            inserted=inserted,
            pending_limit=pending_limit,
            step=step if step in {"month", "year"} else "calendar_week",
            history_start=history_start.isoformat(),
            chunk_end_utc=boundary.isoformat(),
        ),
    )
    return inserted


def claim_next_backfill_chunk(dataset_id: str) -> dict[str, Any] | None:
    """Claim the next newest pending backfill window for one dataset."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            with next_job as (
                select id
                from ops.backfill_jobs
                where dataset_id = %s
                  and (
                    status = 'pending'
                    or (status = 'failed' and attempt_count < %s)
                  )
                order by chunk_start_utc desc
                limit 1
                for update skip locked
            )
            update ops.backfill_jobs as job
            set status = 'in_progress',
                attempt_count = attempt_count + 1,
                started_at = now(),
                updated_at = now(),
                last_error = null
            from next_job
            where job.id = next_job.id
            returning job.id, job.dataset_id, job.chunk_start_utc, job.chunk_end_utc
            """,
            (dataset_id, BACKFILL_MAX_ATTEMPTS),
        )
        row = cur.fetchone()
        conn.commit()

    if row is None:
        logger.info(
            "No backfill chunk available %s",
            format_log_fields(**current_airflow_log_fields(), dataset_id=dataset_id),
        )
        return None

    dataset = get_dataset(dataset_id)
    step = (dataset.get("backfill") or {}).get("step", "day")
    job_id, claimed_dataset_id, chunk_start_utc, chunk_end_utc = row
    chunk_start_utc = chunk_start_utc.astimezone(timezone.utc)
    chunk_end_utc = chunk_end_utc.astimezone(timezone.utc)
    logger.info(
        "Claimed backfill chunk %s",
        format_log_fields(
            **current_airflow_log_fields(),
            dataset_id=claimed_dataset_id,
            job_id=job_id,
            chunk_start_utc=chunk_start_utc.isoformat(),
            chunk_end_utc=chunk_end_utc.isoformat(),
        ),
    )
    return {
        "id": job_id,
        "dataset_id": claimed_dataset_id,
        "chunk_start_utc": chunk_start_utc.isoformat(),
        "chunk_end_utc": chunk_end_utc.isoformat(),
        "chunk_start_cli": (
            _format_cli_hour(chunk_start_utc)
            if step in {"hour", "day"}
            else _format_cli_timestamp(chunk_start_utc)
        ),
        "chunk_end_cli": (
            _format_cli_hour(chunk_end_utc)
            if step in {"hour", "day"}
            else _format_cli_timestamp(chunk_end_utc)
        ),
    }


def has_backfill_chunk(chunk: dict[str, Any] | None) -> bool:
    """Return whether the previous claim step produced a backfill window."""

    return chunk is not None


def mark_backfill_completed(job_id: int) -> None:
    """Mark a claimed backfill job as completed."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            update ops.backfill_jobs
            set status = 'completed',
                completed_at = now(),
                updated_at = now(),
                last_error = null
            where id = %s
            """,
            (job_id,),
        )
        conn.commit()
    logger.info(
        "Marked backfill job complete %s",
        format_log_fields(**current_airflow_log_fields(), job_id=job_id),
    )


def mark_backfill_failed(job_id: int, error_message: str) -> None:
    """Mark a backfill job as failed and persist the task error excerpt."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            update ops.backfill_jobs
            set status = 'failed',
                updated_at = now(),
                last_error = %s
            where id = %s
            """,
            (error_message[:4000], job_id),
        )
        conn.commit()
    logger.error(
        "Marked backfill job failed %s",
        format_log_fields(
            **current_airflow_log_fields(), job_id=job_id, error=error_message[:4000]
        ),
    )


def has_completed_backfill(dataset_id: str) -> bool:
    """Return whether a dataset has completed at least one backfill chunk."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            select exists(
                select 1
                from ops.backfill_jobs
                where dataset_id = %s
                  and status = 'completed'
            )
            """,
            (dataset_id,),
        )
        return bool(cur.fetchone()[0])


def trigger_backfill_dag_if_idle(
    dataset_id: str, ignore_run_id: str | None = None
) -> bool:
    """Trigger the backfill DAG when pending backfill work exists and the DAG is idle."""

    backfill_dag_id = f"{dataset_id}_backfill"

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            select exists(
                select 1
                from ops.backfill_jobs
                where dataset_id = %s
                  and status in ('pending', 'failed')
            )
            """,
            (dataset_id,),
        )
        has_pending = bool(cur.fetchone()[0])

        if ignore_run_id:
            cur.execute(
                """
                select count(*)
                from dag_run
                where dag_id = %s
                  and state in ('queued', 'running')
                  and run_id <> %s
                """,
                (backfill_dag_id, ignore_run_id),
            )
        else:
            cur.execute(
                """
                select count(*)
                from dag_run
                where dag_id = %s
                  and state in ('queued', 'running')
                """,
                (backfill_dag_id,),
            )
        active_runs = int(cur.fetchone()[0])

    if not has_pending:
        logger.info(
            "Not triggering backfill DAG because no pending jobs exist %s",
            format_log_fields(
                **current_airflow_log_fields(),
                dataset_id=dataset_id,
                target_dag_id=backfill_dag_id,
            ),
        )
        return False

    if active_runs > 0:
        logger.info(
            "Not triggering backfill DAG because another run is active %s",
            format_log_fields(
                **current_airflow_log_fields(),
                dataset_id=dataset_id,
                target_dag_id=backfill_dag_id,
                active_runs=active_runs,
            ),
        )
        return False

    from airflow.api.common.trigger_dag import trigger_dag

    run_id = f"backfill_chain__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
    trigger_dag(
        dag_id=backfill_dag_id, run_id=run_id, conf={"trigger_source": "backfill_queue"}
    )
    logger.info(
        "Triggered backfill DAG because pending jobs exist and the DAG is idle %s",
        format_log_fields(
            **current_airflow_log_fields(),
            dataset_id=dataset_id,
            target_dag_id=backfill_dag_id,
            triggered_run_id=run_id,
        ),
    )
    return True
