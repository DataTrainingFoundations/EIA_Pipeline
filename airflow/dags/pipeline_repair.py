"""Bronze verification repair queue helpers for Airflow dataset DAGs."""

from __future__ import annotations

import logging
import os
from contextlib import closing
from datetime import datetime, timezone
from typing import Any

from pipeline_runtime import (
    _advance_step,
    _floor_to_step,
    _format_cli_hour,
    current_airflow_log_fields,
    db_connect,
    format_log_fields,
)

logger = logging.getLogger(__name__)
BRONZE_REPAIR_MAX_ATTEMPTS = int(os.getenv("AIRFLOW_BRONZE_REPAIR_MAX_ATTEMPTS", "5"))
BRONZE_REPAIR_STALE_MINUTES = int(
    os.getenv("AIRFLOW_BRONZE_REPAIR_STALE_MINUTES", "60")
)


def _load_existing_repair_jobs(
    cur, dataset_id: str
) -> dict[tuple[datetime, datetime], str]:
    """Load existing repair jobs so enqueue logic can reuse or skip windows."""

    cur.execute(
        """
        select hour_start_utc, hour_end_utc, status
        from ops.bronze_repair_jobs
        where dataset_id = %s
        """,
        (dataset_id,),
    )
    return {
        (
            hour_start_utc.astimezone(timezone.utc),
            hour_end_utc.astimezone(timezone.utc),
        ): status
        for hour_start_utc, hour_end_utc, status in cur.fetchall()
    }


def recover_stale_bronze_repair_jobs(dataset_id: str) -> int:
    """Mark stale repair jobs as failed so repair workers can claim them again."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            update ops.bronze_repair_jobs
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
                f"auto-recovered stale bronze repair job after {BRONZE_REPAIR_STALE_MINUTES} minutes",
                dataset_id,
                str(BRONZE_REPAIR_STALE_MINUTES),
                BRONZE_REPAIR_MAX_ATTEMPTS,
            ),
        )
        recovered = cur.rowcount
        conn.commit()
    if recovered:
        logger.warning(
            "Recovered stale bronze repair jobs %s",
            format_log_fields(
                **current_airflow_log_fields(),
                dataset_id=dataset_id,
                recovered=recovered,
                stale_minutes=BRONZE_REPAIR_STALE_MINUTES,
                max_attempts=BRONZE_REPAIR_MAX_ATTEMPTS,
            ),
        )
    return recovered


def enqueue_bronze_repair_jobs(
    dataset_id: str,
    *,
    statuses: tuple[str, ...] = ("missing",),
    max_pending_override: int | None = None,
) -> int:
    """Queue repair jobs for missing or partial Bronze coverage windows."""

    if not statuses:
        return 0

    pending_limit = (
        max_pending_override
        if max_pending_override is not None
        else int(os.getenv("AIRFLOW_BRONZE_REPAIR_MAX_PENDING", "24"))
    )
    boundary = _floor_to_step(datetime.now(timezone.utc), "hour")

    recover_stale_bronze_repair_jobs(dataset_id)

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        existing_jobs = _load_existing_repair_jobs(cur, dataset_id)
        cur.execute(
            """
            select hour_start_utc, status
            from ops.bronze_hourly_coverage
            where dataset_id = %s
              and status = any(%s)
              and hour_start_utc < %s
            order by hour_start_utc asc
            """,
            (dataset_id, list(statuses), boundary),
        )
        coverage_rows = [
            (hour_start_utc.astimezone(timezone.utc), coverage_status)
            for hour_start_utc, coverage_status in cur.fetchall()
        ]

        enqueued = 0
        open_window_count = 0
        for hour_start_utc, coverage_status in coverage_rows:
            hour_end_utc = _advance_step(hour_start_utc, "hour")
            existing_status = existing_jobs.get((hour_start_utc, hour_end_utc))

            if existing_status in {"pending", "in_progress"}:
                open_window_count += 1
                if pending_limit > 0 and open_window_count >= pending_limit:
                    break
                continue

            if pending_limit > 0 and open_window_count >= pending_limit:
                break

            cur.execute(
                """
                insert into ops.bronze_repair_jobs (
                    dataset_id,
                    hour_start_utc,
                    hour_end_utc,
                    coverage_status,
                    status,
                    updated_at,
                    coverage_verified_at
                )
                values (%s, %s, %s, %s, 'pending', now(), now())
                on conflict (dataset_id, hour_start_utc, hour_end_utc) do update
                set coverage_status = excluded.coverage_status,
                    status = case
                        when ops.bronze_repair_jobs.status in ('pending', 'in_progress') then ops.bronze_repair_jobs.status
                        else 'pending'
                    end,
                    updated_at = now(),
                    coverage_verified_at = excluded.coverage_verified_at,
                    last_error = case
                        when ops.bronze_repair_jobs.status in ('pending', 'in_progress') then ops.bronze_repair_jobs.last_error
                        else null
                    end,
                    started_at = case
                        when ops.bronze_repair_jobs.status in ('pending', 'in_progress') then ops.bronze_repair_jobs.started_at
                        else null
                    end,
                    completed_at = case
                        when ops.bronze_repair_jobs.status in ('pending', 'in_progress') then ops.bronze_repair_jobs.completed_at
                        else null
                    end
                """,
                (dataset_id, hour_start_utc, hour_end_utc, coverage_status),
            )
            enqueued += 1
            open_window_count += 1

        conn.commit()
    logger.info(
        "Bronze repair enqueue complete %s",
        format_log_fields(
            **current_airflow_log_fields(),
            dataset_id=dataset_id,
            enqueued=enqueued,
            pending_limit=pending_limit,
            statuses=",".join(statuses),
            chunk_end_utc=boundary.isoformat(),
        ),
    )
    return enqueued


def claim_next_bronze_repair_hour(dataset_id: str) -> dict[str, Any] | None:
    """Claim the next oldest repair hour for one dataset."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            with next_job as (
                select id
                from ops.bronze_repair_jobs
                where dataset_id = %s
                  and (
                    status = 'pending'
                    or (status = 'failed' and attempt_count < %s)
                  )
                order by hour_start_utc asc
                limit 1
                for update skip locked
            )
            update ops.bronze_repair_jobs as job
            set status = 'in_progress',
                attempt_count = attempt_count + 1,
                started_at = now(),
                updated_at = now(),
                last_error = null
            from next_job
            where job.id = next_job.id
            returning job.id, job.dataset_id, job.hour_start_utc, job.hour_end_utc, job.coverage_status
            """,
            (dataset_id, BRONZE_REPAIR_MAX_ATTEMPTS),
        )
        row = cur.fetchone()
        conn.commit()

    if row is None:
        logger.info(
            "No bronze repair hour available %s",
            format_log_fields(**current_airflow_log_fields(), dataset_id=dataset_id),
        )
        return None

    job_id, claimed_dataset_id, hour_start_utc, hour_end_utc, coverage_status = row
    hour_start_utc = hour_start_utc.astimezone(timezone.utc)
    hour_end_utc = hour_end_utc.astimezone(timezone.utc)
    logger.info(
        "Claimed bronze repair hour %s",
        format_log_fields(
            **current_airflow_log_fields(),
            dataset_id=claimed_dataset_id,
            job_id=job_id,
            chunk_start_utc=hour_start_utc.isoformat(),
            chunk_end_utc=hour_end_utc.isoformat(),
            coverage_status=coverage_status,
        ),
    )
    return {
        "id": job_id,
        "dataset_id": claimed_dataset_id,
        "chunk_start_utc": hour_start_utc.isoformat(),
        "chunk_end_utc": hour_end_utc.isoformat(),
        "chunk_start_cli": _format_cli_hour(hour_start_utc),
        "chunk_end_cli": _format_cli_hour(hour_end_utc),
        "coverage_status": coverage_status,
    }


def has_repair_chunk(chunk: dict[str, Any] | None) -> bool:
    """Return whether the previous claim step produced a repair hour."""

    return chunk is not None


def mark_bronze_repair_completed(job_id: int) -> None:
    """Mark a repair job as completed."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            update ops.bronze_repair_jobs
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
        "Marked bronze repair job complete %s",
        format_log_fields(**current_airflow_log_fields(), job_id=job_id),
    )


def mark_bronze_repair_failed(job_id: int, error_message: str) -> None:
    """Mark a repair job as failed and store the task error excerpt."""

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            update ops.bronze_repair_jobs
            set status = 'failed',
                updated_at = now(),
                last_error = %s
            where id = %s
            """,
            (error_message[:4000], job_id),
        )
        conn.commit()
    logger.error(
        "Marked bronze repair job failed %s",
        format_log_fields(
            **current_airflow_log_fields(), job_id=job_id, error=error_message[:4000]
        ),
    )


def trigger_repair_dag_if_idle(
    dataset_id: str, ignore_run_id: str | None = None
) -> bool:
    """Trigger the repair DAG when pending repair work exists and no run is active."""

    repair_dag_id = f"{dataset_id}_bronze_hourly_repair"

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            select exists(
                select 1
                from ops.bronze_repair_jobs
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
                (repair_dag_id, ignore_run_id),
            )
        else:
            cur.execute(
                """
                select count(*)
                from dag_run
                where dag_id = %s
                  and state in ('queued', 'running')
                """,
                (repair_dag_id,),
            )
        active_runs = int(cur.fetchone()[0])

    if not has_pending:
        logger.info(
            "Not triggering repair DAG because no pending jobs exist %s",
            format_log_fields(
                **current_airflow_log_fields(),
                dataset_id=dataset_id,
                target_dag_id=repair_dag_id,
            ),
        )
        return False

    if active_runs > 0:
        logger.info(
            "Not triggering repair DAG because another run is active %s",
            format_log_fields(
                **current_airflow_log_fields(),
                dataset_id=dataset_id,
                target_dag_id=repair_dag_id,
                active_runs=active_runs,
            ),
        )
        return False

    from airflow.api.common.trigger_dag import trigger_dag

    run_id = f"repair_chain__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
    trigger_dag(
        dag_id=repair_dag_id, run_id=run_id, conf={"trigger_source": "repair_queue"}
    )
    logger.info(
        "Triggered repair DAG because pending repair jobs exist and the DAG is idle %s",
        format_log_fields(
            **current_airflow_log_fields(),
            dataset_id=dataset_id,
            target_dag_id=repair_dag_id,
            triggered_run_id=run_id,
        ),
    )
    return True
