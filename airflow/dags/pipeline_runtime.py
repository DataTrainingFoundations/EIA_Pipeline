"""Shared runtime helpers for Airflow DAG modules.

This module contains path resolution, dataset registry access, database
connections, log formatting, and Spark submit command rendering. The queue,
repair, validation, and DAG-factory modules all build on these helpers.
"""

from __future__ import annotations

import logging
import os
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import yaml

SUPPORTED_BACKFILL_STEPS = {"hour", "day", "month", "year"}
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_IVY_CACHE = os.getenv("SPARK_IVY_CACHE", "/tmp/.ivy")
SPARK_SUBMIT_TOTAL_CORES = os.getenv("SPARK_SUBMIT_TOTAL_CORES", "4")
SPARK_SUBMIT_EXECUTOR_CORES = os.getenv("SPARK_SUBMIT_EXECUTOR_CORES", "2")
logger = logging.getLogger(__name__)


def _workspace_root() -> Path:
    """Return the workspace root used by Airflow containers."""

    return Path(os.getenv("WORKSPACE_ROOT", "/workspace"))


def _spark_jobs_root() -> Path:
    """Return the folder that contains Spark job entrypoints."""

    return _workspace_root() / "spark" / "jobs"


def load_dataset_registry() -> dict[str, dict[str, Any]]:
    """Load the dataset registry that drives dataset-specific DAG behavior."""

    registry_path = _workspace_root() / "ingestion" / "src" / "dataset_registry.yml"
    with registry_path.open("r", encoding="utf-8") as registry_file:
        payload = yaml.safe_load(registry_file) or {}
    datasets = payload.get("datasets", [])
    return {dataset["id"]: dataset for dataset in datasets}


def get_dataset(dataset_id: str) -> dict[str, Any]:
    """Return one dataset config from the shared ingestion registry."""

    registry = load_dataset_registry()
    if dataset_id not in registry:
        available = ", ".join(sorted(registry))
        raise ValueError(
            f"Unknown dataset '{dataset_id}'. Available datasets: {available}"
        )
    return registry[dataset_id]


def db_connect():
    """Open a Postgres connection to the shared warehouse database."""

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "platform"),
        user=os.getenv("POSTGRES_USER", "platform"),
        password=os.getenv("POSTGRES_PASSWORD", "platform"),
    )


def split_table_name(table_name: str) -> tuple[str, str]:
    """Split `schema.table` names while defaulting bare names to `public`."""

    if "." not in table_name:
        return "public", table_name
    return tuple(table_name.split(".", 1))  # type: ignore[return-value]


def format_log_fields(**fields: Any) -> str:
    """Render non-empty logging fields in a stable key=value format."""

    rendered_parts = []
    for key, value in fields.items():
        if value is None or value == "":
            continue
        rendered_parts.append(f"{key}={value}")
    return " ".join(rendered_parts)


def current_airflow_log_fields() -> dict[str, Any]:
    """Best-effort lookup of the current Airflow task context for logging."""

    try:
        from airflow.operators.python import get_current_context

        context = get_current_context()
    except Exception:  # pragma: no cover - only available during Airflow runs
        return {}

    task = context.get("task")
    dag = context.get("dag")
    return {
        "dag_id": getattr(dag, "dag_id", None),
        "task_id": getattr(task, "task_id", None),
        "run_id": context.get("run_id"),
    }


def build_spark_submit_command(
    application_file: str,
    *,
    packages: str | None = None,
    application_args: list[str] | None = None,
    job_name: str | None = None,
) -> str:
    """Build the shell command Airflow uses to submit one Spark job.

    The local compose stack runs only two Spark workers. Capping per-job cores
    keeps the first scheduled DAG wave from monopolizing the whole cluster.
    """

    ivy_cache_dir = SPARK_IVY_CACHE
    command = [
        "mkdir",
        "-p",
        ivy_cache_dir,
        "&&",
        "spark-submit",
        "--master",
        SPARK_MASTER_URL,
        "--conf",
        f"spark.jars.ivy={ivy_cache_dir}",
        "--conf",
        "spark.pyspark.python=python3",
        "--conf",
        "spark.pyspark.driver.python=python3",
        "--conf",
        f"spark.cores.max={SPARK_SUBMIT_TOTAL_CORES}",
        "--conf",
        f"spark.executor.cores={SPARK_SUBMIT_EXECUTOR_CORES}",
    ]
    if job_name:
        command.extend(["--name", job_name])
    if packages:
        command.extend(["--packages", packages])
    application_path = str(_spark_jobs_root() / application_file)
    command.append(application_path)
    command.extend(application_args or [])
    rendered_command = " ".join(_shell_quote_command_part(part) for part in command)
    logger.info(
        "Prepared spark-submit command %s",
        format_log_fields(
            application=application_path,
            job_name=job_name or application_file,
            master=SPARK_MASTER_URL,
            packages=packages,
        ),
    )
    return rendered_command


def _shell_quote_command_part(part: str) -> str:
    """Quote one shell argument while preserving Airflow template expressions."""

    if part in {"&&", "||"}:
        return part
    if "{{" in part or "}}" in part or "${" in part:
        escaped = part.replace('"', '\\"')
        return f'"{escaped}"'
    return shlex.quote(part)


def _parse_utc(value: str | datetime) -> datetime:
    """Parse a string or datetime and normalize it to UTC."""

    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _floor_to_step(value: datetime, step: str) -> datetime:
    """Round a datetime down to the beginning of the configured step."""

    if step == "hour":
        return value.replace(minute=0, second=0, microsecond=0)
    if step == "day":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if step == "month":
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if step == "year":
        return value.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unsupported step '{step}'")


def _advance_step(value: datetime, step: str) -> datetime:
    """Move one scheduling step forward while preserving UTC semantics."""

    if step == "hour":
        return value.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    if step == "day":
        return value.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
            days=1
        )
    if step == "month":
        year = value.year + (1 if value.month == 12 else 0)
        month = 1 if value.month == 12 else value.month + 1
        return value.replace(
            year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0
        )
    if step == "year":
        try:
            return value.replace(year=value.year + 1)
        except ValueError:
            return value.replace(month=2, day=28, year=value.year + 1)
    raise ValueError(f"Unsupported step '{step}'")


def _retreat_step(value: datetime, step: str) -> datetime:
    """Move one scheduling step backward while preserving UTC semantics."""

    if step == "hour":
        return value - timedelta(hours=1)
    if step == "day":
        return value - timedelta(days=1)
    if step == "month":
        year = value.year - (1 if value.month == 1 else 0)
        month = 12 if value.month == 1 else value.month - 1
        return value.replace(year=year, month=month, day=1)
    if step == "year":
        try:
            return value.replace(year=value.year - 1)
        except ValueError:
            return value.replace(month=2, day=28, year=value.year - 1)
    raise ValueError(f"Unsupported step '{step}'")


def _format_cli_hour(value: datetime) -> str:
    """Format a UTC datetime the way ingestion CLI expects hourly boundaries."""

    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H")


def _format_cli_timestamp(value: datetime) -> str:
    """Format a UTC datetime as an ISO timestamp for non-hourly ingestion windows."""

    return value.astimezone(timezone.utc).isoformat()
