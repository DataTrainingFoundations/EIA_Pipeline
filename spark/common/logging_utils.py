"""Shared logging helpers for Spark jobs.

These helpers keep job logs readable in Airflow by using stable key=value
context fields for starts, skips, and completions.
"""

from __future__ import annotations

import logging
import os
from typing import Any


def configure_logging() -> None:
    """Configure the default log format for Spark job entrypoints."""

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")


def format_log_fields(**fields: Any) -> str:
    """Render non-empty logging fields in a stable key=value format."""

    parts = []
    for key, value in fields.items():
        if value is None or value == "":
            continue
        parts.append(f"{key}={value}")
    return " ".join(parts)


def log_job_start(logger: logging.Logger, job_name: str, **fields: Any) -> None:
    """Log the start of a Spark job with the context Airflow operators need."""

    logger.info("Spark job starting %s", format_log_fields(job_name=job_name, **fields))


def log_job_complete(logger: logging.Logger, job_name: str, **fields: Any) -> None:
    """Log the successful completion of a Spark job with summary metrics."""

    logger.info("Spark job complete %s", format_log_fields(job_name=job_name, **fields))
