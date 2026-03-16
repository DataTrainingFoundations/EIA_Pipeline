"""CLI entrypoint for pulling EIA rows and publishing Kafka events.

This file stays as the stable command-line entrypoint used by Airflow and local
scripts. The heavy lifting lives in helper modules so teammates can follow the
fetch flow one step at a time.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.eia_api import fetch_dataset_rows
from src.event_factory import build_event
from src.publish_kafka import publish_events
from src.registry import load_dataset_registry

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the ingestion fetch command using [start, end) windows."""

    now = datetime.now(timezone.utc)
    default_end = now.replace(minute=0, second=0, microsecond=0)
    default_start = default_end - timedelta(hours=24)

    parser = argparse.ArgumentParser(description="Fetch EIA data and publish to Kafka using [start, end) source windows.")
    parser.add_argument("--dataset", help="Dataset id in registry; if omitted all datasets run.")
    parser.add_argument("--start", default=default_start.strftime("%Y-%m-%dT%H"), help="Inclusive UTC hour boundary for the source window.")
    parser.add_argument("--end", default=default_end.strftime("%Y-%m-%dT%H"), help="Exclusive UTC hour boundary for the source window.")
    parser.add_argument("--page-size", type=int, default=5000)
    parser.add_argument("--max-pages", type=int, default=500)
    parser.add_argument("--respondent", help="Optional respondent filter, example PJM.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _select_datasets(
    registry: dict[str, dict[str, Any]],
    dataset_id: str | None,
) -> list[dict[str, Any]]:
    """Return either one requested dataset or every registered dataset."""

    if dataset_id is None:
        return list(registry.values())
    if dataset_id not in registry:
        available = ", ".join(sorted(registry.keys()))
        raise ValueError(f"Unknown dataset '{dataset_id}'. Available: {available}")
    return [registry[dataset_id]]


def main() -> None:
    """Fetch EIA rows, build Bronze event envelopes, and publish them to Kafka."""

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise RuntimeError("EIA_API_KEY is required.")
    ingestion_run_id = datetime.now(timezone.utc).strftime("fetch_%Y%m%dT%H%M%S%fZ")

    registry_path = Path(__file__).with_name("dataset_registry.yml")
    registry = load_dataset_registry(registry_path)
    datasets = _select_datasets(registry, args.dataset)

    for dataset in datasets:
        rows = fetch_dataset_rows(
            api_key=api_key,
            dataset_config=dataset,
            start=args.start,
            end=args.end,
            page_size=args.page_size,
            max_pages=args.max_pages,
            respondent=args.respondent,
        )
        logger.info(
            "Fetched dataset window dataset_id=%s route=%s topic=%s source_window_start=%s source_window_end=%s page_size=%s max_pages=%s rows_fetched=%s ingestion_run_id=%s",
            dataset["id"],
            dataset["route"],
            dataset["topic"],
            args.start,
            args.end,
            args.page_size,
            args.max_pages,
            len(rows),
            ingestion_run_id,
        )

        events: list[dict[str, Any]] = []
        invalid_rows: list[str] = []
        for row in rows:
            try:
                events.append(
                    build_event(
                        dataset_id=dataset["id"],
                        route=dataset["route"],
                        row=row,
                        frequency=dataset.get("frequency", "hourly"),
                        ingestion_run_id=ingestion_run_id,
                        source_window_start=args.start,
                        source_window_end=args.end,
                    )
                )
            except ValueError as exc:
                invalid_rows.append(str(exc))

        if invalid_rows:
            sample_errors = "; ".join(invalid_rows[:5])
            raise RuntimeError(
                f"Validation failed for dataset '{dataset['id']}' with {len(invalid_rows)} invalid rows. "
                f"Sample errors: {sample_errors}"
            )

        if args.dry_run:
            logger.info(
                "Dry run complete dataset_id=%s route=%s topic=%s source_window_start=%s source_window_end=%s rows_fetched=%s events_built=%s invalid_rows=%s ingestion_run_id=%s",
                dataset["id"],
                dataset["route"],
                dataset["topic"],
                args.start,
                args.end,
                len(rows),
                len(events),
                len(invalid_rows),
                ingestion_run_id,
            )
            continue

        sent = publish_events(topic=dataset["topic"], events=events)
        logger.info(
            "Published dataset events dataset_id=%s route=%s topic=%s source_window_start=%s source_window_end=%s rows_fetched=%s events_built=%s invalid_rows=%s published=%s ingestion_run_id=%s",
            dataset["id"],
            dataset["route"],
            dataset["topic"],
            args.start,
            args.end,
            len(rows),
            len(events),
            len(invalid_rows),
            sent,
            ingestion_run_id,
        )


if __name__ == "__main__":
    main()
