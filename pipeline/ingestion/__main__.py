from __future__ import annotations

import os

from pipeline.core.settings import load_eia_settings, load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session
from pipeline.ingestion.raw_ingest import run_ingestion


def _read_optional_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if value.lower() == "none":
        return ""
    return value


def main() -> None:
    snowflake_settings = load_snowflake_settings()
    eia_settings = load_eia_settings()
    session = get_snowpark_session(snowflake_settings)
    try:
        run_ingestion(
            session,
            eia_settings,
            target_dataset_id=_read_optional_env("TARGET_DATASET_ID"),
            start_date=_read_optional_env("BACKFILL_START_DATE"),
            end_date=_read_optional_env("BACKFILL_END_DATE"),
            rolling_hours=_read_optional_env("ROLLING_HOURS"),
            default_rolling_hours=eia_settings.default_rolling_hours,
        )
    finally:
        close_session(session)


if __name__ == "__main__":
    main()
