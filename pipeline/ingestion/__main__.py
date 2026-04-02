from __future__ import annotations

import os

from pipeline.core.settings import load_eia_settings, load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session
from pipeline.ingestion.raw_ingest import run_ingestion


def main() -> None:
    snowflake_settings = load_snowflake_settings()
    eia_settings = load_eia_settings()
    session = get_snowpark_session(snowflake_settings)
    try:
        run_ingestion(
            session,
            eia_settings,
            target_dataset_id=os.environ.get("TARGET_DATASET_ID", "").strip(),
            start_date=os.environ.get("BACKFILL_START_DATE", "").strip(),
            end_date=os.environ.get("BACKFILL_END_DATE", "").strip(),
            rolling_hours=os.environ.get("ROLLING_HOURS", "").strip(),
            default_rolling_hours=eia_settings.default_rolling_hours,
        )
    finally:
        close_session(session)


if __name__ == "__main__":
    main()
