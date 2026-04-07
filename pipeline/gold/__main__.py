from __future__ import annotations

import argparse

from pipeline.core.settings import load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session
from pipeline.gold.transform import run_gold


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold: Snowflake SILVER -> Snowflake GOLD")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    parser.add_argument("--scope", choices=["hourly", "monthly", "all"], default="all")
    args = parser.parse_args()

    settings = load_snowflake_settings(schema="GOLD")
    session = get_snowpark_session(settings)
    try:
        run_gold(session, args.date, settings.database, scope=args.scope)
    finally:
        close_session(session)


if __name__ == "__main__":
    main()
