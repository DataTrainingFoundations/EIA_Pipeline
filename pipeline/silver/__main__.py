from __future__ import annotations

import argparse

from pipeline.core.settings import load_snowflake_settings
from pipeline.core.snowflake import close_session, get_snowpark_session
from pipeline.silver.transform import run_silver


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver: Snowflake RAW -> Snowflake SILVER")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["electricity_generation", "electricity_demand"],
    )
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()

    settings = load_snowflake_settings()
    session = get_snowpark_session(settings)
    try:
        run_silver(session, args.dataset, args.date, settings.database)
    finally:
        close_session(session)


if __name__ == "__main__":
    main()
