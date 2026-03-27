"""
publish_snowflake.py
====================
Snowflake writer utility — replaces publish_kafka.py.

Uses a Snowpark Session to write raw EIA records directly into a Snowflake
table.  Each dataset in dataset_registry.yml maps to one raw table
(configured via the `snowflake_table` field).

TABLE SCHEMA
------------
Raw tables are created on first write (if they don't exist) using
`mode="append"` with `table_type="transient"` so they don't consume
Snowflake Time Travel storage.  The schema is inferred from the records:

    PERIOD          VARCHAR
    RESPONDENT      VARCHAR
    RESPONDENT_NAME VARCHAR
    FUELTYPE        VARCHAR   (generation only)
    TYPE            VARCHAR   (demand only)
    TYPE_NAME       VARCHAR
    VALUE           FLOAT
    VALUE_UNITS     VARCHAR
    _DATASET_ID     VARCHAR
    _FETCHED_AT     VARCHAR
    _INGESTED_AT    TIMESTAMP_NTZ   (added by this module)

Column names are uppercased and hyphens replaced with underscores to match
Snowflake identifier conventions.

ENVIRONMENT VARIABLES
---------------------
SNOWFLAKE_ACCOUNT    — <orgname>-<account_name>
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE       — defaults to SYSADMIN
SNOWFLAKE_WAREHOUSE  — defaults to COMPUTE_WH
SNOWFLAKE_DATABASE   — defaults to EIA
SNOWFLAKE_SCHEMA     — defaults to RAW
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def get_session():
    """Build and return a Snowpark Session from environment variables."""
    from snowflake.snowpark import Session

    connection_params = {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "EIA"),
        "schema":    os.environ.get("SNOWFLAKE_SCHEMA",    "RAW"),
    }
    session = Session.builder.configs(connection_params).create()
    logger.info(
        "Snowpark session opened → %s.%s",
        connection_params["database"],
        connection_params["schema"],
    )
    return session


def _normalise_keys(record: dict[str, Any]) -> dict[str, Any]:
    """
    Uppercase all keys and replace hyphens with underscores so they are
    valid Snowflake column identifiers.
    e.g. "respondent-name" → "RESPONDENT_NAME"
    """
    return {k.upper().replace("-", "_"): v for k, v in record.items()}


def write_records(
    session,
    table_name: str,
    records: list[dict[str, Any]],
) -> int:
    """
    Write a list of record dicts to a Snowflake table using Snowpark.

    - Column names are normalised (upper + hyphens → underscores).
    - An _INGESTED_AT timestamp column is added to every row.
    - The table is created if it does not exist (inferred schema).
    - Writes are appended so multiple ingest runs accumulate; deduplication
      happens in the silver Spark job.

    Args:
        session:     An active Snowpark Session.
        table_name:  Fully-qualified or schema-relative table name,
                     e.g. "ELECTRICITY_GENERATION" or "EIA.RAW.ELECTRICITY_GENERATION".
        records:     List of raw record dicts from the EIA API.

    Returns:
        Number of rows written.
    """
    if not records:
        logger.warning("write_records called with empty list for table '%s'", table_name)
        return 0

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    normalised = []
    for rec in records:
        row = _normalise_keys(rec)
        row["_INGESTED_AT"] = ingested_at
        normalised.append(row)

    df = session.create_dataframe(normalised)

    (
        df.write
        .mode("append")
        .save_as_table(
            table_name,
            table_type="transient",   # no Time Travel — saves storage cost
            column_order="name",      # match by column name, not position
        )
    )

    logger.info("Wrote %d rows → Snowflake table '%s'", len(normalised), table_name)
    return len(normalised)


def close_session(session) -> None:
    """Close the Snowpark session."""
    session.close()
    logger.info("Snowpark session closed.")
