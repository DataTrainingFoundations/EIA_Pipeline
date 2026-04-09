from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

from pipeline.core import snowflake


def test_get_snowpark_session_and_close_session(fake_session, monkeypatch) -> None:
    created = {}

    class Builder:
        @staticmethod
        def configs(config):
            created["config"] = config

            class Created:
                @staticmethod
                def create():
                    return fake_session

            return Created()

    fake_session.close = lambda: created.setdefault("closed", True)
    monkeypatch.setattr(snowflake.Session, "builder", Builder)

    settings = SimpleNamespace(
        account="acct",
        user="user",
        password="pw",
        role="role",
        warehouse="wh",
        database="db",
        schema="schema",
    )
    session = snowflake.get_snowpark_session(settings)
    snowflake.close_session(session)

    assert created["config"]["database"] == "db"
    assert created["closed"] is True


def test_normalizers_and_type_helpers() -> None:
    assert snowflake.normalize_record_keys({"a-b": 1, "Two": 2}) == {"A_B": 1, "TWO": 2}
    assert isinstance(snowflake._snowpark_type("double"), snowflake.DoubleType)
    assert isinstance(snowflake._snowpark_type("int"), snowflake.LongType)
    assert isinstance(snowflake._snowpark_type("text"), snowflake.StringType)
    assert snowflake._coerce_raw_value("1.5", "double") == 1.5
    assert snowflake._coerce_raw_value("7", "int") == 7
    assert snowflake._coerce_raw_value(4, "string") == "4"
    assert snowflake._coerce_raw_value(None, "string") is None


def test_build_struct_and_write_raw_records(fake_session, monkeypatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 4, 9, 10, 30)

    monkeypatch.setattr(snowflake, "datetime", FixedDateTime)

    struct = snowflake._build_raw_struct_type(["A_B", "COUNT"], {"a-b": "string", "COUNT": "int"})
    assert struct.names == ["A_B", "COUNT"]

    written = snowflake.write_raw_records(
        fake_session,
        "RAW.TABLE",
        [{"a-b": "x", "count": "5"}],
        {"a-b": "string", "count": "int"},
    )

    assert written == 1
    rows, schema = fake_session.created[0]
    assert rows[0][0] == "x"
    assert rows[0][1] == 5
    assert schema.names[-1] == "_INGESTED_AT"


def test_rows_exist_and_table_checks_handle_success_and_failure(fake_session) -> None:
    fake_session.sql_results = [[{"N": 2}], [{"N": 0}], RuntimeError("boom")]
    assert snowflake.rows_exist_for_business_date(fake_session, "RAW.TABLE", "2026-04-09", frequency="hourly") is True
    assert snowflake.table_has_rows_for_partition_date(fake_session, "RAW.TABLE", "2026-04-09") is False
    assert snowflake.rows_exist_for_business_date(fake_session, "RAW.TABLE", "2026-04-01", frequency="monthly") is False


def test_table_exists_and_sql_literal(fake_session) -> None:
    fake_session.tables["DB.TABLE"] = SimpleNamespace(limit=lambda _n: SimpleNamespace(collect=lambda: []))
    assert snowflake.table_exists(fake_session, "DB.TABLE") is True
    assert snowflake.table_exists(fake_session, "MISSING.TABLE") is False
    assert snowflake._sql_literal("O'Hare") == "'O''Hare'"
    assert snowflake._sql_literal(None) == "NULL"


def test_pipeline_state_helpers(fake_session, monkeypatch) -> None:
    monkeypatch.setattr(snowflake, "ensure_pipeline_state_table", lambda session, database: "EIA_PIPELINE.META.PIPELINE_RUN_STATE")
    fake_session.sql_results = [
        [],
        [SimpleNamespace(as_dict=lambda: {"DATASET_ID": "ds", "BOOTSTRAP_INGEST_COMPLETE": True})],
    ]
    default_state = snowflake.get_pipeline_state(fake_session, "EIA_PIPELINE", "ds")
    loaded_state = snowflake.get_pipeline_state(fake_session, "EIA_PIPELINE", "ds")

    assert default_state["dataset_id"] == "ds"
    assert loaded_state["bootstrap_ingest_complete"] is True


def test_ensure_pipeline_state_table_and_upserts_emit_sql(fake_session) -> None:
    table_name = snowflake.ensure_pipeline_state_table(fake_session, "EIA_PIPELINE")
    assert table_name == "EIA_PIPELINE.META.PIPELINE_RUN_STATE"
    assert "CREATE TABLE IF NOT EXISTS" in fake_session.sql_calls[0]
    assert any("ADD COLUMN IF NOT EXISTS dataset_id" in call for call in fake_session.sql_calls[1:])

    fake_session.sql_calls.clear()
    snowflake.upsert_pipeline_state(
        fake_session,
        "EIA_PIPELINE",
        "ds",
        frequency="hourly",
        bootstrap_ingest_complete=True,
        bootstrap_transform_complete=False,
        last_raw_partition="2026-04-09",
        last_silver_partition=None,
        last_gold_partition=None,
        last_ingest_started_at="2026-04-09T00:00:00",
        last_ingest_succeeded_at=None,
        last_transform_started_at=None,
        last_transform_succeeded_at=None,
        last_ingest_error_message=None,
        last_transform_error_message="bad",
    )
    assert "MERGE INTO EIA_PIPELINE.META.PIPELINE_RUN_STATE" in fake_session.sql_calls[-1]


def test_update_ingest_and_transform_state_reuse_prior_values(monkeypatch) -> None:
    captured = []
    prior_state = {
        "bootstrap_transform_complete": True,
        "bootstrap_ingest_complete": False,
        "last_raw_partition": date(2026, 4, 1),
        "last_silver_partition": date(2026, 4, 2),
        "last_gold_partition": date(2026, 4, 3),
        "last_transform_started_at": datetime(2026, 4, 4, 1, 0),
        "last_transform_succeeded_at": datetime(2026, 4, 4, 2, 0),
        "last_ingest_started_at": datetime(2026, 4, 5, 1, 0),
        "last_ingest_succeeded_at": datetime(2026, 4, 5, 2, 0),
        "last_transform_error_message": "old",
        "last_ingest_error_message": "older",
    }
    monkeypatch.setattr(snowflake, "get_pipeline_state", lambda *args, **kwargs: dict(prior_state))
    monkeypatch.setattr(snowflake, "upsert_pipeline_state", lambda *args, **kwargs: captured.append(kwargs))

    snowflake.update_ingest_state(
        None,
        "EIA_PIPELINE",
        "ds",
        frequency="hourly",
        bootstrap_ingest_complete=True,
        last_raw_partition=None,
        last_ingest_started_at="new_start",
        last_ingest_succeeded_at="new_end",
        last_ingest_error_message="err",
    )
    snowflake.update_transform_state(
        None,
        "EIA_PIPELINE",
        "ds",
        frequency="hourly",
        bootstrap_transform_complete=False,
        last_silver_partition=None,
        last_gold_partition=None,
        last_transform_started_at="ts1",
        last_transform_succeeded_at="ts2",
        last_transform_error_message="boom",
    )

    assert captured[0]["last_raw_partition"] == "2026-04-01"
    assert captured[0]["last_transform_error_message"] == "old"
    assert captured[1]["last_ingest_started_at"] == "2026-04-05T01:00:00"
    assert captured[1]["last_transform_error_message"] == "boom"


def test_partition_queries_and_result_normalization(fake_session, monkeypatch) -> None:
    monkeypatch.setattr(snowflake, "table_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(snowflake, "current_partition_for_frequency", lambda frequency: "2026-04-09")
    fake_session.sql_results = [
        [
            {"PARTITION_DATE": date(2026, 4, 1), "PROCESSED_AT": datetime(2026, 4, 2, 3, 0), "ROW_COUNT": 5},
        ],
        [
            {
                "PARTITION_DATE": date(2026, 4, 1),
                "PROCESSED_AT": datetime(2026, 4, 2, 3, 0),
                "ROW_COUNT": 24,
                "DISTINCT_PERIOD_COUNT": 24,
            },
            {
                "PARTITION_DATE": date(2026, 4, 9),
                "PROCESSED_AT": datetime(2026, 4, 9, 3, 0),
                "ROW_COUNT": 4,
                "DISTINCT_PERIOD_COUNT": 4,
            },
        ],
        [
            {"PARTITION_DATE": date(2026, 4, 1), "PROCESSED_AT": datetime(2026, 4, 2, 3, 0), "ROW_COUNT": 5},
        ],
    ]

    status = snowflake.list_partition_status(
        fake_session,
        "DB.TABLE",
        frequency="monthly",
        processed_column="gold_processed_at",
        start_date="2026-04-15",
        end_date="2026-05-20",
    )
    raw = snowflake.raw_partitions_for_dataset(fake_session, "DB.RAW", frequency="hourly")
    mapped = snowflake.pipeline_partitions_for_table(
        fake_session,
        "DB.SILVER",
        frequency="hourly",
        processed_column="silver_processed_at",
    )

    assert status[0]["partition_date"] == "2026-04-01"
    assert raw[0]["is_complete"] is True
    assert raw[1]["is_complete"] is True
    assert mapped["2026-04-01"]["row_count"] == 5
    assert snowflake.raw_partition_expression("monthly") == "TO_DATE(PERIOD || '-01')"
    assert snowflake.current_partition_for_frequency("hourly")
    assert snowflake._normalize_result_value(date(2026, 4, 1)) == "2026-04-01"
    assert snowflake._normalize_result_value(None) is None
