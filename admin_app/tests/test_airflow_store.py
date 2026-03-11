from __future__ import annotations

from pathlib import Path
from airflow_store import AirflowStore, expected_lag_for_dag, extract_error_excerpt
from config import AppConfig


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="platform",
        postgres_user="platform",
        postgres_password="platform",
        minio_endpoint="minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
        eia_api_key="test",
        app_timezone="UTC",
        api_cache_ttl_seconds=300,
        query_timeout_seconds=60,
        default_days=7,
        airflow_log_dir=tmp_path,
        registry_path=tmp_path / "dataset_registry.yml",
    )


def test_extract_error_excerpt_and_read_task_log(tmp_path: Path) -> None:
    log_path = tmp_path / "dag_id=test_dag" / "run_id=scheduled__20260310T180500Z" / "task_id=test_task"
    log_path.mkdir(parents=True)
    log_file = log_path / "attempt=1.log"
    log_file.write_text(
        "\n".join(
            [
                "INFO starting",
                "Traceback (most recent call last):",
                "ValueError: failure",
            ]
        ),
        encoding="utf-8",
    )

    store = AirflowStore(_config(tmp_path))
    record = store.read_task_log("test_dag", "scheduled__20260310T180500Z", "test_task", 1)
    assert record is not None
    assert "ValueError: failure" in (record.error_excerpt or "")


def test_expected_lag_and_fallback_excerpt() -> None:
    assert expected_lag_for_dag("electricity_region_data_backfill").seconds == 1200
    excerpt = extract_error_excerpt("line1\nline2\nline3")
    assert excerpt == "line1\nline2\nline3"


def test_get_dag_health_uses_escaped_like_patterns(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    captured: dict[str, str] = {}

    def fake_read_sql(self, query: str, params=None):  # noqa: ANN001,ARG001
        captured["query"] = query
        import pandas as pd

        return pd.DataFrame(
            [
                {
                    "dag_id": "electricity_region_data_incremental",
                    "is_paused": False,
                    "latest_run_state": "success",
                    "latest_run_start": None,
                    "latest_run_end": None,
                }
            ]
        )

    monkeypatch.setattr(AirflowStore, "read_sql", fake_read_sql)

    store = AirflowStore(_config(tmp_path))
    store.get_dag_health()

    assert "electricity_%%" in captured["query"]
    assert "platinum_%%" in captured["query"]


def test_read_sql_omits_params_when_not_provided(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    execute_calls: list[tuple[object, ...]] = []

    class FakeCursor:
        description = [("dag_id",)]

        def execute(self, *args) -> None:  # noqa: ANN002
            execute_calls.append(args)

        def fetchall(self):
            return [("electricity_region_data_incremental",)]

        def close(self) -> None:
            pass

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            pass

    monkeypatch.setattr(AirflowStore, "_connection", lambda self: FakeConnection())

    store = AirflowStore(_config(tmp_path))
    df = store.read_sql("select dag_id from dag")

    assert df["dag_id"].tolist() == ["electricity_region_data_incremental"]
    assert execute_calls == [("select dag_id from dag",)]
