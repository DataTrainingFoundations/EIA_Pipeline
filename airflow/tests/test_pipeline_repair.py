from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pipeline_repair


class FakeCursor:
    def __init__(self, *, fetchone_values=None, fetchall_values=None, rowcount: int = 0) -> None:
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.rowcount = rowcount
        self.executed: list[tuple[str, object]] = []

    def execute(self, query, params=None) -> None:  # noqa: ANN001
        self.executed.append((str(query), params))

    def fetchone(self):
        if self.fetchone_values:
            return self.fetchone_values.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_values:
            return self.fetchall_values.pop(0)
        return []

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.committed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        return None


def test_load_existing_repair_jobs_normalizes_timezones() -> None:
    cursor = FakeCursor(
        fetchall_values=[
            [
                (
                    datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc),
                    "pending",
                )
            ]
        ]
    )

    jobs = pipeline_repair._load_existing_repair_jobs(cursor, "electricity_region_data")

    assert jobs == {
        (
            datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc),
        ): "pending"
    }


def test_recover_stale_bronze_repair_jobs_returns_rowcount(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor(rowcount=2)
    conn = FakeConnection(cursor)
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: conn)

    recovered = pipeline_repair.recover_stale_bronze_repair_jobs("electricity_region_data")

    assert recovered == 2
    assert conn.committed is True


def test_enqueue_bronze_repair_jobs_returns_zero_when_statuses_are_empty() -> None:
    assert pipeline_repair.enqueue_bronze_repair_jobs("electricity_region_data", statuses=()) == 0


def test_enqueue_bronze_repair_jobs_respects_existing_pending_windows(monkeypatch) -> None:  # noqa: ANN001
    coverage_hour = datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc)
    cursor = FakeCursor(
        fetchall_values=[
            [
                (
                    coverage_hour,
                    datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc),
                    "pending",
                )
            ],
            [(coverage_hour, "missing")],
        ]
    )
    conn = FakeConnection(cursor)
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: conn)
    monkeypatch.setattr(pipeline_repair, "recover_stale_bronze_repair_jobs", lambda _dataset_id: 0)
    monkeypatch.setattr(
        pipeline_repair,
        "_floor_to_step",
        lambda _dt, _step: datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc),
    )

    enqueued = pipeline_repair.enqueue_bronze_repair_jobs(
        "electricity_region_data",
        statuses=("missing", "partial"),
        max_pending_override=1,
    )

    assert enqueued == 0
    assert conn.committed is True


def test_enqueue_bronze_repair_jobs_requeues_failed_windows(monkeypatch) -> None:  # noqa: ANN001
    coverage_hour = datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc)
    cursor = FakeCursor(
        fetchall_values=[
            [
                (
                    coverage_hour,
                    datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc),
                    "failed",
                )
            ],
            [(coverage_hour, "partial")],
        ]
    )
    conn = FakeConnection(cursor)
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: conn)
    monkeypatch.setattr(pipeline_repair, "recover_stale_bronze_repair_jobs", lambda _dataset_id: 0)
    monkeypatch.setattr(
        pipeline_repair,
        "_floor_to_step",
        lambda _dt, _step: datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc),
    )

    enqueued = pipeline_repair.enqueue_bronze_repair_jobs(
        "electricity_region_data",
        statuses=("missing", "partial"),
        max_pending_override=2,
    )

    assert enqueued == 1
    assert "insert into ops.bronze_repair_jobs" in cursor.executed[-1][0].lower()


def test_claim_next_bronze_repair_hour_returns_none_when_empty(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor(fetchone_values=[None])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(cursor))

    assert pipeline_repair.claim_next_bronze_repair_hour("electricity_region_data") is None


def test_claim_next_bronze_repair_hour_returns_cli_fields(monkeypatch) -> None:  # noqa: ANN001
    row = (
        7,
        "electricity_region_data",
        datetime(2026, 3, 10, 5, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 10, 6, 0, tzinfo=timezone.utc),
        "missing",
    )
    cursor = FakeCursor(fetchone_values=[row])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(cursor))

    claimed = pipeline_repair.claim_next_bronze_repair_hour("electricity_region_data")

    assert claimed == {
        "id": 7,
        "dataset_id": "electricity_region_data",
        "chunk_start_utc": "2026-03-10T05:00:00+00:00",
        "chunk_end_utc": "2026-03-10T06:00:00+00:00",
        "chunk_start_cli": "2026-03-10T05",
        "chunk_end_cli": "2026-03-10T06",
        "coverage_status": "missing",
    }


def test_has_repair_chunk_reflects_presence() -> None:
    assert pipeline_repair.has_repair_chunk(None) is False
    assert pipeline_repair.has_repair_chunk({"id": 1}) is True


def test_mark_bronze_repair_completed_updates_job(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: conn)

    pipeline_repair.mark_bronze_repair_completed(42)

    assert conn.committed is True
    assert cursor.executed[0][1] == (42,)


def test_mark_bronze_repair_failed_truncates_error_message(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: conn)

    pipeline_repair.mark_bronze_repair_failed(42, "x" * 5000)

    assert conn.committed is True
    params = cursor.executed[0][1]
    assert len(params[0]) == 4000
    assert params[1] == 42


def test_trigger_repair_dag_if_idle_returns_false_when_no_pending_work(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor(fetchone_values=[(False,), (0,)])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(cursor))

    assert pipeline_repair.trigger_repair_dag_if_idle("electricity_region_data") is False


def test_trigger_repair_dag_if_idle_returns_false_when_run_active(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor(fetchone_values=[(True,), (1,)])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(cursor))

    assert pipeline_repair.trigger_repair_dag_if_idle("electricity_region_data") is False


def test_trigger_repair_dag_if_idle_triggers_when_pending_and_idle(monkeypatch) -> None:  # noqa: ANN001
    triggered: list[dict] = []
    cursor = FakeCursor(fetchone_values=[(True,), (0,)])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(cursor))
    monkeypatch.setitem(
        sys.modules,
        "airflow.api.common.trigger_dag",
        types.SimpleNamespace(trigger_dag=lambda **kwargs: triggered.append(kwargs)),
    )

    result = pipeline_repair.trigger_repair_dag_if_idle("electricity_region_data")

    assert result is True
    assert triggered[0]["dag_id"] == "electricity_region_data_bronze_hourly_repair"


def test_trigger_repair_dag_if_idle_supports_ignore_run_id(monkeypatch) -> None:  # noqa: ANN001
    triggered: list[dict] = []
    cursor = FakeCursor(fetchone_values=[(True,), (0,)])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(cursor))
    monkeypatch.setitem(
        sys.modules,
        "airflow.api.common.trigger_dag",
        types.SimpleNamespace(trigger_dag=lambda **kwargs: triggered.append(kwargs)),
    )

    result = pipeline_repair.trigger_repair_dag_if_idle(
        "electricity_region_data",
        ignore_run_id="repair_chain__existing",
    )

    assert result is True
    assert cursor.executed[1][1] == ("electricity_region_data_bronze_hourly_repair", "repair_chain__existing")
