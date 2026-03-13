from datetime import datetime, timezone

import pipeline_backfill


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: ANN001
        return cls(2026, 3, 11, 12, 0, 0, tzinfo=tz or timezone.utc)


class FakeCursor:
    def __init__(self, values=None) -> None:
        self.inserted_params: list[tuple] = []
        self.rowcount = 0
        self._fetchall_result = []
        self.values = list(values or [])

    def execute(self, query, params=None):  # noqa: ANN001
        query_text = str(query).lower()
        if "select chunk_start_utc, chunk_end_utc, status" in query_text:
            self._fetchall_result = []
            self.rowcount = 0
        elif "insert into ops.backfill_jobs" in query_text:
            self.inserted_params.append(tuple(params))
            self.rowcount = 1

    def fetchone(self):
        return self.values.pop(0)

    def fetchall(self):
        return self._fetchall_result

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_enqueue_backfill_jobs_inserts_newest_windows_first(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor()
    monkeypatch.setattr(pipeline_backfill, "db_connect", lambda: FakeConnection(cursor))
    monkeypatch.setattr(pipeline_backfill, "get_dataset", lambda _dataset_id: {"backfill": {"start": "2026-03-08T00:00:00+00:00", "step": "day", "max_pending_chunks": 2}})
    monkeypatch.setattr(pipeline_backfill, "recover_stale_backfill_jobs", lambda _dataset_id: 0)
    monkeypatch.setattr(pipeline_backfill, "datetime", FixedDatetime)

    inserted = pipeline_backfill.enqueue_backfill_jobs("electricity_region_data")

    assert inserted == 2
    assert cursor.inserted_params[0][1].isoformat() == "2026-03-09T00:00:00+00:00"
    assert cursor.inserted_params[0][2].isoformat() == "2026-03-11T12:00:00+00:00"
    assert cursor.inserted_params[1][1].isoformat() == "2026-03-08T00:00:00+00:00"
    assert cursor.inserted_params[1][2].isoformat() == "2026-03-09T00:00:00+00:00"


def test_has_completed_backfill_returns_true_when_completed_exists(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor(values=[(True,)])
    monkeypatch.setattr(pipeline_backfill, "db_connect", lambda: FakeConnection(cursor))

    assert pipeline_backfill.has_completed_backfill("electricity_region_data") is True


def test_trigger_backfill_dag_if_idle_triggers_when_pending_and_idle(monkeypatch) -> None:  # noqa: ANN001
    import sys
    import types

    triggered: list[dict] = []
    cursor = FakeCursor(values=[(True,), (0,)])
    monkeypatch.setattr(pipeline_backfill, "db_connect", lambda: FakeConnection(cursor))
    monkeypatch.setitem(sys.modules, "airflow.api.common.trigger_dag", types.SimpleNamespace(trigger_dag=lambda **kwargs: triggered.append(kwargs)))

    assert pipeline_backfill.trigger_backfill_dag_if_idle("electricity_region_data") is True
    assert triggered[0]["dag_id"] == "electricity_region_data_backfill"
