import sys
import types

import pipeline_repair


class FakeCursor:
    def __init__(self, values):
        self.values = list(values)
        self.rowcount = 0

    def execute(self, query, params=None):  # noqa: ANN001
        return None

    def fetchone(self):
        return self.values.pop(0)

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


def test_trigger_repair_dag_if_idle_triggers_when_pending_and_idle(monkeypatch) -> None:  # noqa: ANN001
    triggered: list[dict] = []
    fake_cursor = FakeCursor([(True,), (0,)])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(fake_cursor))
    monkeypatch.setitem(sys.modules, "airflow.api.common.trigger_dag", types.SimpleNamespace(trigger_dag=lambda **kwargs: triggered.append(kwargs)))

    result = pipeline_repair.trigger_repair_dag_if_idle("electricity_region_data")

    assert result is True
    assert triggered[0]["dag_id"] == "electricity_region_data_bronze_hourly_repair"


def test_trigger_repair_dag_if_idle_handles_existing_airflow_context_fields(monkeypatch) -> None:  # noqa: ANN001
    triggered: list[dict] = []
    fake_cursor = FakeCursor([(True,), (0,)])
    monkeypatch.setattr(pipeline_repair, "db_connect", lambda: FakeConnection(fake_cursor))
    monkeypatch.setattr(
        pipeline_repair,
        "current_airflow_log_fields",
        lambda: {"dag_id": "electricity_region_data_bronze_hourly_repair", "task_id": "trigger_next", "run_id": "repair_chain__existing"},
    )
    monkeypatch.setitem(sys.modules, "airflow.api.common.trigger_dag", types.SimpleNamespace(trigger_dag=lambda **kwargs: triggered.append(kwargs)))

    result = pipeline_repair.trigger_repair_dag_if_idle("electricity_region_data")

    assert result is True
    assert triggered[0]["dag_id"] == "electricity_region_data_bronze_hourly_repair"
