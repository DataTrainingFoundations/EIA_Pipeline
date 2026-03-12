import pytest

import pipeline_validation


class FakeCursor:
    def __init__(self, values):
        self.values = list(values)

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

    def close(self) -> None:
        return None


def test_validate_numeric_bounds_raises_when_invalid_rows_exist(monkeypatch) -> None:  # noqa: ANN001
    cursor = FakeCursor([(3,), (1,)])
    monkeypatch.setattr(pipeline_validation, "db_connect", lambda: FakeConnection(cursor))

    with pytest.raises(ValueError, match="outside bounds"):
        pipeline_validation.validate_numeric_bounds("platinum.grid_operations_hourly", "coverage_ratio", min_value=0.0)
