from __future__ import annotations

import pipeline_validation
import pytest


class FakeCursor:
    def __init__(self, values: list[tuple]) -> None:
        self.values = list(values)
        self.executed: list[tuple[object, object]] = []

    def execute(self, query, params=None) -> None:  # noqa: ANN001
        self.executed.append((query, params))

    def fetchone(self):
        return self.values.pop(0)

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


def test_stage_table_has_rows_returns_false_when_table_is_missing(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(False,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    assert (
        pipeline_validation.stage_table_has_rows(
            "platinum.grid_operations_hourly_stage"
        )
        is False
    )


def test_stage_table_has_rows_returns_false_when_table_is_empty(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(True,), (False,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    assert (
        pipeline_validation.stage_table_has_rows(
            "platinum.grid_operations_hourly_stage"
        )
        is False
    )


def test_stage_table_has_rows_returns_true_when_table_has_rows(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(True,), (True,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    assert (
        pipeline_validation.stage_table_has_rows(
            "platinum.grid_operations_hourly_stage"
        )
        is True
    )


def test_merge_stage_into_target_skips_when_stage_missing_and_allowed(
    monkeypatch,
) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        pipeline_validation, "stage_table_has_rows", lambda _table_name: False
    )
    executed: list[str] = []
    monkeypatch.setattr(
        pipeline_validation.logger,
        "info",
        lambda message, *_args: executed.append(message),
    )

    pipeline_validation.merge_stage_into_target(
        "platinum.grid_operations_hourly",
        "platinum.grid_operations_hourly_stage",
        ["period", "respondent"],
        ["period", "respondent"],
        allow_missing_stage=True,
    )

    assert executed == ["Skipping merge because the stage table is missing or empty %s"]


def test_merge_stage_into_target_executes_merge_without_dropping_stage(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(pipeline_validation, "db_connect", lambda: conn)

    pipeline_validation.merge_stage_into_target(
        "platinum.grid_operations_hourly",
        "platinum.grid_operations_hourly_stage",
        ["period", "respondent", "updated_at"],
        ["period", "respondent"],
        drop_stage=False,
    )

    assert conn.committed is True
    assert len(cursor.executed) == 1
    assert "insert into" in str(cursor.executed[0][0]).lower()


def test_validate_table_has_rows_allows_missing_table(
    monkeypatch,
) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        pipeline_validation, "stage_table_has_rows", lambda _table_name: False
    )

    pipeline_validation.validate_table_has_rows(
        "platinum.grid_operations_hourly_stage",
        allow_missing_table=True,
    )


def test_validate_table_has_rows_allows_empty_result(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(0,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    pipeline_validation.validate_table_has_rows(
        "platinum.grid_operations_hourly_stage",
        allow_empty_result=True,
    )


def test_validate_table_has_rows_raises_when_below_minimum(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(0,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    with pytest.raises(ValueError, match="expected at least 1 row"):
        pipeline_validation.validate_table_has_rows(
            "platinum.grid_operations_hourly_stage"
        )


def test_validate_distinct_values_allows_empty_result(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(0,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    pipeline_validation.validate_distinct_values(
        "platinum.grid_operations_hourly_stage",
        "respondent",
        allow_empty_result=True,
    )


def test_validate_distinct_values_raises_when_below_minimum(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(0,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    with pytest.raises(ValueError, match="distinct value"):
        pipeline_validation.validate_distinct_values(
            "platinum.grid_operations_hourly_stage",
            "respondent",
        )


def test_validate_numeric_bounds_allows_empty_result(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(0,), (0,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    pipeline_validation.validate_numeric_bounds(
        "platinum.grid_operations_hourly",
        "coverage_ratio",
        min_value=0.0,
        allow_empty_result=True,
    )


def test_validate_numeric_bounds_supports_where_clause_and_params(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(2,), (0,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    pipeline_validation.validate_numeric_bounds(
        "platinum.grid_operations_hourly",
        "coverage_ratio",
        min_value=0.0,
        max_value=1.0,
        where_clause="respondent = %s",
        params=["PJM"],
    )

    assert cursor.executed[0][1] == ["PJM"]
    assert cursor.executed[1][1] == [0.0, 1.0, "PJM"]


def test_validate_numeric_bounds_raises_when_invalid_rows_exist(
    monkeypatch,
) -> None:  # noqa: ANN001
    cursor = FakeCursor([(3,), (1,)])
    monkeypatch.setattr(
        pipeline_validation, "db_connect", lambda: FakeConnection(cursor)
    )

    with pytest.raises(ValueError, match="outside bounds"):
        pipeline_validation.validate_numeric_bounds(
            "platinum.grid_operations_hourly", "coverage_ratio", min_value=0.0
        )
