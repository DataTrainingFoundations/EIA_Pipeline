from __future__ import annotations

import data_access
import data_access_grid
import data_access_planning
import data_access_shared
import data_access_summary
import pandas as pd
import psycopg2


class FakeCursor:
    def __init__(self, fetchone_value) -> None:  # noqa: ANN001
        self.fetchone_value = fetchone_value
        self.executed: list[str] = []

    def execute(self, query, params=None) -> None:  # noqa: ANN001
        self.executed.append(str(query))

    def fetchone(self):  # noqa: ANN201
        return self.fetchone_value

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        return None


def test_connection_kwargs_use_environment_overrides(
    monkeypatch,
) -> None:  # noqa: ANN001
    monkeypatch.setenv("POSTGRES_HOST", "warehouse")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("POSTGRES_DB", "analytics")
    monkeypatch.setenv("POSTGRES_USER", "reader")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    kwargs = data_access_shared._connection_kwargs()

    assert kwargs == {
        "host": "warehouse",
        "port": 6543,
        "dbname": "analytics",
        "user": "reader",
        "password": "secret",
    }


def test_get_connection_and_safe_read_sql_delegate_to_database(
    monkeypatch,
) -> None:  # noqa: ANN001
    captured: dict[str, object] = {}
    fake_connection = FakeConnection(FakeCursor((True,)))

    def fake_connect(**kwargs):  # noqa: ANN001
        captured["kwargs"] = kwargs
        return fake_connection

    def fake_read_sql(query, conn, params=None):  # noqa: ANN001
        captured["query"] = query
        captured["conn"] = conn
        captured["params"] = params
        return pd.DataFrame([{"value": 1}])

    monkeypatch.setattr(data_access_shared.psycopg2, "connect", fake_connect)
    monkeypatch.setattr(data_access_shared.pd, "read_sql_query", fake_read_sql)

    conn = data_access_shared.get_connection()
    df = data_access_shared._safe_read_sql("select * from test where id = %s", [7])

    assert conn is fake_connection
    assert captured["kwargs"]["host"] == "localhost"
    assert captured["query"] == "select * from test where id = %s"
    assert captured["conn"] is fake_connection
    assert captured["params"] == [7]
    assert df.iloc[0]["value"] == 1


def test_table_has_rows_handles_success_and_database_error(
    monkeypatch,
) -> None:  # noqa: ANN001
    data_access_summary.table_has_rows.clear()
    cursor = FakeCursor((True,))
    monkeypatch.setattr(
        data_access_summary, "get_connection", lambda: FakeConnection(cursor)
    )

    assert data_access_summary.table_has_rows("platinum.region_demand_daily") is True
    assert "select exists" in cursor.executed[0].lower()

    data_access_summary.table_has_rows.clear()
    monkeypatch.setattr(
        data_access_summary,
        "get_connection",
        lambda: (_ for _ in ()).throw(psycopg2.Error()),
    )
    assert data_access_summary.table_has_rows("platinum.region_demand_daily") is False


def test_summary_queries_and_respondent_listing_use_safe_read_sql(
    monkeypatch,
) -> None:  # noqa: ANN001
    calls: list[dict[str, object]] = []
    for func in (
        data_access_summary.get_summary_coverage,
        data_access_summary.get_grid_operations_coverage,
        data_access_summary.get_planning_coverage,
        data_access_summary.list_respondents,
        data_access_summary.get_backfill_status,
    ):
        func.clear()

    def fake_read_sql(query, params=None):  # noqa: ANN001
        calls.append({"query": query, "params": params})
        if "select distinct respondent" in query:
            return pd.DataFrame({"respondent": ["MISO", None, "PJM"]})
        if "ops.backfill_job_summary" in query:
            return pd.DataFrame(
                [{"dataset_id": "electricity_region_data", "status": "pending"}]
            )
        return pd.DataFrame(
            [
                {
                    "min_date": "2026-03-01",
                    "max_date": "2026-03-10",
                    "min_period": "2026-03-01T00:00:00+00:00",
                    "max_period": "2026-03-10T23:00:00+00:00",
                    "row_count": 24,
                    "respondent_count": 2,
                }
            ]
        )

    monkeypatch.setattr(data_access_summary, "_safe_read_sql", fake_read_sql)

    assert data_access_summary.get_summary_coverage()["row_count"] == 24
    assert data_access_summary.get_grid_operations_coverage()["respondent_count"] == 2
    assert data_access_summary.get_planning_coverage()["row_count"] == 24
    assert data_access_summary.list_respondents() == ["MISO", "PJM"]
    assert len(data_access_summary.get_backfill_status()) == 1
    assert any("ops.backfill_job_summary" in call["query"] for call in calls)


def test_grid_and_planning_queries_apply_all_filters(
    monkeypatch,
) -> None:  # noqa: ANN001
    calls: list[dict[str, object]] = []
    for func in (
        data_access_grid.load_grid_operations_alerts,
        data_access_grid.load_latest_grid_operations_snapshot,
        data_access_grid.load_latest_grid_alerts,
        data_access_grid.load_grid_watchlist,
        data_access_planning.load_resource_planning_daily,
        data_access_planning.load_planning_watchlist,
    ):
        func.clear()

    def fake_read_sql(query, params=None):  # noqa: ANN001
        calls.append({"query": query, "params": params})
        return pd.DataFrame()

    monkeypatch.setattr(data_access_grid, "_safe_read_sql", fake_read_sql)
    monkeypatch.setattr(data_access_planning, "_safe_read_sql", fake_read_sql)

    data_access_grid.load_grid_operations_alerts(
        "2026-03-10T00:00:00+00:00", "2026-03-10T12:00:00+00:00", ["PJM"]
    )
    data_access_grid.load_latest_grid_operations_snapshot(
        "2026-03-10T00:00:00+00:00", "2026-03-10T12:00:00+00:00", ["PJM"]
    )
    data_access_grid.load_latest_grid_alerts(
        "2026-03-10T00:00:00+00:00", "2026-03-10T12:00:00+00:00", ["PJM"]
    )
    data_access_grid.load_grid_watchlist(
        "2026-03-10T00:00:00+00:00", "2026-03-10T12:00:00+00:00", ["PJM"]
    )
    data_access_planning.load_resource_planning_daily(
        "2026-03-01", "2026-03-10", ["PJM"]
    )
    data_access_planning.load_planning_watchlist("2026-03-01", "2026-03-10", ["PJM"])

    assert "period desc" in calls[0]["query"]
    assert "latest_period" in calls[1]["query"]
    assert "latest_period" in calls[2]["query"]
    assert "alert_rollup" in calls[3]["query"]
    assert data_access_shared.GRID_OPERATIONS_ALERT_TABLE in calls[3]["query"]
    assert calls[3]["params"] == [
        "2026-03-10T00:00:00+00:00",
        "2026-03-10T12:00:00+00:00",
        ["PJM"],
        "2026-03-10T00:00:00+00:00",
        "2026-03-10T12:00:00+00:00",
        ["PJM"],
    ]
    assert "order by date, respondent" in calls[4]["query"]
    assert calls[5]["params"] == ["2026-03-01", "2026-03-10", ["PJM"]]


def test_data_access_wrapper_reexports_split_modules() -> None:
    assert data_access.load_grid_watchlist is data_access_grid.load_grid_watchlist
    assert (
        data_access.load_latest_planning_snapshot
        is data_access_planning.load_latest_planning_snapshot
    )
    assert data_access.get_connection is data_access_shared.get_connection
    assert data_access.get_summary_coverage is data_access_summary.get_summary_coverage
