import data_access_grid
import data_access_planning
import data_access_summary
import pandas as pd
import psycopg2


def test_load_grid_operations_hourly_passes_filters(
    monkeypatch,
) -> None:  # noqa: ANN001
    calls: list[dict] = []
    data_access_grid.load_grid_operations_hourly.clear()

    def fake_read_sql(query, params=None):  # noqa: ANN001
        calls.append({"query": query, "params": params})
        return pd.DataFrame()

    monkeypatch.setattr(data_access_grid, "_safe_read_sql", fake_read_sql)

    data_access_grid.load_grid_operations_hourly(
        "2026-03-10T00:00:00+00:00", "2026-03-10T23:00:00+00:00", ["PJM"]
    )

    assert "period >= %s" in calls[0]["query"]
    assert "respondent = any(%s)" in calls[0]["query"]
    assert calls[0]["params"] == [
        "2026-03-10T00:00:00+00:00",
        "2026-03-10T23:00:00+00:00",
        ["PJM"],
    ]


def test_load_latest_planning_snapshot_passes_filters(
    monkeypatch,
) -> None:  # noqa: ANN001
    calls: list[dict] = []
    data_access_planning.load_latest_planning_snapshot.clear()

    def fake_read_sql(query, params=None):  # noqa: ANN001
        calls.append({"query": query, "params": params})
        return pd.DataFrame()

    monkeypatch.setattr(data_access_planning, "_safe_read_sql", fake_read_sql)

    data_access_planning.load_latest_planning_snapshot(
        "2026-03-01", "2026-03-05", ["PJM", "MISO"]
    )

    assert "latest_date" in calls[0]["query"]
    assert calls[0]["params"] == ["2026-03-01", "2026-03-05", ["PJM", "MISO"]]


def test_get_backfill_status_returns_empty_frame_on_database_error(
    monkeypatch,
) -> None:  # noqa: ANN001
    data_access_summary.get_backfill_status.clear()
    monkeypatch.setattr(
        data_access_summary,
        "_safe_read_sql",
        lambda *args, **kwargs: (_ for _ in ()).throw(psycopg2.Error()),
    )

    result = data_access_summary.get_backfill_status()

    assert list(result.columns) == [
        "dataset_id",
        "status",
        "job_count",
        "min_chunk_start_utc",
        "max_chunk_end_utc",
        "last_updated_at",
    ]
