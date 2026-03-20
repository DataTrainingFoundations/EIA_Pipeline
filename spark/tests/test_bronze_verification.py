from __future__ import annotations

from datetime import datetime, timezone

from jobs.bronze_hourly_coverage_verify import build_hourly_coverage_df


def test_build_hourly_coverage_df_marks_missing_and_partial(spark_session) -> None:
    bronze_df = spark_session.createDataFrame(
        [
            {"event_ts": datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)},
            {"event_ts": datetime(2026, 1, 1, 0, 25, tzinfo=timezone.utc)},
            {"event_ts": datetime(2026, 1, 1, 1, 15, tzinfo=timezone.utc)},
            {"event_ts": datetime(2026, 1, 1, 2, 5, tzinfo=timezone.utc)},
            {"event_ts": datetime(2026, 1, 1, 2, 35, tzinfo=timezone.utc)},
        ]
    )

    coverage_df = build_hourly_coverage_df(
        bronze_df,
        "electricity_region_data",
        datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc),
    )
    rows = {
        row["hour_label"]: (
            row["observed_row_count"],
            row["status"],
            row["expected_row_count"],
        )
        for row in coverage_df.selectExpr(
            "date_format(hour_start_utc, 'yyyy-MM-dd HH') as hour_label",
            "observed_row_count",
            "status",
            "expected_row_count",
        ).collect()
    }

    assert rows["2026-01-01 00"] == (2, "verified", 2)
    assert rows["2026-01-01 01"] == (1, "partial", 2)
    assert rows["2026-01-01 02"] == (2, "verified", 2)
    assert rows["2026-01-01 03"] == (0, "missing", 2)
