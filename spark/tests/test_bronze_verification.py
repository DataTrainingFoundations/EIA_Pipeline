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
        row["hour_start_utc"].replace(tzinfo=timezone.utc): (row["observed_row_count"], row["status"], row["expected_row_count"])
        for row in coverage_df.collect()
    }

    assert rows[datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)] == (2, "verified", 2)
    assert rows[datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)] == (1, "partial", 2)
    assert rows[datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)] == (2, "verified", 2)
    assert rows[datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc)] == (0, "missing", 2)
