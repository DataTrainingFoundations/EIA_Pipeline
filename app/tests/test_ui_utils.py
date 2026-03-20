from __future__ import annotations

import pandas as pd
import ui_utils


def test_date_and_timezone_helpers_handle_invalid_values() -> None:
    start_date, end_date = ui_utils.build_default_date_range(
        pd.Timestamp("2026-03-01T00:00:00Z"),
        pd.Timestamp("2026-03-10T00:00:00Z"),
        lookback_days=20,
    )

    assert start_date.isoformat() == "2026-03-01"
    assert end_date.isoformat() == "2026-03-10"
    assert ui_utils.get_timezone_options()[0] == ui_utils.DEFAULT_APP_TIMEZONE
    assert ui_utils.resolve_timezone_name("America/Toronto") == "America/Toronto"
    assert ui_utils.resolve_timezone_name("Not/A_Zone") == "UTC"
    assert ui_utils.format_timestamp(None, "UTC") == "n/a"
    assert ui_utils.format_timestamp(pd.NaT, "UTC") == "n/a"


def test_timestamp_conversion_numeric_helpers_and_priority_sorting() -> None:
    values = pd.Series(["2026-03-10T12:00:00Z", "bad-value"])
    converted = ui_utils.convert_timestamp_series(values, "America/Toronto")
    formatted = ui_utils.format_timestamp(
        pd.Timestamp("2026-03-10T12:00:00Z"), "America/Toronto"
    )

    df = pd.DataFrame(
        {
            "respondent": ["B", "A", "C"],
            "priority": ["medium", "high", "low"],
            "metric": ["1.2", "bad", "3"],
            "timestamp": [
                pd.Timestamp("2026-03-10T10:00:00Z"),
                None,
                pd.Timestamp("2026-03-10T09:00:00Z"),
            ],
        }
    )
    cleaned_df = ui_utils.coerce_numeric(df, ["metric", "missing"])
    ranked_df = ui_utils.rank_priority_labels(df, "priority", ["high", "medium", "low"])

    assert str(converted.dt.tz) == "America/Toronto"
    assert pd.isna(converted.iloc[1])
    assert formatted.endswith("America/Toronto")
    assert cleaned_df["metric"].iloc[0] == 1.2
    assert pd.isna(cleaned_df["metric"].iloc[1])
    assert cleaned_df["metric"].iloc[2] == 3.0
    assert ui_utils.latest_non_null_value(
        df, "timestamp", ["timestamp"]
    ) == pd.Timestamp("2026-03-10T10:00:00Z")
    assert (
        ui_utils.latest_non_null_value(
            pd.DataFrame(columns=["timestamp"]), "timestamp", ["timestamp"]
        )
        is None
    )
    assert ui_utils.safe_quantile(pd.Series(["1", "2", "bad"]), 0.5) == 1.5
    assert ui_utils.safe_quantile(pd.Series(["bad"]), 0.5) is None
    assert ranked_df["respondent"].tolist() == ["A", "B", "C"]
