from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H")


def resolve_ingest_window(
    *,
    start_date: str = "",
    end_date: str = "",
    rolling_hours: str = "",
    default_rolling_hours: float = 2.0,
    rolling_days: float | None = None,
) -> tuple[str, str]:
    if start_date and end_date:
        return f"{start_date}T00", f"{end_date}T23"

    if rolling_hours:
        hours = float(rolling_hours)
    elif rolling_days is not None:
        hours = float(rolling_days) * 24
    else:
        hours = default_rolling_hours

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    return _fmt(start), _fmt(now)


def resolve_processing_date(conf: dict | None, default_ds: str) -> str:
    conf = conf or {}
    return (conf.get("date") or default_ds).strip()
