from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


def _normalize_optional_value(value: str | None) -> str:
    normalized = str(value or "").strip()
    if normalized.lower() == "none":
        return ""
    return normalized


def _fmt_hour(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H")


def _fmt_month(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def month_anchor_date(value: str) -> str:
    parsed = _parse_date(value)
    return parsed.replace(day=1).isoformat()


def shift_partition(value: str, frequency: str, step: int) -> str:
    parsed = datetime.strptime(value, "%Y-%m-%d")
    if frequency == "monthly":
        return _shift_months(_month_floor(parsed), step).date().isoformat()
    return (parsed + timedelta(days=step)).date().isoformat()


def enumerate_partitions(start_date: str, end_date: str, *, frequency: str) -> list[str]:
    start = month_anchor_date(start_date) if frequency == "monthly" else start_date
    end = month_anchor_date(end_date) if frequency == "monthly" else end_date
    partitions: list[str] = []
    current = start
    while current <= end:
        partitions.append(current)
        current = shift_partition(current, frequency, 1)
    return partitions


def current_partition_date(frequency: str) -> str:
    now = datetime.now(timezone.utc)
    if frequency == "monthly":
        return _month_floor(now).date().isoformat()
    return now.date().isoformat()


def _month_floor(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _shift_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return dt.replace(year=year, month=month, day=1)


def _hourly_chunks(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    windows: list[tuple[str, str]] = []
    chunk_size = max(int(chunk_days), 1)
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_size - 1), end)
        windows.append((f"{current.isoformat()}T00", f"{chunk_end.isoformat()}T23"))
        current = chunk_end + timedelta(days=1)
    return windows


def _monthly_chunks(start_date: str, end_date: str, chunk_months: int) -> list[tuple[str, str]]:
    start = _month_floor(datetime.strptime(start_date, "%Y-%m-%d"))
    end = _month_floor(datetime.strptime(end_date, "%Y-%m-%d"))
    windows: list[tuple[str, str]] = []
    chunk_size = max(int(chunk_months), 1)
    current = start
    while current <= end:
        next_chunk = _shift_months(current, chunk_size)
        chunk_end = min(_shift_months(next_chunk, -1), end)
        windows.append((_fmt_month(current), _fmt_month(chunk_end)))
        current = next_chunk
    return windows


def resolve_ingest_windows(
    dataset: dict,
    *,
    start_date: str = "",
    end_date: str = "",
    rolling_hours: str = "",
    default_rolling_hours: float = 2.0,
) -> list[tuple[str, str]]:
    start_date = _normalize_optional_value(start_date)
    end_date = _normalize_optional_value(end_date)
    rolling_hours = _normalize_optional_value(rolling_hours)
    frequency = dataset.get("frequency", "hourly")
    if start_date and end_date:
        if frequency == "monthly":
            return _monthly_chunks(start_date, end_date, int(dataset.get("chunk_months", 12)))
        return _hourly_chunks(start_date, end_date, int(dataset.get("chunk_days", 7)))

    now = datetime.now(timezone.utc)
    if frequency == "monthly":
        rolling_months = int(dataset.get("rolling_months", 12))
        end = _month_floor(now)
        start = _shift_months(end, -(rolling_months - 1))
        return [(_fmt_month(start), _fmt_month(end))]

    if rolling_hours:
        hours = float(rolling_hours)
    elif dataset.get("rolling_days") is not None:
        hours = float(dataset["rolling_days"]) * 24
    else:
        hours = default_rolling_hours
    start = now - timedelta(hours=hours)
    return [(_fmt_hour(start), _fmt_hour(now))]


def resolve_processing_date(conf: dict | None, default_ds: str) -> str:
    conf = conf or {}
    return _normalize_optional_value(conf.get("date") or default_ds)


def resolve_business_date(conf: dict | None, default_ds: str, *, frequency: str) -> str:
    target_date = resolve_processing_date(conf, default_ds)
    if frequency == "monthly":
        return month_anchor_date(target_date)
    return target_date
