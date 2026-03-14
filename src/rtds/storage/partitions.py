"""Partition helpers for canonical persisted datasets."""

from __future__ import annotations

from datetime import date, datetime

from rtds.core.time import ensure_utc, parse_utc


def normalize_date_utc(value: date | datetime | str) -> str:
    """Normalize supported date-like values to `YYYY-MM-DD`."""

    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise ValueError("date token must not be empty")
        if "T" in candidate:
            return parse_utc(candidate).date().isoformat()
        return date.fromisoformat(candidate).isoformat()
    if isinstance(value, datetime):
        return ensure_utc(value, field_name="date_utc").date().isoformat()
    return value.isoformat()


def partition_path_component(key: str, value: date | datetime | str) -> str:
    """Build a stable hive-style partition path component."""

    normalized_key = key.strip()
    if not normalized_key:
        raise ValueError("partition key must not be empty")
    return f"{normalized_key}={normalize_date_utc(value)}"


__all__ = [
    "normalize_date_utc",
    "partition_path_component",
]
