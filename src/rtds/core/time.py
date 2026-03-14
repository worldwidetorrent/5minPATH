"""UTC-only time helpers and 5-minute window utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from rtds.core.types import Milliseconds, Seconds, UTCDateTime

WINDOW_DURATION = timedelta(minutes=5)


def utc_now() -> UTCDateTime:
    """Return the current timezone-aware UTC timestamp."""

    return datetime.now(UTC)


def ensure_utc(value: datetime, *, field_name: str = "timestamp") -> UTCDateTime:
    """Normalize an aware datetime to UTC and reject naive datetimes."""

    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return value.astimezone(UTC)


def parse_utc(value: str) -> UTCDateTime:
    """Parse canonical UTC datetime strings in ISO or compact form."""

    candidate = value.strip()
    if not candidate:
        raise ValueError("timestamp string must not be empty")

    if candidate.endswith("Z"):
        try:
            return ensure_utc(
                datetime.fromisoformat(candidate.replace("Z", "+00:00")),
                field_name="timestamp",
            )
        except ValueError:
            pass

        for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S%fZ"):
            try:
                return datetime.strptime(candidate, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue

    return ensure_utc(datetime.fromisoformat(candidate), field_name="timestamp")


def format_utc(value: datetime, *, timespec: str = "seconds") -> str:
    """Format a UTC datetime with a trailing Z."""

    return ensure_utc(value).isoformat(timespec=timespec).replace("+00:00", "Z")


def format_utc_compact(value: datetime, *, include_millis: bool = False) -> str:
    """Format a UTC datetime in filename-safe compact form."""

    normalized = ensure_utc(value)
    base = normalized.strftime("%Y%m%dT%H%M%S")
    if include_millis:
        millis = normalized.microsecond // 1000
        return f"{base}{millis:03d}Z"
    return f"{base}Z"


def is_5m_boundary(value: datetime) -> bool:
    """Return True when the timestamp sits on an exact 5-minute boundary."""

    normalized = ensure_utc(value)
    return (
        normalized.minute % 5 == 0
        and normalized.second == 0
        and normalized.microsecond == 0
    )


def floor_to_5m(value: datetime) -> UTCDateTime:
    """Floor a timestamp to the canonical 5-minute UTC boundary."""

    normalized = ensure_utc(value)
    floored_minute = normalized.minute - (normalized.minute % 5)
    return normalized.replace(minute=floored_minute, second=0, microsecond=0)


def window_end(window_start_ts: datetime) -> UTCDateTime:
    """Compute the canonical end timestamp for a 5-minute window."""

    normalized = ensure_utc(window_start_ts)
    if not is_5m_boundary(normalized):
        raise ValueError("window_start_ts must be aligned to a 5-minute UTC boundary")
    return normalized + WINDOW_DURATION


def seconds_remaining(window_end_ts: datetime, as_of_ts: datetime) -> Seconds:
    """Return remaining whole seconds in the window, clamped at zero."""

    remaining = ensure_utc(window_end_ts) - ensure_utc(as_of_ts)
    if remaining.total_seconds() <= 0:
        return Seconds(0)
    return Seconds(int(remaining.total_seconds()))


def age_ms(reference_ts: datetime, event_ts: datetime) -> Milliseconds:
    """Return the non-negative age in milliseconds between two UTC timestamps."""

    delta = ensure_utc(reference_ts) - ensure_utc(event_ts)
    if delta.total_seconds() < 0:
        raise ValueError("reference_ts must not be earlier than event_ts")
    return Milliseconds(int(delta.total_seconds() * 1000))


__all__ = [
    "WINDOW_DURATION",
    "age_ms",
    "ensure_utc",
    "floor_to_5m",
    "format_utc",
    "format_utc_compact",
    "is_5m_boundary",
    "parse_utc",
    "seconds_remaining",
    "utc_now",
    "window_end",
]
