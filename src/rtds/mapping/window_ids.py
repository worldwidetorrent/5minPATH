"""Authoritative utilities for canonical BTC 5-minute window IDs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from rtds.core.enums import AssetCode
from rtds.core.ids import build_window_id, parse_window_id
from rtds.core.time import ensure_utc, floor_to_5m, window_end
from rtds.core.types import UTCDateTime, WindowId

WINDOW_INTERVAL = timedelta(minutes=5)
WINDOWS_PER_DAY = 288


@dataclass(slots=True, frozen=True)
class WindowBounds:
    """Canonical 5-minute window bounds keyed by `window_id`."""

    window_id: WindowId
    window_start_ts: UTCDateTime
    window_end_ts: UTCDateTime


def owning_window_start(ts: datetime) -> UTCDateTime:
    """Floor an arbitrary timestamp into its owning 5-minute window start."""

    return floor_to_5m(ts)


def owning_window_id(ts: datetime, *, asset: AssetCode | str = AssetCode.BTC) -> WindowId:
    """Return the canonical window ID owning the provided UTC timestamp."""

    window_start_ts = owning_window_start(ts)
    return build_window_id(asset, window_start_ts)


def get_window_bounds(window_id: str) -> WindowBounds:
    """Return canonical start and end timestamps for a window ID."""

    _, window_start_ts = parse_window_id(window_id)
    return WindowBounds(
        window_id=WindowId(window_id),
        window_start_ts=window_start_ts,
        window_end_ts=window_end(window_start_ts),
    )


def iter_window_ids(
    start_ts: datetime,
    *,
    periods: int,
    asset: AssetCode | str = AssetCode.BTC,
) -> list[WindowId]:
    """Generate a forward strip of canonical window IDs from a UTC start."""

    if periods <= 0:
        raise ValueError("periods must be positive")

    current_start = owning_window_start(start_ts)
    return [
        build_window_id(asset, current_start + (index * WINDOW_INTERVAL))
        for index in range(periods)
    ]


def generate_window_strip_for_horizon(
    start_ts: datetime,
    *,
    horizon: timedelta,
    asset: AssetCode | str = AssetCode.BTC,
) -> list[WindowId]:
    """Generate all window IDs touched by a forward-looking horizon."""

    if horizon <= timedelta(0):
        raise ValueError("horizon must be positive")

    window_count = int(horizon.total_seconds() // WINDOW_INTERVAL.total_seconds())
    if horizon % WINDOW_INTERVAL:
        window_count += 1

    return iter_window_ids(start_ts, periods=window_count, asset=asset)


def daily_window_schedule(
    day: date,
    *,
    asset: AssetCode | str = AssetCode.BTC,
) -> list[WindowBounds]:
    """Generate the full canonical 288-window daily UTC schedule."""

    day_start = datetime.combine(day, time.min, tzinfo=UTC)
    window_ids = iter_window_ids(day_start, periods=WINDOWS_PER_DAY, asset=asset)
    return [get_window_bounds(window_id) for window_id in window_ids]


def generate_window_strip(
    start: date | datetime,
    *,
    periods: int | None = None,
    horizon: timedelta | None = None,
    asset: AssetCode | str = AssetCode.BTC,
) -> list[WindowBounds]:
    """Generate a canonical window strip for a date or forward horizon."""

    if isinstance(start, date) and not isinstance(start, datetime):
        if periods is not None or horizon is not None:
            raise ValueError("date-based strips do not accept periods or horizon")
        return daily_window_schedule(start, asset=asset)

    if periods is not None and horizon is not None:
        raise ValueError("specify periods or horizon, not both")
    if periods is None and horizon is None:
        raise ValueError("datetime-based strips require periods or horizon")

    start_ts = ensure_utc(start, field_name="start_ts")
    if periods is not None:
        window_ids = iter_window_ids(start_ts, periods=periods, asset=asset)
        return [get_window_bounds(window_id) for window_id in window_ids]

    assert horizon is not None
    return [
        get_window_bounds(window_id)
        for window_id in generate_window_strip_for_horizon(start_ts, horizon=horizon, asset=asset)
    ]


__all__ = [
    "WINDOW_INTERVAL",
    "WINDOWS_PER_DAY",
    "WindowBounds",
    "daily_window_schedule",
    "generate_window_strip",
    "generate_window_strip_for_horizon",
    "get_window_bounds",
    "iter_window_ids",
    "owning_window_id",
    "owning_window_start",
]
