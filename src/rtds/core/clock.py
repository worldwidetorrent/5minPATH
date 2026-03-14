"""Clock abstractions for production and tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from rtds.core.time import ensure_utc, utc_now
from rtds.core.types import UTCDateTime


class Clock(Protocol):
    """Minimal clock interface used throughout the project."""

    def now(self) -> UTCDateTime:
        """Return the current timezone-aware UTC timestamp."""


@dataclass(slots=True)
class RealClock:
    """Production clock backed by the system wall clock."""

    def now(self) -> UTCDateTime:
        return utc_now()


@dataclass(slots=True)
class ManualClock:
    """Mutable clock for deterministic unit and integration tests."""

    current_ts: UTCDateTime

    def __post_init__(self) -> None:
        self.current_ts = ensure_utc(self.current_ts, field_name="current_ts")

    def now(self) -> UTCDateTime:
        return self.current_ts

    def set(self, value: datetime) -> UTCDateTime:
        self.current_ts = ensure_utc(value, field_name="current_ts")
        return self.current_ts

    def advance(
        self,
        *,
        seconds: int = 0,
        milliseconds: int = 0,
        microseconds: int = 0,
    ) -> UTCDateTime:
        self.current_ts = self.current_ts + timedelta(
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds,
        )
        return self.current_ts


__all__ = ["Clock", "ManualClock", "RealClock"]
