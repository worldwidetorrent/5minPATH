"""Freshness quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from rtds.core.time import age_ms, ensure_utc
from rtds.schemas.quality import SourceFreshnessState

DEFAULT_STALE_AFTER_MS = 2_000
DEFAULT_MISSING_AFTER_MS = 10_000


@dataclass(slots=True, frozen=True)
class FreshnessPolicy:
    """Explicit thresholds for per-source liveness."""

    stale_after_ms: int = DEFAULT_STALE_AFTER_MS
    missing_after_ms: int = DEFAULT_MISSING_AFTER_MS

    def __post_init__(self) -> None:
        if self.stale_after_ms < 0:
            raise ValueError("stale_after_ms must be non-negative")
        if self.missing_after_ms < self.stale_after_ms:
            raise ValueError("missing_after_ms must be >= stale_after_ms")


DEFAULT_FRESHNESS_POLICY = FreshnessPolicy()


def assess_source_freshness(
    source_id: str,
    *,
    as_of_ts: datetime,
    last_event_ts: datetime | None,
    policy: FreshnessPolicy = DEFAULT_FRESHNESS_POLICY,
) -> SourceFreshnessState:
    """Return age, stale, and missing flags for one source."""

    normalized_as_of_ts = ensure_utc(as_of_ts, field_name="as_of_ts")
    diagnostics: list[str] = []

    if last_event_ts is None:
        diagnostics.append("missing_source")
        return SourceFreshnessState(
            source_id=source_id,
            as_of_ts=normalized_as_of_ts,
            last_event_ts=None,
            last_event_age_ms=None,
            stale_flag=True,
            missing_flag=True,
            usable_flag=False,
            diagnostics=tuple(diagnostics),
        )

    normalized_last_event_ts = ensure_utc(last_event_ts, field_name="last_event_ts")
    event_age_ms = int(age_ms(normalized_as_of_ts, normalized_last_event_ts))
    stale_flag = event_age_ms > policy.stale_after_ms
    missing_flag = event_age_ms > policy.missing_after_ms

    if missing_flag:
        diagnostics.append("missing_source")
    elif stale_flag:
        diagnostics.append("stale_source")

    return SourceFreshnessState(
        source_id=source_id,
        as_of_ts=normalized_as_of_ts,
        last_event_ts=normalized_last_event_ts,
        last_event_age_ms=event_age_ms,
        stale_flag=stale_flag,
        missing_flag=missing_flag,
        usable_flag=not stale_flag and not missing_flag,
        diagnostics=tuple(diagnostics),
    )


__all__ = [
    "DEFAULT_FRESHNESS_POLICY",
    "DEFAULT_MISSING_AFTER_MS",
    "DEFAULT_STALE_AFTER_MS",
    "FreshnessPolicy",
    "assess_source_freshness",
]
