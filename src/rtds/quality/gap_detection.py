"""Gap-detection quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from rtds.core.time import age_ms, ensure_utc
from rtds.mapping.anchor_assignment import ChainlinkTick
from rtds.schemas.quality import ChainlinkQualityState

DEFAULT_CHAINLINK_STALE_AFTER_MS = 3_000
DEFAULT_CHAINLINK_MISSING_AFTER_MS = 10_000
DEFAULT_CHAINLINK_SILENCE_AFTER_MS = 3_000
DEFAULT_MAX_INTER_TICK_GAP_MS = 3_000
DEFAULT_GAP_LOOKBACK_MS = 30_000


@dataclass(slots=True, frozen=True)
class GapDetectionPolicy:
    """Explicit Chainlink liveness and gap thresholds."""

    stale_after_ms: int = DEFAULT_CHAINLINK_STALE_AFTER_MS
    missing_after_ms: int = DEFAULT_CHAINLINK_MISSING_AFTER_MS
    silence_after_ms: int = DEFAULT_CHAINLINK_SILENCE_AFTER_MS
    max_inter_tick_gap_ms: int = DEFAULT_MAX_INTER_TICK_GAP_MS
    lookback_ms: int = DEFAULT_GAP_LOOKBACK_MS

    def __post_init__(self) -> None:
        if self.stale_after_ms < 0:
            raise ValueError("stale_after_ms must be non-negative")
        if self.missing_after_ms < self.stale_after_ms:
            raise ValueError("missing_after_ms must be >= stale_after_ms")
        if self.silence_after_ms < 0:
            raise ValueError("silence_after_ms must be non-negative")
        if self.max_inter_tick_gap_ms <= 0:
            raise ValueError("max_inter_tick_gap_ms must be positive")
        if self.lookback_ms <= 0:
            raise ValueError("lookback_ms must be positive")


DEFAULT_GAP_DETECTION_POLICY = GapDetectionPolicy()


def assess_chainlink_quality(
    ticks: Iterable[ChainlinkTick],
    *,
    as_of_ts: datetime,
    policy: GapDetectionPolicy = DEFAULT_GAP_DETECTION_POLICY,
) -> ChainlinkQualityState:
    """Assess Chainlink age, silence, and gap state up to one snapshot timestamp."""

    normalized_as_of_ts = ensure_utc(as_of_ts, field_name="as_of_ts")
    history = sorted(
        (tick for tick in ticks if tick.event_ts <= normalized_as_of_ts),
        key=lambda tick: tick.event_ts,
    )
    diagnostics: list[str] = []

    if not history:
        diagnostics.extend(("chainlink_gap_detected", "chainlink_missing", "chainlink_silence"))
        return ChainlinkQualityState(
            as_of_ts=normalized_as_of_ts,
            last_event_ts=None,
            current_age_ms=None,
            stale_flag=True,
            missing_flag=True,
            silence_flag=True,
            gap_flag=True,
            last_inter_tick_gap_ms=None,
            max_observed_gap_ms=None,
            usable_flag=False,
            diagnostics=tuple(diagnostics),
        )

    last_tick = history[-1]
    current_age = int(age_ms(normalized_as_of_ts, last_tick.event_ts))
    stale_flag = current_age > policy.stale_after_ms
    missing_flag = current_age > policy.missing_after_ms
    silence_flag = current_age > policy.silence_after_ms

    lookback_start = normalized_as_of_ts - timedelta(milliseconds=policy.lookback_ms)
    recent_ticks = [tick for tick in history if tick.event_ts >= lookback_start]
    inter_tick_gaps = [
        int(age_ms(current.event_ts, previous.event_ts))
        for previous, current in zip(recent_ticks, recent_ticks[1:], strict=False)
    ]
    last_inter_tick_gap_ms = inter_tick_gaps[-1] if inter_tick_gaps else None
    observed_max_gap_ms = max(inter_tick_gaps + [current_age])
    gap_flag = observed_max_gap_ms > policy.max_inter_tick_gap_ms

    if gap_flag:
        diagnostics.append("chainlink_gap_detected")
    if missing_flag:
        diagnostics.append("chainlink_missing")
    elif stale_flag:
        diagnostics.append("chainlink_stale")
    if silence_flag:
        diagnostics.append("chainlink_silence")

    return ChainlinkQualityState(
        as_of_ts=normalized_as_of_ts,
        last_event_ts=last_tick.event_ts,
        current_age_ms=current_age,
        stale_flag=stale_flag,
        missing_flag=missing_flag,
        silence_flag=silence_flag,
        gap_flag=gap_flag,
        last_inter_tick_gap_ms=last_inter_tick_gap_ms,
        max_observed_gap_ms=observed_max_gap_ms,
        usable_flag=not stale_flag and not missing_flag and not silence_flag and not gap_flag,
        diagnostics=tuple(diagnostics),
    )


__all__ = [
    "DEFAULT_CHAINLINK_MISSING_AFTER_MS",
    "DEFAULT_CHAINLINK_SILENCE_AFTER_MS",
    "DEFAULT_CHAINLINK_STALE_AFTER_MS",
    "DEFAULT_GAP_DETECTION_POLICY",
    "DEFAULT_GAP_LOOKBACK_MS",
    "DEFAULT_MAX_INTER_TICK_GAP_MS",
    "GapDetectionPolicy",
    "assess_chainlink_quality",
]
