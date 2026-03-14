from datetime import UTC, datetime
from decimal import Decimal

from rtds.mapping.anchor_assignment import ChainlinkTick
from rtds.quality.gap_detection import GapDetectionPolicy, assess_chainlink_quality


def _tick(second: int, *, minute: int = 0, price: str = "60000") -> ChainlinkTick:
    event_ts = datetime(2026, 3, 13, 12, minute, second, tzinfo=UTC)
    return ChainlinkTick(
        event_id=f"tick-{minute:02d}-{second:02d}",
        event_ts=event_ts,
        recv_ts=event_ts,
        price=Decimal(price),
    )


def test_assess_chainlink_quality_healthy_recent_stream() -> None:
    result = assess_chainlink_quality(
        [_tick(0), _tick(1), _tick(2)],
        as_of_ts=datetime(2026, 3, 13, 12, 0, 2, 500000, tzinfo=UTC),
        policy=GapDetectionPolicy(
            stale_after_ms=3_000,
            missing_after_ms=10_000,
            silence_after_ms=3_000,
            max_inter_tick_gap_ms=3_000,
            lookback_ms=30_000,
        ),
    )

    assert result.current_age_ms == 500
    assert result.last_inter_tick_gap_ms == 1_000
    assert result.max_observed_gap_ms == 1_000
    assert result.stale_flag is False
    assert result.silence_flag is False
    assert result.gap_flag is False
    assert result.usable_flag is True
    assert result.diagnostics == ()


def test_assess_chainlink_quality_flags_silence_and_gap_on_stale_tail() -> None:
    result = assess_chainlink_quality(
        [_tick(0), _tick(1), _tick(2)],
        as_of_ts=datetime(2026, 3, 13, 12, 0, 8, tzinfo=UTC),
        policy=GapDetectionPolicy(
            stale_after_ms=3_000,
            missing_after_ms=10_000,
            silence_after_ms=3_000,
            max_inter_tick_gap_ms=3_000,
            lookback_ms=30_000,
        ),
    )

    assert result.current_age_ms == 6_000
    assert result.stale_flag is True
    assert result.missing_flag is False
    assert result.silence_flag is True
    assert result.gap_flag is True
    assert result.max_observed_gap_ms == 6_000
    assert result.usable_flag is False
    assert result.diagnostics == ("chainlink_gap_detected", "chainlink_silence", "chainlink_stale")


def test_assess_chainlink_quality_flags_historical_gap_without_current_silence() -> None:
    result = assess_chainlink_quality(
        [_tick(0), _tick(5), _tick(6)],
        as_of_ts=datetime(2026, 3, 13, 12, 0, 6, 500000, tzinfo=UTC),
        policy=GapDetectionPolicy(
            stale_after_ms=3_000,
            missing_after_ms=10_000,
            silence_after_ms=3_000,
            max_inter_tick_gap_ms=3_000,
            lookback_ms=30_000,
        ),
    )

    assert result.current_age_ms == 500
    assert result.last_inter_tick_gap_ms == 1_000
    assert result.max_observed_gap_ms == 5_000
    assert result.stale_flag is False
    assert result.silence_flag is False
    assert result.gap_flag is True
    assert result.usable_flag is False
    assert result.diagnostics == ("chainlink_gap_detected",)
