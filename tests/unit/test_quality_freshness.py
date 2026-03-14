from datetime import UTC, datetime

from rtds.quality.freshness import FreshnessPolicy, assess_source_freshness


def test_assess_source_freshness_healthy_source() -> None:
    as_of_ts = datetime(2026, 3, 13, 12, 0, 1, tzinfo=UTC)
    last_event_ts = datetime(2026, 3, 13, 12, 0, 0, 250000, tzinfo=UTC)

    result = assess_source_freshness(
        "binance",
        as_of_ts=as_of_ts,
        last_event_ts=last_event_ts,
        policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
    )

    assert result.last_event_age_ms == 750
    assert result.stale_flag is False
    assert result.missing_flag is False
    assert result.usable_flag is True
    assert result.diagnostics == ()


def test_assess_source_freshness_stale_but_not_missing() -> None:
    as_of_ts = datetime(2026, 3, 13, 12, 0, 5, tzinfo=UTC)
    last_event_ts = datetime(2026, 3, 13, 12, 0, 1, tzinfo=UTC)

    result = assess_source_freshness(
        "coinbase",
        as_of_ts=as_of_ts,
        last_event_ts=last_event_ts,
        policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
    )

    assert result.last_event_age_ms == 4_000
    assert result.stale_flag is True
    assert result.missing_flag is False
    assert result.usable_flag is False
    assert result.diagnostics == ("stale_source",)


def test_assess_source_freshness_missing_source() -> None:
    result = assess_source_freshness(
        "kraken",
        as_of_ts=datetime(2026, 3, 13, 12, 0, 5, tzinfo=UTC),
        last_event_ts=None,
        policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
    )

    assert result.last_event_age_ms is None
    assert result.stale_flag is True
    assert result.missing_flag is True
    assert result.usable_flag is False
    assert result.diagnostics == ("missing_source",)
