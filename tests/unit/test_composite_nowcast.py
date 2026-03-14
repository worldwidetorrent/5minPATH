from datetime import UTC, datetime
from decimal import Decimal

from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import build_exchange_spot_instrument_id
from rtds.features.composite_nowcast import (
    MedianFamilyAggregationPolicy,
    aggregate_mid_prices,
    compute_composite_nowcast,
)
from rtds.quality.freshness import FreshnessPolicy
from rtds.schemas.normalized import SCHEMA_VERSION, ExchangeQuote


def _exchange_quote(
    venue: VenueCode,
    *,
    event_ts: datetime,
    mid_price: str,
    crossed_market_flag: bool = False,
    normalization_status: str = "normalized",
) -> ExchangeQuote:
    mid = Decimal(mid_price)
    half_spread = Decimal("1")
    if crossed_market_flag:
        best_bid = mid + half_spread
        best_ask = mid - half_spread
    else:
        best_bid = mid - half_spread
        best_ask = mid + half_spread

    return ExchangeQuote(
        venue_id=venue.value,
        instrument_id=str(build_exchange_spot_instrument_id(venue, AssetCode.BTC)),
        asset_id=AssetCode.BTC.value,
        event_ts=event_ts,
        recv_ts=event_ts,
        proc_ts=event_ts,
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=mid,
        bid_size=Decimal("1"),
        ask_size=Decimal("1"),
        raw_event_id=f"{venue.value}:{event_ts.isoformat()}",
        normalizer_version="0.1.0",
        schema_version=SCHEMA_VERSION,
        created_ts=event_ts,
        crossed_market_flag=crossed_market_flag,
        normalization_status=normalization_status,
    )


def test_compute_composite_nowcast_returns_median_for_three_fresh_venues() -> None:
    as_of_ts = datetime(2026, 3, 13, 12, 0, 1, tzinfo=UTC)
    result = compute_composite_nowcast(
        [
            _exchange_quote(
                VenueCode.BINANCE,
                event_ts=datetime(2026, 3, 13, 12, 0, 0, 100000, tzinfo=UTC),
                mid_price="60010",
            ),
            _exchange_quote(
                VenueCode.COINBASE,
                event_ts=datetime(2026, 3, 13, 12, 0, 0, 200000, tzinfo=UTC),
                mid_price="60005",
            ),
            _exchange_quote(
                VenueCode.KRAKEN,
                event_ts=datetime(2026, 3, 13, 12, 0, 0, 300000, tzinfo=UTC),
                mid_price="60007",
            ),
        ],
        as_of_ts=as_of_ts,
        freshness_policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
        minimum_venue_count=3,
    )

    assert result.composite_now_price == Decimal("60007")
    assert result.composite_method == "median_3"
    assert result.contributing_venues == ("binance", "coinbase", "kraken")
    assert result.dispersion_abs_usd == Decimal("5")
    assert result.composite_missing_flag is False
    assert result.quality_score > Decimal("0")


def test_aggregate_mid_prices_returns_median_of_four() -> None:
    price, method = aggregate_mid_prices(
        [
            Decimal("60000"),
            Decimal("60004"),
            Decimal("60008"),
            Decimal("60012"),
        ]
    )

    assert price == Decimal("60006")
    assert method == "median_4"


def test_aggregate_mid_prices_trims_one_high_and_one_low_for_five_or_more() -> None:
    price, method = aggregate_mid_prices(
        [
            Decimal("59900"),
            Decimal("60000"),
            Decimal("60002"),
            Decimal("60004"),
            Decimal("60150"),
        ],
        policy=MedianFamilyAggregationPolicy(trim_count_each_side=1),
    )

    assert price == Decimal("60002")
    assert method == "trimmed_median"


def test_compute_composite_nowcast_excludes_stale_venue_and_returns_missing_when_minimum_not_met(
) -> None:
    result = compute_composite_nowcast(
        [
            _exchange_quote(
                VenueCode.BINANCE,
                event_ts=datetime(2026, 3, 13, 12, 0, 9, 500000, tzinfo=UTC),
                mid_price="60000",
            ),
            _exchange_quote(
                VenueCode.COINBASE,
                event_ts=datetime(2026, 3, 13, 12, 0, 9, 700000, tzinfo=UTC),
                mid_price="60002",
            ),
            _exchange_quote(
                VenueCode.KRAKEN,
                event_ts=datetime(2026, 3, 13, 12, 0, 2, tzinfo=UTC),
                mid_price="60001",
            ),
        ],
        as_of_ts=datetime(2026, 3, 13, 12, 0, 10, tzinfo=UTC),
        freshness_policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
        minimum_venue_count=3,
    )

    assert result.composite_now_price is None
    assert result.composite_missing_flag is True
    assert result.contributing_venue_count == 2
    assert result.contributing_venues == ("binance", "coinbase")
    assert "composite_missing" in result.diagnostics


def test_compute_composite_nowcast_excludes_crossed_quote() -> None:
    result = compute_composite_nowcast(
        [
            _exchange_quote(
                VenueCode.BINANCE,
                event_ts=datetime(2026, 3, 13, 12, 0, 0, 100000, tzinfo=UTC),
                mid_price="60000",
            ),
            _exchange_quote(
                VenueCode.COINBASE,
                event_ts=datetime(2026, 3, 13, 12, 0, 0, 200000, tzinfo=UTC),
                mid_price="60001",
                crossed_market_flag=True,
            ),
            _exchange_quote(
                VenueCode.KRAKEN,
                event_ts=datetime(2026, 3, 13, 12, 0, 0, 300000, tzinfo=UTC),
                mid_price="60002",
            ),
        ],
        as_of_ts=datetime(2026, 3, 13, 12, 0, 1, tzinfo=UTC),
        freshness_policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
        minimum_venue_count=3,
    )

    assert result.composite_now_price is None
    assert result.contributing_venues == ("binance", "kraken")
    assert result.composite_missing_flag is True


def test_compute_composite_nowcast_is_deterministic_for_reordered_inputs() -> None:
    older_binance = _exchange_quote(
        VenueCode.BINANCE,
        event_ts=datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC),
        mid_price="59990",
    )
    newer_binance = _exchange_quote(
        VenueCode.BINANCE,
        event_ts=datetime(2026, 3, 13, 12, 0, 0, 900000, tzinfo=UTC),
        mid_price="60000",
    )
    coinbase = _exchange_quote(
        VenueCode.COINBASE,
        event_ts=datetime(2026, 3, 13, 12, 0, 0, 700000, tzinfo=UTC),
        mid_price="60001",
    )
    kraken = _exchange_quote(
        VenueCode.KRAKEN,
        event_ts=datetime(2026, 3, 13, 12, 0, 0, 800000, tzinfo=UTC),
        mid_price="60002",
    )

    first = compute_composite_nowcast(
        [older_binance, newer_binance, coinbase, kraken],
        as_of_ts=datetime(2026, 3, 13, 12, 0, 1, tzinfo=UTC),
        freshness_policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
        minimum_venue_count=3,
    )
    second = compute_composite_nowcast(
        [kraken, coinbase, newer_binance, older_binance],
        as_of_ts=datetime(2026, 3, 13, 12, 0, 1, tzinfo=UTC),
        freshness_policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
        minimum_venue_count=3,
    )

    assert first.composite_now_price == Decimal("60001")
    assert second.composite_now_price == Decimal("60001")
    assert first.contributing_venues == second.contributing_venues
    assert first.per_venue_mids == second.per_venue_mids
    assert first.quality_score == second.quality_score
