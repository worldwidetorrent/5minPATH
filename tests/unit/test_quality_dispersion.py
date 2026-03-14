from datetime import UTC, datetime
from decimal import Decimal

from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import build_exchange_spot_instrument_id
from rtds.quality.dispersion import DispersionPolicy, assess_exchange_composite_quality
from rtds.quality.freshness import FreshnessPolicy
from rtds.schemas.normalized import SCHEMA_VERSION, ExchangeQuote


def _exchange_quote(
    venue: VenueCode,
    *,
    event_ts: datetime,
    mid_price: str,
) -> ExchangeQuote:
    mid = Decimal(mid_price)
    spread = Decimal("2")
    return ExchangeQuote(
        venue_id=venue.value,
        instrument_id=str(build_exchange_spot_instrument_id(venue, AssetCode.BTC)),
        asset_id=AssetCode.BTC.value,
        event_ts=event_ts,
        recv_ts=event_ts,
        proc_ts=event_ts,
        best_bid=mid - (spread / Decimal("2")),
        best_ask=mid + (spread / Decimal("2")),
        mid_price=mid,
        bid_size=Decimal("1.25"),
        ask_size=Decimal("1.50"),
        raw_event_id=f"{venue.value}:{event_ts.isoformat()}",
        normalizer_version="0.1.0",
        schema_version=SCHEMA_VERSION,
        created_ts=event_ts,
    )


def test_assess_exchange_composite_quality_flags_outlier_but_keeps_two_trusted_venues() -> None:
    as_of_ts = datetime(2026, 3, 13, 12, 0, 1, tzinfo=UTC)
    quotes = [
        _exchange_quote(
            VenueCode.BINANCE,
            event_ts=datetime(2026, 3, 13, 12, 0, 0, 500000, tzinfo=UTC),
            mid_price="60000",
        ),
        _exchange_quote(
            VenueCode.COINBASE,
            event_ts=datetime(2026, 3, 13, 12, 0, 0, 700000, tzinfo=UTC),
            mid_price="60001",
        ),
        _exchange_quote(
            VenueCode.KRAKEN,
            event_ts=datetime(2026, 3, 13, 12, 0, 0, 900000, tzinfo=UTC),
            mid_price="60150",
        ),
    ]

    result = assess_exchange_composite_quality(
        quotes,
        as_of_ts=as_of_ts,
        freshness_policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
        dispersion_policy=DispersionPolicy(
            min_contributing_venues=2,
            outlier_abs_threshold_usd=Decimal("25"),
            outlier_bps_threshold=Decimal("3"),
        ),
    )

    assert result.contributing_venue_count == 3
    assert result.trusted_venue_count == 2
    assert result.outlier_venue_ids == (VenueCode.KRAKEN.value,)
    assert result.trusted_venue_ids == (
        VenueCode.BINANCE.value,
        VenueCode.COINBASE.value,
    )
    assert result.per_venue_age_ms[VenueCode.BINANCE.value] == 500
    assert result.dispersion_abs_usd == Decimal("150")
    assert result.usable_flag is True
    assert result.diagnostics == ("outlier_detected",)


def test_assess_exchange_composite_quality_rejects_insufficient_fresh_venues() -> None:
    as_of_ts = datetime(2026, 3, 13, 12, 0, 10, tzinfo=UTC)
    quotes = [
        _exchange_quote(
            VenueCode.BINANCE,
            event_ts=datetime(2026, 3, 13, 12, 0, 9, 500000, tzinfo=UTC),
            mid_price="60000",
        ),
        _exchange_quote(
            VenueCode.COINBASE,
            event_ts=datetime(2026, 3, 13, 11, 59, 58, tzinfo=UTC),
            mid_price="60001",
        ),
    ]

    result = assess_exchange_composite_quality(
        quotes,
        as_of_ts=as_of_ts,
        freshness_policy=FreshnessPolicy(stale_after_ms=2_000, missing_after_ms=10_000),
        dispersion_policy=DispersionPolicy(min_contributing_venues=2),
    )

    assert result.contributing_venue_count == 1
    assert result.trusted_venue_count == 1
    assert result.insufficient_venues_flag is True
    assert result.dispersion_abs_usd is None
    assert result.outlier_venue_ids == ()
    assert result.usable_flag is False
    assert result.diagnostics == ("insufficient_contributing_venues",)
