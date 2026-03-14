from datetime import UTC, datetime
from decimal import Decimal

import pytest

from rtds.core.clock import ManualClock
from rtds.core.enums import AssetCode, SnapshotOrigin, StatusValue, VenueCode, WindowType
from rtds.core.units import validate_contract_price, validate_usd_price, validate_volatility
from rtds.schemas.canonical import Asset, Instrument, Market, OracleFeed, SnapshotRef, Venue, Window


def test_asset_and_venue_apply_canonical_defaults() -> None:
    asset = Asset(asset_id=AssetCode.BTC)
    venue = Venue(venue_id=VenueCode.CHAINLINK)

    assert asset.base_symbol == "BTC"
    assert asset.display_name == "Bitcoin"
    assert asset.status is StatusValue.ACTIVE
    assert venue.display_name == "CHAINLINK"


def test_instrument_market_and_oracle_feed_build_ids() -> None:
    instrument = Instrument(
        venue_id=VenueCode.COINBASE,
        asset_id=AssetCode.BTC,
        instrument_type="spot",
        venue_symbol="BTC-USD",
    )
    market = Market(
        venue_id=VenueCode.POLYMARKET,
        asset_id=AssetCode.BTC,
        market_id="0xabc123",
        title="Will BTC finish above its 5m open?",
    )
    oracle_feed = OracleFeed(asset_id=AssetCode.BTC)

    assert instrument.instrument_id == "coinbase:spot:BTC-USD"
    assert market.market_id == "0xabc123"
    assert oracle_feed.oracle_feed_id == "chainlink:stream:BTC-USD"


def test_window_and_snapshot_ref_build_canonical_ids() -> None:
    window = Window(
        asset_id="BTC",
        window_type=WindowType.UPDOWN_5M,
        window_start_ts=datetime(2026, 3, 13, 12, 5, tzinfo=UTC),
    )
    snapshot = SnapshotRef(
        window_id=window.window_id,
        market_id="0xabc123",
        snapshot_ts=datetime(2026, 3, 13, 12, 7, 3, 250000, tzinfo=UTC),
        snapshot_origin=SnapshotOrigin.EVENT_CHAINLINK_TICK,
    )

    assert window.window_end_ts == datetime(2026, 3, 13, 12, 10, tzinfo=UTC)
    assert window.window_id == "btc-5m-20260313T120500Z"
    assert snapshot.snapshot_id == "snap:btc-5m-20260313T120500Z:0xabc123:20260313T120703250Z"
    assert snapshot.snapshot_origin is SnapshotOrigin.EVENT_CHAINLINK_TICK


def test_manual_clock_and_numeric_validators() -> None:
    clock = ManualClock(datetime(2026, 3, 13, 12, 5, tzinfo=UTC))
    assert clock.advance(seconds=3) == datetime(2026, 3, 13, 12, 5, 3, tzinfo=UTC)
    assert validate_contract_price("0.57") == Decimal("0.57")
    assert validate_usd_price("82500.10") == Decimal("82500.10")
    assert validate_volatility("0.85") == Decimal("0.85")


def test_invalid_contract_price_and_window_are_rejected() -> None:
    with pytest.raises(ValueError):
        validate_contract_price("1.01")

    with pytest.raises(ValueError):
        Window(asset_id="BTC", window_start_ts=datetime(2026, 3, 13, 12, 6, tzinfo=UTC))
