from datetime import UTC, datetime

import pytest

from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import (
    build_exchange_spot_instrument_id,
    build_oracle_feed_id,
    build_polymarket_market_id,
    build_snapshot_id,
    validate_exchange_spot_instrument_id,
    validate_oracle_feed_id,
    validate_polymarket_market_id,
    validate_snapshot_id,
)


def test_build_and_validate_exchange_spot_instrument_id() -> None:
    instrument_id = build_exchange_spot_instrument_id(VenueCode.BINANCE, "BTCUSDT")
    assert instrument_id == "binance:spot:BTCUSDT"
    assert validate_exchange_spot_instrument_id(str(instrument_id)) == instrument_id


def test_build_oracle_feed_id_uses_phase_one_shape() -> None:
    oracle_feed_id = build_oracle_feed_id(AssetCode.BTC)
    assert oracle_feed_id == "chainlink:stream:BTC-USD"
    assert validate_oracle_feed_id(str(oracle_feed_id)) == oracle_feed_id


def test_polymarket_market_id_rejects_whitespace() -> None:
    with pytest.raises(ValueError):
        build_polymarket_market_id("0xabc 123")

    assert validate_polymarket_market_id("0xabc123") == "0xabc123"


def test_snapshot_id_round_trip() -> None:
    snapshot_id = build_snapshot_id(
        "btc-5m-20260313T120500Z",
        "0xabc123",
        datetime(2026, 3, 13, 12, 7, 3, 250000, tzinfo=UTC),
    )
    assert snapshot_id == "snap:btc-5m-20260313T120500Z:0xabc123:20260313T120703250Z"
    assert validate_snapshot_id(str(snapshot_id)) == snapshot_id
