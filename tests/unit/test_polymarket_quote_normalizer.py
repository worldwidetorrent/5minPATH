import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from rtds.normalizers.polymarket import normalize_polymarket_quote

FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "polymarket_quotes"
)


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_normalize_polymarket_quote_fixture() -> None:
    quote = normalize_polymarket_quote(
        _load_fixture("book_snapshot.json"),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 280000, tzinfo=UTC),
    )

    assert quote.venue_id == "polymarket"
    assert quote.market_id == "0xbtc1210"
    assert quote.asset_id == "BTC"
    assert quote.event_ts == datetime(2026, 3, 13, 12, 7, 3, 250000, tzinfo=UTC)
    assert quote.recv_ts == datetime(2026, 3, 13, 12, 7, 3, 280000, tzinfo=UTC)
    assert quote.up_bid == Decimal("0.28")
    assert quote.up_ask == Decimal("0.31")
    assert quote.down_bid == Decimal("0.70")
    assert quote.down_ask == Decimal("0.73")
    assert quote.up_bid_size_contracts == Decimal("250")
    assert quote.up_ask_size_contracts == Decimal("180")
    assert quote.down_bid_size_contracts == Decimal("320")
    assert quote.down_ask_size_contracts == Decimal("210")
    assert quote.market_mid_up == Decimal("0.295")
    assert quote.market_mid_down == Decimal("0.715")
    assert quote.market_spread_up_abs == Decimal("0.03")
    assert quote.market_spread_down_abs == Decimal("0.03")
    assert quote.token_yes_id == "btc1210-yes"
    assert quote.token_no_id == "btc1210-no"
    assert quote.last_trade_price == Decimal("0.30")
    assert quote.last_trade_size_contracts == Decimal("50")
    assert quote.last_trade_side == "buy"
    assert quote.last_trade_outcome == "up"
    assert quote.quote_sequence_id == "pmq-1001"
    assert quote.market_quote_type == "orderbook_top"
    assert quote.source_event_missing_ts_flag is False
    assert quote.normalization_status == "normalized"
    assert quote.raw_event_id.startswith("rawpolyquote:")


def test_normalize_polymarket_quote_uses_yes_no_fallback_and_recv_ts() -> None:
    recv_ts = datetime(2026, 3, 13, 12, 7, 5, 100000, tzinfo=UTC)
    quote = normalize_polymarket_quote(
        _load_fixture("book_snapshot_yes_no.json"),
        recv_ts=recv_ts,
    )

    assert quote.market_id == "0xbtc1215"
    assert quote.event_ts == recv_ts
    assert quote.token_yes_id == "btc1215-yes"
    assert quote.token_no_id == "btc1215-no"
    assert quote.up_bid == Decimal("0.46")
    assert quote.up_ask == Decimal("0.49")
    assert quote.down_bid == Decimal("0.51")
    assert quote.down_ask == Decimal("0.54")
    assert quote.source_event_missing_ts_flag is True
    assert quote.normalization_status == "normalized_with_missing_event_ts"


def test_normalize_polymarket_quote_rejects_incomplete_books() -> None:
    payload = _load_fixture("book_snapshot.json")
    del payload["outcomes"]["down"]["ask"]

    with pytest.raises(ValueError, match="outcome book must contain a ask object"):
        normalize_polymarket_quote(
            payload,
            recv_ts=datetime(2026, 3, 13, 12, 7, 3, 280000, tzinfo=UTC),
        )


def test_normalize_polymarket_quote_rejects_non_btc_asset() -> None:
    payload = _load_fixture("book_snapshot.json")
    payload["asset_id"] = "ETH"

    with pytest.raises(ValueError, match="only supports BTC markets"):
        normalize_polymarket_quote(
            payload,
            recv_ts=datetime(2026, 3, 13, 12, 7, 3, 280000, tzinfo=UTC),
        )
