import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from rtds.normalizers.exchange import (
    normalize_binance_quote,
    normalize_coinbase_quote,
    normalize_exchange_quote,
    normalize_kraken_quote,
)

FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "exchange_quotes"
)


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_normalize_binance_quote_fixture() -> None:
    quote = normalize_binance_quote(
        _load_fixture("binance_book_ticker.json"),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 195000, tzinfo=UTC),
    )

    assert quote.venue_id == "binance"
    assert quote.instrument_id == "binance:spot:BTCUSDT"
    assert quote.asset_id == "BTC"
    assert quote.event_ts == datetime(2026, 3, 13, 12, 7, 3, 180000, tzinfo=UTC)
    assert quote.recv_ts == datetime(2026, 3, 13, 12, 7, 3, 195000, tzinfo=UTC)
    assert quote.bid == Decimal("83186.90")
    assert quote.ask == Decimal("83187.30")
    assert quote.mid == Decimal("83187.10")
    assert quote.bid_size == Decimal("1.250")
    assert quote.ask_size == Decimal("0.980")
    assert quote.sequence_id == "48291015501"
    assert quote.quote_type == "bookTicker"
    assert quote.source_event_missing_ts_flag is False
    assert quote.normalization_status == "normalized"
    assert quote.raw_event_id.startswith("rawquote:")


def test_normalize_coinbase_quote_fixture() -> None:
    quote = normalize_coinbase_quote(
        _load_fixture("coinbase_ticker.json"),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 155000, tzinfo=UTC),
    )

    assert quote.venue_id == "coinbase"
    assert quote.instrument_id == "coinbase:spot:BTC-USD"
    assert quote.event_ts == datetime(2026, 3, 13, 12, 7, 3, 120000, tzinfo=UTC)
    assert quote.bid == Decimal("83185.70")
    assert quote.ask == Decimal("83186.10")
    assert quote.mid == Decimal("83185.90")
    assert quote.bid_size == Decimal("0.55")
    assert quote.ask_size == Decimal("0.42")
    assert quote.sequence_id == "5401"
    assert quote.quote_type == "ticker"
    assert quote.source_event_missing_ts_flag is False


def test_normalize_kraken_quote_fixture() -> None:
    quote = normalize_kraken_quote(
        _load_fixture("kraken_book.json"),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 130000, tzinfo=UTC),
    )

    assert quote.venue_id == "kraken"
    assert quote.instrument_id == "kraken:spot:BTC-USD"
    assert quote.event_ts == datetime(2026, 3, 13, 12, 7, 3, 100000, tzinfo=UTC)
    assert quote.bid == Decimal("83186.55")
    assert quote.ask == Decimal("83187.15")
    assert quote.mid == Decimal("83186.85")
    assert quote.bid_size == Decimal("0.80")
    assert quote.ask_size == Decimal("0.65")
    assert quote.sequence_id == "8241512"
    assert quote.quote_type == "book"
    assert quote.source_event_missing_ts_flag is False


def test_normalize_exchange_quote_falls_back_to_recv_ts_when_source_ts_missing() -> None:
    payload = _load_fixture("binance_book_ticker.json")
    payload.pop("E")
    recv_ts = datetime(2026, 3, 13, 12, 7, 3, 195000, tzinfo=UTC)

    quote = normalize_exchange_quote(
        venue="binance",
        payload=payload,
        recv_ts=recv_ts,
    )

    assert quote.event_ts == recv_ts
    assert quote.source_event_missing_ts_flag is True
    assert quote.normalization_status == "normalized_with_missing_event_ts"


def test_normalize_exchange_quote_rejects_non_btc_symbols() -> None:
    payload = _load_fixture("coinbase_ticker.json")
    payload["events"][0]["tickers"][0]["product_id"] = "ETH-USD"

    with pytest.raises(ValueError, match="only supports BTC spot symbols"):
        normalize_coinbase_quote(
            payload,
            recv_ts=datetime(2026, 3, 13, 12, 7, 3, 155000, tzinfo=UTC),
        )
