"""Exchange quote normalization for phase-1 BTC spot venues."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any, Callable

from rtds.core.enums import AssetCode, VenueCode
from rtds.core.ids import build_exchange_spot_instrument_id
from rtds.core.time import ensure_utc, parse_utc
from rtds.core.units import to_decimal
from rtds.schemas.normalized import SCHEMA_VERSION, ExchangeQuote

NORMALIZER_VERSION = "0.1.0"
BINANCE_QUOTE_TYPE = "book_ticker"
COINBASE_QUOTE_TYPE = "ticker"
KRAKEN_QUOTE_TYPE = "book"


def normalize_exchange_quote(
    *,
    venue: VenueCode | str,
    payload: dict[str, Any],
    recv_ts: datetime,
    proc_ts: datetime | None = None,
    raw_event_id: str | None = None,
) -> ExchangeQuote:
    """Normalize one exchange quote update into the canonical schema."""

    venue_code = _coerce_supported_venue(venue)
    normalized_recv_ts = ensure_utc(recv_ts, field_name="recv_ts")
    normalized_proc_ts = (
        ensure_utc(proc_ts, field_name="proc_ts")
        if proc_ts is not None
        else normalized_recv_ts
    )
    parser = _PARSERS[venue_code]
    parsed = parser(payload)
    event_ts = parsed["event_ts"] or normalized_recv_ts
    source_event_missing_ts_flag = parsed["event_ts"] is None
    best_bid = to_decimal(parsed["best_bid"], field_name="best_bid")
    best_ask = to_decimal(parsed["best_ask"], field_name="best_ask")

    return ExchangeQuote(
        venue_id=venue_code.value,
        instrument_id=str(
            build_exchange_spot_instrument_id(
                venue_code,
                _canonical_instrument_symbol(
                    venue_code=venue_code,
                    venue_symbol=str(parsed["venue_symbol"]),
                ),
            )
        ),
        asset_id=AssetCode.BTC.value,
        event_ts=event_ts,
        recv_ts=normalized_recv_ts,
        proc_ts=normalized_proc_ts,
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=(best_bid + best_ask) / Decimal("2"),
        bid_size=to_decimal(parsed["bid_size"], field_name="bid_size"),
        ask_size=to_decimal(parsed["ask_size"], field_name="ask_size"),
        raw_event_id=raw_event_id or _build_raw_event_id(
            venue_code=venue_code,
            recv_ts=normalized_recv_ts,
            payload=payload,
        ),
        normalizer_version=NORMALIZER_VERSION,
        schema_version=SCHEMA_VERSION,
        created_ts=normalized_proc_ts,
        quote_type=parsed["quote_type"],
        quote_depth_level=1,
        sequence_id=parsed["sequence_id"],
        source_event_missing_ts_flag=source_event_missing_ts_flag,
        crossed_market_flag=best_bid > best_ask,
        locked_market_flag=best_bid == best_ask,
        normalization_status=_normalization_status(
            source_event_missing_ts_flag=source_event_missing_ts_flag,
            best_bid=best_bid,
            best_ask=best_ask,
        ),
    )


def normalize_binance_quote(
    payload: dict[str, Any],
    *,
    recv_ts: datetime,
    proc_ts: datetime | None = None,
    raw_event_id: str | None = None,
) -> ExchangeQuote:
    """Normalize one Binance book-ticker style update."""

    return normalize_exchange_quote(
        venue=VenueCode.BINANCE,
        payload=payload,
        recv_ts=recv_ts,
        proc_ts=proc_ts,
        raw_event_id=raw_event_id,
    )


def normalize_coinbase_quote(
    payload: dict[str, Any],
    *,
    recv_ts: datetime,
    proc_ts: datetime | None = None,
    raw_event_id: str | None = None,
) -> ExchangeQuote:
    """Normalize one Coinbase ticker update."""

    return normalize_exchange_quote(
        venue=VenueCode.COINBASE,
        payload=payload,
        recv_ts=recv_ts,
        proc_ts=proc_ts,
        raw_event_id=raw_event_id,
    )


def normalize_kraken_quote(
    payload: dict[str, Any],
    *,
    recv_ts: datetime,
    proc_ts: datetime | None = None,
    raw_event_id: str | None = None,
) -> ExchangeQuote:
    """Normalize one Kraken book update."""

    return normalize_exchange_quote(
        venue=VenueCode.KRAKEN,
        payload=payload,
        recv_ts=recv_ts,
        proc_ts=proc_ts,
        raw_event_id=raw_event_id,
    )


def _coerce_supported_venue(venue: VenueCode | str) -> VenueCode:
    venue_code = venue if isinstance(venue, VenueCode) else VenueCode(str(venue).lower())
    if venue_code not in {
        VenueCode.BINANCE,
        VenueCode.COINBASE,
        VenueCode.KRAKEN,
    }:
        raise ValueError("phase-1 exchange quote normalization supports binance, coinbase, kraken")
    return venue_code


def _build_raw_event_id(
    *,
    venue_code: VenueCode,
    recv_ts: datetime,
    payload: dict[str, Any],
) -> str:
    digest = sha256()
    digest.update(venue_code.value.encode("utf-8"))
    digest.update(ensure_utc(recv_ts, field_name="recv_ts").isoformat().encode("utf-8"))
    digest.update(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    )
    return f"rawquote:{digest.hexdigest()}"


def _canonical_instrument_symbol(*, venue_code: VenueCode, venue_symbol: str) -> str:
    if venue_code is VenueCode.KRAKEN:
        return venue_symbol.replace("/", "-")
    return venue_symbol


def _normalization_status(
    *,
    source_event_missing_ts_flag: bool,
    best_bid: Decimal,
    best_ask: Decimal,
) -> str:
    if best_bid > best_ask:
        return "crossed_market"
    if best_bid == best_ask:
        return "locked_market"
    if source_event_missing_ts_flag:
        return "normalized_with_missing_event_ts"
    return "normalized"


def _parse_binance(payload: dict[str, Any]) -> dict[str, Any]:
    venue_symbol = _require_string(payload.get("s"), field_name="s")
    _require_btc_symbol(venue_symbol)
    event_ts_ms = payload.get("E")
    return {
        "venue_symbol": venue_symbol,
        "event_ts": _parse_epoch_millis(event_ts_ms),
        "best_bid": payload.get("b"),
        "best_ask": payload.get("a"),
        "bid_size": payload.get("B"),
        "ask_size": payload.get("A"),
        "sequence_id": _optional_string(payload.get("u")),
        "quote_type": _optional_string(payload.get("e")) or BINANCE_QUOTE_TYPE,
    }


def _parse_coinbase(payload: dict[str, Any]) -> dict[str, Any]:
    events = payload.get("events")
    if not isinstance(events, list) or not events:
        raise ValueError("coinbase payload must contain a non-empty events list")
    event_block = events[0]
    if not isinstance(event_block, dict):
        raise ValueError("coinbase event block must be an object")
    tickers = event_block.get("tickers")
    if not isinstance(tickers, list) or not tickers:
        raise ValueError("coinbase payload must contain a non-empty tickers list")
    ticker = tickers[0]
    if not isinstance(ticker, dict):
        raise ValueError("coinbase ticker entry must be an object")

    venue_symbol = _require_string(ticker.get("product_id"), field_name="product_id")
    _require_btc_symbol(venue_symbol)
    event_ts = _parse_optional_ts(
        payload.get("timestamp"),
        fallback=ticker.get("time"),
    )
    sequence = payload.get("sequence_num")
    if sequence is None:
        sequence = ticker.get("sequence_num")

    return {
        "venue_symbol": venue_symbol,
        "event_ts": event_ts,
        "best_bid": ticker.get("best_bid"),
        "best_ask": ticker.get("best_ask"),
        "bid_size": ticker.get("best_bid_quantity"),
        "ask_size": ticker.get("best_ask_quantity"),
        "sequence_id": _optional_string(sequence),
        "quote_type": _optional_string(payload.get("channel"))
        or _optional_string(event_block.get("type"))
        or COINBASE_QUOTE_TYPE,
    }


def _parse_kraken(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise ValueError("kraken payload must contain a non-empty data list")
    entry = data[0]
    if not isinstance(entry, dict):
        raise ValueError("kraken data entry must be an object")

    venue_symbol = _require_string(entry.get("symbol"), field_name="symbol")
    _require_btc_symbol(venue_symbol)
    bid_entry = _extract_price_level(entry.get("bids"), side="bids")
    ask_entry = _extract_price_level(entry.get("asks"), side="asks")

    return {
        "venue_symbol": venue_symbol,
        "event_ts": _parse_optional_ts(entry.get("timestamp")),
        "best_bid": bid_entry["price"],
        "best_ask": ask_entry["price"],
        "bid_size": bid_entry["size"],
        "ask_size": ask_entry["size"],
        "sequence_id": _optional_string(entry.get("checksum"))
        or _optional_string(entry.get("sequence")),
        "quote_type": _optional_string(payload.get("channel")) or KRAKEN_QUOTE_TYPE,
    }


def _extract_price_level(levels: Any, *, side: str) -> dict[str, Any]:
    if not isinstance(levels, list) or not levels:
        raise ValueError(f"kraken payload must contain a non-empty {side} list")
    first_level = levels[0]
    if not isinstance(first_level, dict):
        raise ValueError(f"kraken {side} entry must be an object")
    price = first_level.get("price")
    size = first_level.get("qty")
    if size is None:
        size = first_level.get("size")
    return {"price": price, "size": size}


def _parse_optional_ts(value: Any, *, fallback: Any = None) -> datetime | None:
    for candidate in (value, fallback):
        if candidate is None:
            continue
        text = _optional_string(candidate)
        if text is not None:
            return parse_utc(text)
    return None


def _parse_epoch_millis(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        millis = int(value)
    elif isinstance(value, (int, float)):
        millis = int(value)
    else:
        raise ValueError("epoch millisecond timestamp must be int, float, or string")
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


def _require_string(value: Any, *, field_name: str) -> str:
    text = _optional_string(value)
    if text is None:
        raise ValueError(f"{field_name} must be present")
    return text


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _require_btc_symbol(venue_symbol: str) -> None:
    normalized = venue_symbol.replace("-", "").replace("/", "").upper()
    if not normalized.startswith("BTC"):
        raise ValueError("phase-1 exchange quote normalization only supports BTC spot symbols")


_PARSERS: dict[VenueCode, Callable[[dict[str, Any]], dict[str, Any]]] = {
    VenueCode.BINANCE: _parse_binance,
    VenueCode.COINBASE: _parse_coinbase,
    VenueCode.KRAKEN: _parse_kraken,
}

__all__ = [
    "BINANCE_QUOTE_TYPE",
    "COINBASE_QUOTE_TYPE",
    "KRAKEN_QUOTE_TYPE",
    "NORMALIZER_VERSION",
    "normalize_binance_quote",
    "normalize_coinbase_quote",
    "normalize_exchange_quote",
    "normalize_kraken_quote",
]
