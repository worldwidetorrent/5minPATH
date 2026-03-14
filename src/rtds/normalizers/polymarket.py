"""Polymarket executable quote normalization."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any

from rtds.core.enums import AssetCode, VenueCode
from rtds.core.time import ensure_utc, parse_utc
from rtds.core.units import to_decimal
from rtds.schemas.normalized import SCHEMA_VERSION, PolymarketQuote

NORMALIZER_VERSION = "0.1.0"
QUOTE_TYPE = "orderbook_top"


def normalize_polymarket_quote(
    payload: dict[str, Any],
    *,
    recv_ts: datetime,
    proc_ts: datetime | None = None,
    raw_event_id: str | None = None,
) -> PolymarketQuote:
    """Normalize one Polymarket top-of-book payload into canonical quote state."""

    normalized_recv_ts = ensure_utc(recv_ts, field_name="recv_ts")
    normalized_proc_ts = (
        ensure_utc(proc_ts, field_name="proc_ts")
        if proc_ts is not None
        else normalized_recv_ts
    )
    market_id = _require_string(payload.get("market_id"), field_name="market_id")
    event_ts = _parse_optional_ts(payload.get("event_ts"))
    source_event_missing_ts_flag = event_ts is None
    normalized_event_ts = event_ts or normalized_recv_ts

    outcomes = payload.get("outcomes")
    if not isinstance(outcomes, dict):
        raise ValueError("payload must contain an outcomes object")

    up_book = _extract_outcome_book(outcomes, primary_key="up", fallback_key="yes")
    down_book = _extract_outcome_book(outcomes, primary_key="down", fallback_key="no")

    up_bid = to_decimal(up_book["bid_price"], field_name="up_bid")
    up_ask = to_decimal(up_book["ask_price"], field_name="up_ask")
    down_bid = to_decimal(down_book["bid_price"], field_name="down_bid")
    down_ask = to_decimal(down_book["ask_price"], field_name="down_ask")

    return PolymarketQuote(
        venue_id=VenueCode.POLYMARKET.value,
        market_id=market_id,
        asset_id=_coerce_asset_id(payload.get("asset_id")),
        event_ts=normalized_event_ts,
        recv_ts=normalized_recv_ts,
        proc_ts=normalized_proc_ts,
        up_bid=up_bid,
        up_ask=up_ask,
        down_bid=down_bid,
        down_ask=down_ask,
        up_bid_size_contracts=to_decimal(
            up_book["bid_size"],
            field_name="up_bid_size_contracts",
        ),
        up_ask_size_contracts=to_decimal(
            up_book["ask_size"],
            field_name="up_ask_size_contracts",
        ),
        down_bid_size_contracts=to_decimal(
            down_book["bid_size"],
            field_name="down_bid_size_contracts",
        ),
        down_ask_size_contracts=to_decimal(
            down_book["ask_size"],
            field_name="down_ask_size_contracts",
        ),
        raw_event_id=raw_event_id
        or _build_raw_event_id(
            market_id=market_id,
            recv_ts=normalized_recv_ts,
            payload=payload,
        ),
        normalizer_version=NORMALIZER_VERSION,
        schema_version=SCHEMA_VERSION,
        created_ts=normalized_proc_ts,
        token_yes_id=_optional_string(
            payload.get("token_yes_id") or payload.get("clob_token_id_up")
        ),
        token_no_id=_optional_string(
            payload.get("token_no_id") or payload.get("clob_token_id_down")
        ),
        market_quote_type=_optional_string(payload.get("quote_type")) or QUOTE_TYPE,
        quote_sequence_id=_optional_string(payload.get("sequence_id")),
        last_trade_price=_last_trade_value(payload, "price"),
        last_trade_size_contracts=_last_trade_value(payload, "size_contracts"),
        last_trade_side=_last_trade_side(payload),
        last_trade_outcome=_last_trade_outcome(payload),
        source_event_missing_ts_flag=source_event_missing_ts_flag,
        crossed_market_flag=up_bid > up_ask or down_bid > down_ask,
        locked_market_flag=up_bid == up_ask or down_bid == down_ask,
        quote_completeness_flag=True,
        normalization_status=_normalization_status(
            source_event_missing_ts_flag=source_event_missing_ts_flag,
            up_bid=up_bid,
            up_ask=up_ask,
            down_bid=down_bid,
            down_ask=down_ask,
        ),
    )


def _extract_outcome_book(
    outcomes: dict[str, Any],
    *,
    primary_key: str,
    fallback_key: str,
) -> dict[str, Any]:
    raw_book = outcomes.get(primary_key)
    if raw_book is None:
        raw_book = outcomes.get(fallback_key)
    if not isinstance(raw_book, dict):
        raise ValueError(f"payload must contain a {primary_key}/{fallback_key} outcome book")
    return {
        "bid_price": _extract_level_value(raw_book, "bid", "price"),
        "bid_size": _extract_level_value(raw_book, "bid", "size"),
        "ask_price": _extract_level_value(raw_book, "ask", "price"),
        "ask_size": _extract_level_value(raw_book, "ask", "size"),
    }


def _extract_level_value(book: dict[str, Any], level_key: str, field_name: str) -> Any:
    level = book.get(level_key)
    if not isinstance(level, dict):
        raise ValueError(f"outcome book must contain a {level_key} object")
    value = level.get(field_name)
    if value is None:
        raise ValueError(f"outcome {level_key} must contain {field_name}")
    return value


def _coerce_asset_id(value: Any) -> str:
    asset_id = _optional_string(value)
    if asset_id is None:
        return AssetCode.BTC.value
    if asset_id.upper() != AssetCode.BTC.value:
        raise ValueError("phase-1 Polymarket quote normalization only supports BTC markets")
    return AssetCode.BTC.value


def _last_trade_value(payload: dict[str, Any], key: str) -> Decimal | None:
    last_trade = payload.get("last_trade")
    if not isinstance(last_trade, dict):
        return None
    value = last_trade.get(key)
    if value is None:
        return None
    return to_decimal(value, field_name=f"last_trade_{key}")


def _last_trade_side(payload: dict[str, Any]) -> str | None:
    last_trade = payload.get("last_trade")
    if not isinstance(last_trade, dict):
        return None
    return _optional_string(last_trade.get("side"))


def _last_trade_outcome(payload: dict[str, Any]) -> str | None:
    last_trade = payload.get("last_trade")
    if not isinstance(last_trade, dict):
        return None
    outcome = _optional_string(last_trade.get("outcome"))
    if outcome is None:
        return None
    lowered = outcome.lower()
    if lowered == "yes":
        return "up"
    if lowered == "no":
        return "down"
    return lowered


def _build_raw_event_id(
    *,
    market_id: str,
    recv_ts: datetime,
    payload: dict[str, Any],
) -> str:
    digest = sha256()
    digest.update(market_id.encode("utf-8"))
    digest.update(ensure_utc(recv_ts, field_name="recv_ts").isoformat().encode("utf-8"))
    digest.update(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    )
    return f"rawpolyquote:{digest.hexdigest()}"


def _parse_optional_ts(value: Any) -> datetime | None:
    text = _optional_string(value)
    if text is None:
        return None
    return parse_utc(text)


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


def _normalization_status(
    *,
    source_event_missing_ts_flag: bool,
    up_bid: Decimal,
    up_ask: Decimal,
    down_bid: Decimal,
    down_ask: Decimal,
) -> str:
    if up_bid > up_ask or down_bid > down_ask:
        return "crossed_market"
    if up_bid == up_ask or down_bid == down_ask:
        return "locked_market"
    if source_event_missing_ts_flag:
        return "normalized_with_missing_event_ts"
    return "normalized"


__all__ = [
    "NORMALIZER_VERSION",
    "QUOTE_TYPE",
    "normalize_polymarket_quote",
]
