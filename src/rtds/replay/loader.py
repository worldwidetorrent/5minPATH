"""Replay dataset loading helpers."""

from __future__ import annotations

import json
from dataclasses import fields
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from rtds.collectors.polymarket.metadata import MarketMetadataCandidate
from rtds.core.time import parse_utc
from rtds.mapping.anchor_assignment import (
    DEFAULT_ORACLE_FEED_ID,
    ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
    ChainlinkTick,
)
from rtds.schemas.normalized import ExchangeQuote, PolymarketQuote
from rtds.schemas.snapshot import SnapshotRecord
from rtds.schemas.window_reference import WindowReferenceRecord


def load_window_references(
    data_root: str | Path,
    *,
    date_utc: date | str,
) -> list[WindowReferenceRecord]:
    """Load persisted window-reference rows for one UTC date."""

    root = Path(data_root) / "reference" / "window_reference" / f"date={_normalize_date(date_utc)}"
    return [WindowReferenceRecord.from_storage_dict(row) for row in _read_jsonl_dir(root)]


def load_exchange_quotes(
    data_root: str | Path,
    *,
    date_utc: date | str,
    session_id: str | None = None,
) -> list[ExchangeQuote]:
    """Load normalized exchange quotes for one UTC date."""

    root = _session_partition_root(
        Path(data_root) / "normalized" / "exchange_quotes" / f"date={_normalize_date(date_utc)}",
        session_id=session_id,
    )
    return [_row_to_exchange_quote(row) for row in _read_jsonl_dir(root)]


def load_polymarket_quotes(
    data_root: str | Path,
    *,
    date_utc: date | str,
    session_id: str | None = None,
) -> list[PolymarketQuote]:
    """Load normalized Polymarket quotes for one UTC date."""

    root = _session_partition_root(
        Path(data_root)
        / "normalized"
        / "polymarket_quotes"
        / f"date={_normalize_date(date_utc)}",
        session_id=session_id,
    )
    return [_row_to_polymarket_quote(row) for row in _read_jsonl_dir(root)]


def load_chainlink_ticks(
    data_root: str | Path,
    *,
    date_utc: date | str,
    session_id: str | None = None,
) -> list[ChainlinkTick]:
    """Load normalized Chainlink ticks for one UTC date."""

    root = _session_partition_root(
        Path(data_root) / "normalized" / "chainlink_ticks" / f"date={_normalize_date(date_utc)}",
        session_id=session_id,
    )
    return [_row_to_chainlink_tick(row) for row in _read_jsonl_dir(root)]


def load_metadata_candidates(
    data_root: str | Path,
    *,
    date_utc: date | str,
    session_id: str | None = None,
) -> list[MarketMetadataCandidate]:
    """Load normalized Polymarket metadata candidates for one UTC date."""

    normalized_root = Path(data_root) / "normalized"
    partition = f"date={_normalize_date(date_utc)}"
    candidate_roots = (
        normalized_root / "market_metadata_events" / partition,
        normalized_root / "polymarket_metadata" / partition,
    )
    for root in candidate_roots:
        rows = _read_jsonl_dir(_session_partition_root(root, session_id=session_id))
        if rows:
            return [_row_to_metadata_candidate(row) for row in rows]
    return []


def load_snapshots(snapshot_root: str | Path) -> list[SnapshotRecord]:
    """Load snapshot rows from a JSONL artifact directory or file."""

    root = Path(snapshot_root)
    rows = _read_jsonl_dir(root) if root.is_dir() else _read_jsonl_file(root)
    return [SnapshotRecord.from_storage_dict(row) for row in rows]


def _read_jsonl_dir(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.jsonl")):
        rows.extend(_read_jsonl_file(path))
    return rows


def _read_jsonl_file(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        payload = json.loads(candidate)
        if not isinstance(payload, dict):
            raise ValueError(f"expected JSON object row in {path}")
        rows.append(payload)
    return rows


def _normalize_date(value: date | str) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(value).isoformat()


def _session_partition_root(root: Path, *, session_id: str | None) -> Path:
    if session_id is None:
        return root
    return root / f"session={session_id}"


def _row_to_exchange_quote(row: dict[str, Any]) -> ExchangeQuote:
    return ExchangeQuote(
        venue_id=str(row["venue_id"]),
        instrument_id=str(row["instrument_id"]),
        asset_id=str(row["asset_id"]),
        event_ts=parse_utc(str(row["event_ts"])),
        recv_ts=parse_utc(str(row["recv_ts"])),
        proc_ts=parse_utc(str(row["proc_ts"])),
        best_bid=Decimal(str(row["best_bid"])),
        best_ask=Decimal(str(row["best_ask"])),
        mid_price=Decimal(str(row["mid_price"])),
        bid_size=Decimal(str(row["bid_size"])),
        ask_size=Decimal(str(row["ask_size"])),
        raw_event_id=str(row["raw_event_id"]),
        normalizer_version=str(row["normalizer_version"]),
        schema_version=str(row["schema_version"]),
        created_ts=parse_utc(str(row["created_ts"])),
        quote_type=_optional_str(row.get("quote_type")),
        quote_depth_level=_optional_int(row.get("quote_depth_level")),
        sequence_id=_optional_str(row.get("sequence_id")),
        source_event_missing_ts_flag=bool(row.get("source_event_missing_ts_flag", False)),
        crossed_market_flag=bool(row.get("crossed_market_flag", False)),
        locked_market_flag=bool(row.get("locked_market_flag", False)),
        normalization_status=str(row.get("normalization_status", "normalized")),
    )


def _row_to_polymarket_quote(row: dict[str, Any]) -> PolymarketQuote:
    return PolymarketQuote(
        venue_id=str(row["venue_id"]),
        market_id=str(row["market_id"]),
        asset_id=str(row["asset_id"]),
        event_ts=parse_utc(str(row["event_ts"])),
        recv_ts=parse_utc(str(row["recv_ts"])),
        proc_ts=parse_utc(str(row["proc_ts"])),
        up_bid=Decimal(str(row["up_bid"])),
        up_ask=Decimal(str(row["up_ask"])),
        down_bid=Decimal(str(row["down_bid"])),
        down_ask=Decimal(str(row["down_ask"])),
        up_bid_size_contracts=Decimal(str(row["up_bid_size_contracts"])),
        up_ask_size_contracts=Decimal(str(row["up_ask_size_contracts"])),
        down_bid_size_contracts=Decimal(str(row["down_bid_size_contracts"])),
        down_ask_size_contracts=Decimal(str(row["down_ask_size_contracts"])),
        raw_event_id=str(row["raw_event_id"]),
        normalizer_version=str(row["normalizer_version"]),
        schema_version=str(row["schema_version"]),
        created_ts=parse_utc(str(row["created_ts"])),
        token_yes_id=_optional_str(row.get("token_yes_id")),
        token_no_id=_optional_str(row.get("token_no_id")),
        market_quote_type=_optional_str(row.get("market_quote_type")),
        quote_sequence_id=_optional_str(row.get("quote_sequence_id")),
        market_mid_up=_optional_decimal(row.get("market_mid_up")),
        market_mid_down=_optional_decimal(row.get("market_mid_down")),
        market_spread_up_abs=_optional_decimal(row.get("market_spread_up_abs")),
        market_spread_down_abs=_optional_decimal(row.get("market_spread_down_abs")),
        last_trade_price=_optional_decimal(row.get("last_trade_price")),
        last_trade_size_contracts=_optional_decimal(row.get("last_trade_size_contracts")),
        last_trade_side=_optional_str(row.get("last_trade_side")),
        last_trade_outcome=_optional_str(row.get("last_trade_outcome")),
        source_event_missing_ts_flag=bool(row.get("source_event_missing_ts_flag", False)),
        crossed_market_flag=bool(row.get("crossed_market_flag", False)),
        locked_market_flag=bool(row.get("locked_market_flag", False)),
        quote_completeness_flag=bool(row.get("quote_completeness_flag", True)),
        normalization_status=str(row.get("normalization_status", "normalized")),
    )


def _row_to_chainlink_tick(row: dict[str, Any]) -> ChainlinkTick:
    return ChainlinkTick(
        event_id=str(row["event_id"]),
        event_ts=parse_utc(str(row["event_ts"])),
        price=Decimal(str(row["price"])),
        recv_ts=None if row.get("recv_ts") is None else parse_utc(str(row["recv_ts"])),
        oracle_feed_id=str(row.get("oracle_feed_id", DEFAULT_ORACLE_FEED_ID)),
        round_id=_optional_str(row.get("round_id")),
        oracle_source=str(row.get("oracle_source", ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC)),
        bid_price=_optional_decimal(row.get("bid_price")),
        ask_price=_optional_decimal(row.get("ask_price")),
    )


def _row_to_metadata_candidate(row: dict[str, Any]) -> MarketMetadataCandidate:
    init_values: dict[str, Any] = {}
    datetime_fields = {
        "recv_ts",
        "proc_ts",
        "created_ts",
        "event_ts",
        "market_open_ts",
        "market_close_ts",
    }
    for field in fields(MarketMetadataCandidate):
        value = row.get(field.name)
        if value is None:
            init_values[field.name] = None
        elif field.name in datetime_fields:
            init_values[field.name] = parse_utc(str(value))
        else:
            init_values[field.name] = value
    return MarketMetadataCandidate(**init_values)


def _optional_decimal(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "load_chainlink_ticks",
    "load_exchange_quotes",
    "load_metadata_candidates",
    "load_polymarket_quotes",
    "load_snapshots",
    "load_window_references",
]
