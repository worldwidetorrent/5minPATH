"""Canonical ID builders and validators."""

from __future__ import annotations

import re
from datetime import datetime

from rtds.core.enums import (
    SPOT_EXCHANGE_VENUES,
    AssetCode,
    InstrumentType,
    VenueCode,
)
from rtds.core.time import format_utc_compact, is_5m_boundary, parse_utc
from rtds.core.types import InstrumentId, MarketId, OracleFeedId, SnapshotId, WindowId

WINDOW_ID_RE = re.compile(r"^(?P<asset>[a-z]+)-5m-(?P<ts>\d{8}T\d{6}Z)$")
SNAPSHOT_ID_RE = re.compile(
    r"^snap:(?P<window_id>[^:]+):(?P<market_id>[^:]+):(?P<ts>\d{8}T\d{9}Z)$"
)
ORACLE_FEED_ID_RE = re.compile(r"^(?P<venue>[a-z]+):(?P<feed_type>[a-z_]+):(?P<symbol>[A-Z0-9-]+)$")
EXCHANGE_SPOT_ID_RE = re.compile(r"^(?P<venue>[a-z]+):spot:(?P<symbol>[A-Z0-9._-]+)$")


def _coerce_asset_code(asset: AssetCode | str) -> AssetCode:
    return asset if isinstance(asset, AssetCode) else AssetCode(str(asset).upper())


def _coerce_venue_code(venue: VenueCode | str) -> VenueCode:
    return venue if isinstance(venue, VenueCode) else VenueCode(str(venue).lower())


def build_window_id(asset: AssetCode | str, window_start_ts: datetime) -> WindowId:
    """Build a canonical window ID from an aligned window start."""

    asset_code = _coerce_asset_code(asset)
    if not is_5m_boundary(window_start_ts):
        raise ValueError("window_start_ts must be aligned to an exact 5-minute boundary")
    return WindowId(f"{asset_code.value.lower()}-5m-{format_utc_compact(window_start_ts)}")


def parse_window_id(window_id: str) -> tuple[AssetCode, datetime]:
    """Parse and validate a canonical window ID."""

    match = WINDOW_ID_RE.fullmatch(window_id)
    if match is None:
        raise ValueError("window_id must match <asset>-5m-<YYYYMMDDTHHMMSSZ>")

    asset = AssetCode(match.group("asset").upper())
    start_ts = parse_utc(match.group("ts"))
    if not is_5m_boundary(start_ts):
        raise ValueError("window_id timestamp must be aligned to an exact 5-minute boundary")
    return asset, start_ts


def validate_window_id(window_id: str) -> WindowId:
    """Return the typed window ID when valid."""

    parse_window_id(window_id)
    return WindowId(window_id)


def build_snapshot_id(window_id: str, market_id: str, snapshot_ts: datetime) -> SnapshotId:
    """Build a human-readable snapshot identifier."""

    validated_window_id = validate_window_id(window_id)
    validated_market_id = validate_polymarket_market_id(market_id)
    timestamp_token = format_utc_compact(snapshot_ts, include_millis=True)
    return SnapshotId(
        f"snap:{validated_window_id}:{validated_market_id}:{timestamp_token}"
    )


def validate_snapshot_id(snapshot_id: str) -> SnapshotId:
    """Validate canonical snapshot IDs."""

    match = SNAPSHOT_ID_RE.fullmatch(snapshot_id)
    if match is None:
        raise ValueError(
            "snapshot_id must match snap:<window_id>:<market_id>:<YYYYMMDDTHHMMSSmmmZ>"
        )

    validate_window_id(match.group("window_id"))
    validate_polymarket_market_id(match.group("market_id"))
    parse_utc(match.group("ts"))
    return SnapshotId(snapshot_id)


def build_oracle_feed_id(
    asset: AssetCode | str,
    *,
    quote_symbol: str = "USD",
    venue: VenueCode | str = VenueCode.CHAINLINK,
    feed_type: InstrumentType | str = InstrumentType.STREAM,
) -> OracleFeedId:
    """Build a canonical oracle feed identifier."""

    asset_code = _coerce_asset_code(asset)
    venue_code = _coerce_venue_code(venue)
    instrument_type = (
        feed_type
        if isinstance(feed_type, InstrumentType)
        else InstrumentType(str(feed_type).lower())
    )
    if venue_code is not VenueCode.CHAINLINK:
        raise ValueError("phase-1 oracle_feed_id values must use the chainlink venue")
    symbol = f"{asset_code.value}-{quote_symbol.strip().upper()}"
    return OracleFeedId(f"{venue_code.value}:{instrument_type.value}:{symbol}")


def validate_oracle_feed_id(oracle_feed_id: str) -> OracleFeedId:
    """Validate canonical oracle feed IDs."""

    match = ORACLE_FEED_ID_RE.fullmatch(oracle_feed_id)
    if match is None:
        raise ValueError("oracle_feed_id must match <venue>:<feed_type>:<SYMBOL>")
    venue = _coerce_venue_code(match.group("venue"))
    if venue is not VenueCode.CHAINLINK:
        raise ValueError("phase-1 oracle feeds must use the chainlink venue")
    InstrumentType(match.group("feed_type"))
    return OracleFeedId(oracle_feed_id)


def build_exchange_spot_instrument_id(venue: VenueCode | str, venue_symbol: str) -> InstrumentId:
    """Build an exchange spot instrument identifier."""

    venue_code = _coerce_venue_code(venue)
    if venue_code not in SPOT_EXCHANGE_VENUES:
        raise ValueError("exchange spot instrument IDs require a supported spot exchange venue")
    symbol = venue_symbol.strip()
    if not symbol:
        raise ValueError("venue_symbol must not be empty")
    return InstrumentId(f"{venue_code.value}:{InstrumentType.SPOT.value}:{symbol}")


def validate_exchange_spot_instrument_id(instrument_id: str) -> InstrumentId:
    """Validate exchange spot instrument identifiers."""

    match = EXCHANGE_SPOT_ID_RE.fullmatch(instrument_id)
    if match is None:
        raise ValueError("instrument_id must match <venue>:spot:<venue_symbol>")
    venue = _coerce_venue_code(match.group("venue"))
    if venue not in SPOT_EXCHANGE_VENUES:
        raise ValueError("instrument_id venue must be a supported spot exchange")
    return InstrumentId(instrument_id)


def build_polymarket_market_id(native_market_id: str) -> MarketId:
    """Canonicalize a Polymarket market identifier."""

    market_id = native_market_id.strip()
    if not market_id:
        raise ValueError("market_id must not be empty")
    if any(char.isspace() for char in market_id):
        raise ValueError("market_id must not contain whitespace")
    return MarketId(market_id)


def validate_polymarket_market_id(market_id: str) -> MarketId:
    """Validate a Polymarket market identifier."""

    return build_polymarket_market_id(market_id)


__all__ = [
    "EXCHANGE_SPOT_ID_RE",
    "ORACLE_FEED_ID_RE",
    "SNAPSHOT_ID_RE",
    "WINDOW_ID_RE",
    "build_exchange_spot_instrument_id",
    "build_oracle_feed_id",
    "build_polymarket_market_id",
    "build_snapshot_id",
    "build_window_id",
    "parse_window_id",
    "validate_exchange_spot_instrument_id",
    "validate_oracle_feed_id",
    "validate_polymarket_market_id",
    "validate_snapshot_id",
    "validate_window_id",
]
