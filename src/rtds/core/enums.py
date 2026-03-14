"""Stable project-wide enums."""

from __future__ import annotations

from enum import StrEnum


class AssetCode(StrEnum):
    """Supported canonical asset identifiers."""

    BTC = "BTC"


class AssetClass(StrEnum):
    """Supported asset classes."""

    CRYPTO = "crypto"


class VenueCode(StrEnum):
    """Supported canonical venue identifiers."""

    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    OKX = "okx"
    BYBIT = "bybit"
    POLYMARKET = "polymarket"
    CHAINLINK = "chainlink"


class VenueType(StrEnum):
    """Supported venue families."""

    SPOT_EXCHANGE = "spot_exchange"
    PREDICTION_MARKET = "prediction_market"
    ORACLE = "oracle"
    SYNTHETIC = "synthetic"


class InstrumentType(StrEnum):
    """Supported instrument/feed types."""

    SPOT = "spot"
    BINARY = "binary"
    STREAM = "stream"
    INDEX = "index"
    METADATA = "metadata"


class MarketType(StrEnum):
    """Supported market types."""

    BINARY_UPDOWN_5M = "binary_updown_5m"


class WindowType(StrEnum):
    """Supported canonical window types."""

    UPDOWN_5M = "updown_5m"


class ConfidenceLevel(StrEnum):
    """Shared confidence vocabulary."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class StatusValue(StrEnum):
    """Shared lifecycle/status vocabulary."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DELISTED = "delisted"
    RESOLVED = "resolved"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class SnapshotOrigin(StrEnum):
    """Snapshot creation origins from ADR 0003."""

    FIXED_1S = "fixed_1s"
    EVENT_POLYMARKET_QUOTE = "event_polymarket_quote"
    EVENT_CHAINLINK_TICK = "event_chainlink_tick"
    EVENT_COMPOSITE_MOVE = "event_composite_move"
    EVENT_QUALITY_TRANSITION = "event_quality_transition"


SPOT_EXCHANGE_VENUES: tuple[VenueCode, ...] = (
    VenueCode.BINANCE,
    VenueCode.COINBASE,
    VenueCode.KRAKEN,
    VenueCode.OKX,
    VenueCode.BYBIT,
)

VENUE_TYPE_BY_CODE: dict[VenueCode, VenueType] = {
    VenueCode.BINANCE: VenueType.SPOT_EXCHANGE,
    VenueCode.COINBASE: VenueType.SPOT_EXCHANGE,
    VenueCode.KRAKEN: VenueType.SPOT_EXCHANGE,
    VenueCode.OKX: VenueType.SPOT_EXCHANGE,
    VenueCode.BYBIT: VenueType.SPOT_EXCHANGE,
    VenueCode.POLYMARKET: VenueType.PREDICTION_MARKET,
    VenueCode.CHAINLINK: VenueType.ORACLE,
}

ASSET_DISPLAY_NAMES: dict[AssetCode, str] = {
    AssetCode.BTC: "Bitcoin",
}

__all__ = [
    "ASSET_DISPLAY_NAMES",
    "SPOT_EXCHANGE_VENUES",
    "AssetClass",
    "AssetCode",
    "ConfidenceLevel",
    "InstrumentType",
    "MarketType",
    "SnapshotOrigin",
    "StatusValue",
    "VENUE_TYPE_BY_CODE",
    "VenueCode",
    "VenueType",
    "WindowType",
]
