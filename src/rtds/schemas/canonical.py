"""Canonical core entity models."""

from __future__ import annotations

from dataclasses import dataclass

from rtds.core.enums import (
    ASSET_DISPLAY_NAMES,
    VENUE_TYPE_BY_CODE,
    AssetClass,
    AssetCode,
    InstrumentType,
    MarketType,
    SnapshotOrigin,
    StatusValue,
    VenueCode,
    VenueType,
    WindowType,
)
from rtds.core.ids import (
    build_exchange_spot_instrument_id,
    build_oracle_feed_id,
    build_snapshot_id,
    build_window_id,
    validate_oracle_feed_id,
    validate_polymarket_market_id,
    validate_window_id,
)
from rtds.core.time import ensure_utc, is_5m_boundary, window_end
from rtds.core.types import UTCDateTime

SCHEMA_VERSION = "0.1.0"


def _coerce_asset_code(value: AssetCode | str) -> AssetCode:
    return value if isinstance(value, AssetCode) else AssetCode(str(value).upper())


def _coerce_venue_code(value: VenueCode | str) -> VenueCode:
    return value if isinstance(value, VenueCode) else VenueCode(str(value).lower())


@dataclass(slots=True, frozen=True)
class Asset:
    """Canonical asset entity."""

    asset_id: AssetCode | str
    asset_class: AssetClass | str = AssetClass.CRYPTO
    base_symbol: str | None = None
    quote_symbol: str | None = "USD"
    display_name: str | None = None
    status: StatusValue | str = StatusValue.ACTIVE

    def __post_init__(self) -> None:
        asset_code = _coerce_asset_code(self.asset_id)
        asset_class = (
            self.asset_class
            if isinstance(self.asset_class, AssetClass)
            else AssetClass(self.asset_class)
        )
        status = self.status if isinstance(self.status, StatusValue) else StatusValue(self.status)

        object.__setattr__(self, "asset_id", asset_code)
        object.__setattr__(self, "asset_class", asset_class)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "base_symbol", self.base_symbol or asset_code.value)
        object.__setattr__(
            self,
            "display_name",
            self.display_name or ASSET_DISPLAY_NAMES[asset_code],
        )
        if self.quote_symbol is not None:
            object.__setattr__(self, "quote_symbol", self.quote_symbol.upper())


@dataclass(slots=True, frozen=True)
class Venue:
    """Canonical venue entity."""

    venue_id: VenueCode | str
    venue_type: VenueType | str | None = None
    display_name: str | None = None
    status: StatusValue | str = StatusValue.ACTIVE

    def __post_init__(self) -> None:
        venue_code = _coerce_venue_code(self.venue_id)
        venue_type = (
            VENUE_TYPE_BY_CODE[venue_code]
            if self.venue_type is None
            else self.venue_type
            if isinstance(self.venue_type, VenueType)
            else VenueType(self.venue_type)
        )
        status = self.status if isinstance(self.status, StatusValue) else StatusValue(self.status)

        object.__setattr__(self, "venue_id", venue_code)
        object.__setattr__(self, "venue_type", venue_type)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "display_name", self.display_name or venue_code.value.upper())


@dataclass(slots=True, frozen=True)
class Instrument:
    """Canonical venue-scoped instrument."""

    venue_id: VenueCode | str
    asset_id: AssetCode | str
    instrument_type: InstrumentType | str
    venue_symbol: str
    quote_ccy: str | None = "USD"
    instrument_id: str | None = None
    status: StatusValue | str = StatusValue.ACTIVE

    def __post_init__(self) -> None:
        venue_code = _coerce_venue_code(self.venue_id)
        asset_code = _coerce_asset_code(self.asset_id)
        instrument_type = (
            self.instrument_type
            if isinstance(self.instrument_type, InstrumentType)
            else InstrumentType(self.instrument_type)
        )
        status = self.status if isinstance(self.status, StatusValue) else StatusValue(self.status)
        venue_symbol = self.venue_symbol.strip()
        if not venue_symbol:
            raise ValueError("venue_symbol must not be empty")

        instrument_id = self.instrument_id
        if instrument_id is None:
            if instrument_type is InstrumentType.SPOT:
                instrument_id = build_exchange_spot_instrument_id(venue_code, venue_symbol)
            else:
                instrument_id = f"{venue_code.value}:{instrument_type.value}:{venue_symbol}"

        object.__setattr__(self, "venue_id", venue_code)
        object.__setattr__(self, "asset_id", asset_code)
        object.__setattr__(self, "instrument_type", instrument_type)
        object.__setattr__(self, "venue_symbol", venue_symbol)
        object.__setattr__(self, "instrument_id", str(instrument_id))
        object.__setattr__(self, "status", status)
        if self.quote_ccy is not None:
            object.__setattr__(self, "quote_ccy", self.quote_ccy.upper())


@dataclass(slots=True, frozen=True)
class Market:
    """Canonical execution market."""

    venue_id: VenueCode | str
    asset_id: AssetCode | str
    market_id: str
    title: str
    market_type: MarketType | str = MarketType.BINARY_UPDOWN_5M
    status: StatusValue | str = StatusValue.ACTIVE

    def __post_init__(self) -> None:
        venue_code = _coerce_venue_code(self.venue_id)
        asset_code = _coerce_asset_code(self.asset_id)
        market_type = (
            self.market_type
            if isinstance(self.market_type, MarketType)
            else MarketType(self.market_type)
        )
        status = self.status if isinstance(self.status, StatusValue) else StatusValue(self.status)

        if venue_code is VenueCode.POLYMARKET:
            market_id = str(validate_polymarket_market_id(self.market_id))
        else:
            market_id = self.market_id.strip()
            if not market_id:
                raise ValueError("market_id must not be empty")

        title = self.title.strip()
        if not title:
            raise ValueError("title must not be empty")

        object.__setattr__(self, "venue_id", venue_code)
        object.__setattr__(self, "asset_id", asset_code)
        object.__setattr__(self, "market_id", market_id)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "market_type", market_type)
        object.__setattr__(self, "status", status)


@dataclass(slots=True, frozen=True)
class OracleFeed:
    """Canonical settlement-relevant oracle feed."""

    asset_id: AssetCode | str
    venue_id: VenueCode | str = VenueCode.CHAINLINK
    feed_name: str = "BTC/USD"
    feed_type: InstrumentType | str = InstrumentType.STREAM
    oracle_feed_id: str | None = None
    status: StatusValue | str = StatusValue.ACTIVE

    def __post_init__(self) -> None:
        asset_code = _coerce_asset_code(self.asset_id)
        venue_code = _coerce_venue_code(self.venue_id)
        feed_type = (
            self.feed_type
            if isinstance(self.feed_type, InstrumentType)
            else InstrumentType(self.feed_type)
        )
        status = self.status if isinstance(self.status, StatusValue) else StatusValue(self.status)

        feed_name = self.feed_name.strip().upper().replace("-", "/")
        if "/" not in feed_name:
            raise ValueError("feed_name must be formatted like BTC/USD")
        _, quote_symbol = feed_name.split("/", 1)

        oracle_feed_id = (
            build_oracle_feed_id(
                asset_code,
                quote_symbol=quote_symbol,
                venue=venue_code,
                feed_type=feed_type,
            )
            if self.oracle_feed_id is None
            else validate_oracle_feed_id(self.oracle_feed_id)
        )

        object.__setattr__(self, "asset_id", asset_code)
        object.__setattr__(self, "venue_id", venue_code)
        object.__setattr__(self, "feed_name", feed_name)
        object.__setattr__(self, "feed_type", feed_type)
        object.__setattr__(self, "oracle_feed_id", str(oracle_feed_id))
        object.__setattr__(self, "status", status)


@dataclass(slots=True, frozen=True)
class Window:
    """Canonical 5-minute resolution window."""

    asset_id: AssetCode | str
    window_start_ts: UTCDateTime
    window_type: WindowType | str = WindowType.UPDOWN_5M
    window_id: str | None = None
    window_end_ts: UTCDateTime | None = None
    duration_seconds: int = 300

    def __post_init__(self) -> None:
        asset_code = _coerce_asset_code(self.asset_id)
        window_type = (
            self.window_type
            if isinstance(self.window_type, WindowType)
            else WindowType(self.window_type)
        )
        start_ts = ensure_utc(self.window_start_ts, field_name="window_start_ts")
        if not is_5m_boundary(start_ts):
            raise ValueError("window_start_ts must be aligned to an exact 5-minute boundary")

        end_ts = window_end(start_ts) if self.window_end_ts is None else ensure_utc(
            self.window_end_ts,
            field_name="window_end_ts",
        )
        if end_ts != window_end(start_ts):
            raise ValueError("window_end_ts must equal window_start_ts + 300 seconds")
        if self.duration_seconds != 300:
            raise ValueError("duration_seconds must equal 300 in phase 1")

        built_window_id = build_window_id(asset_code, start_ts)
        if self.window_id is not None and validate_window_id(self.window_id) != built_window_id:
            raise ValueError("window_id does not match asset_id and window_start_ts")

        object.__setattr__(self, "asset_id", asset_code)
        object.__setattr__(self, "window_type", window_type)
        object.__setattr__(self, "window_start_ts", start_ts)
        object.__setattr__(self, "window_end_ts", end_ts)
        object.__setattr__(self, "window_id", str(built_window_id))


@dataclass(slots=True, frozen=True)
class SnapshotRef:
    """Canonical identity block for a replay snapshot row."""

    window_id: str
    market_id: str
    snapshot_ts: UTCDateTime
    schema_version: str = SCHEMA_VERSION
    feature_version: str = SCHEMA_VERSION
    snapshot_origin: SnapshotOrigin | str = SnapshotOrigin.FIXED_1S
    snapshot_id: str | None = None

    def __post_init__(self) -> None:
        window_id = str(validate_window_id(self.window_id))
        market_id = str(validate_polymarket_market_id(self.market_id))
        snapshot_ts = ensure_utc(self.snapshot_ts, field_name="snapshot_ts")
        snapshot_origin = (
            self.snapshot_origin
            if isinstance(self.snapshot_origin, SnapshotOrigin)
            else SnapshotOrigin(self.snapshot_origin)
        )

        built_snapshot_id = build_snapshot_id(window_id, market_id, snapshot_ts)
        if self.snapshot_id is not None and self.snapshot_id != built_snapshot_id:
            raise ValueError("snapshot_id does not match window_id, market_id, and snapshot_ts")

        object.__setattr__(self, "window_id", window_id)
        object.__setattr__(self, "market_id", market_id)
        object.__setattr__(self, "snapshot_ts", snapshot_ts)
        object.__setattr__(self, "snapshot_origin", snapshot_origin)
        object.__setattr__(self, "snapshot_id", str(built_snapshot_id))


__all__ = [
    "Asset",
    "Instrument",
    "Market",
    "OracleFeed",
    "SCHEMA_VERSION",
    "SnapshotRef",
    "Venue",
    "Window",
]
