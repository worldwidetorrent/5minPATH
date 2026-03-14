"""Replay snapshot schemas."""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping

from rtds.core.enums import SnapshotOrigin
from rtds.core.ids import build_snapshot_id, validate_snapshot_id
from rtds.core.time import ensure_utc, format_utc, parse_utc
from rtds.core.units import to_decimal

SCHEMA_VERSION = "0.1.0"


@dataclass(slots=True, frozen=True)
class SnapshotRecord:
    """Persisted replay snapshot row for one mapped window at one timestamp."""

    snapshot_ts: datetime
    window_id: str
    asset_id: str
    polymarket_market_id: str
    snapshot_origin: str
    window_start_ts: datetime
    window_end_ts: datetime
    polymarket_event_id: str | None
    polymarket_slug: str | None
    mapping_status: str
    assignment_status: str
    market_active_flag: bool | None
    market_closed_flag: bool | None
    oracle_feed_id: str
    chainlink_open_anchor_price: Decimal | None
    chainlink_open_anchor_ts: datetime | None
    chainlink_settle_price: Decimal | None
    chainlink_settle_ts: datetime | None
    chainlink_current_price: Decimal | None
    chainlink_current_ts: datetime | None
    chainlink_current_age_ms: int | None
    composite_now_price: Decimal | None
    composite_method: str
    composite_quality_score: Decimal
    composite_missing_flag: bool
    composite_contributing_venue_count: int
    composite_contributing_venues: tuple[str, ...]
    composite_per_venue_mids: Mapping[str, Decimal | None]
    composite_per_venue_ages: Mapping[str, int | None]
    composite_dispersion_abs_usd: Decimal | None
    composite_dispersion_bps: Decimal | None
    polymarket_quote_event_ts: datetime | None
    polymarket_quote_recv_ts: datetime | None
    polymarket_quote_age_ms: int | None
    up_bid: Decimal | None
    up_ask: Decimal | None
    down_bid: Decimal | None
    down_ask: Decimal | None
    up_bid_size_contracts: Decimal | None
    up_ask_size_contracts: Decimal | None
    down_bid_size_contracts: Decimal | None
    down_ask_size_contracts: Decimal | None
    market_mid_up: Decimal | None
    market_mid_down: Decimal | None
    market_spread_up_abs: Decimal | None
    market_spread_down_abs: Decimal | None
    last_trade_price: Decimal | None
    last_trade_size_contracts: Decimal | None
    exchange_quality_usable_flag: bool
    chainlink_quality_usable_flag: bool
    polymarket_quote_usable_flag: bool
    reference_complete_flag: bool
    snapshot_usable_flag: bool
    quality_diagnostics: tuple[str, ...]
    schema_version: str
    feature_version: str
    created_ts: datetime
    snapshot_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "snapshot_ts",
            ensure_utc(self.snapshot_ts, field_name="snapshot_ts"),
        )
        object.__setattr__(
            self,
            "window_start_ts",
            ensure_utc(self.window_start_ts, field_name="window_start_ts"),
        )
        object.__setattr__(
            self,
            "window_end_ts",
            ensure_utc(self.window_end_ts, field_name="window_end_ts"),
        )
        object.__setattr__(self, "created_ts", ensure_utc(self.created_ts, field_name="created_ts"))
        object.__setattr__(
            self,
            "snapshot_origin",
            (
                self.snapshot_origin
                if isinstance(self.snapshot_origin, str)
                else SnapshotOrigin(self.snapshot_origin).value
            ),
        )
        SnapshotOrigin(self.snapshot_origin)
        built_snapshot_id = str(
            build_snapshot_id(self.window_id, self.polymarket_market_id, self.snapshot_ts)
        )
        if (
            self.snapshot_id is not None
            and str(validate_snapshot_id(self.snapshot_id)) != built_snapshot_id
        ):
            raise ValueError("snapshot_id does not match window_id, market_id, and snapshot_ts")
        object.__setattr__(self, "snapshot_id", built_snapshot_id)

        for field_name in (
            "chainlink_open_anchor_ts",
            "chainlink_settle_ts",
            "chainlink_current_ts",
            "polymarket_quote_event_ts",
            "polymarket_quote_recv_ts",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    ensure_utc(value, field_name=field_name),
                )

        for field_name in (
            "chainlink_open_anchor_price",
            "chainlink_settle_price",
            "chainlink_current_price",
            "composite_now_price",
            "composite_quality_score",
            "composite_dispersion_abs_usd",
            "composite_dispersion_bps",
            "up_bid",
            "up_ask",
            "down_bid",
            "down_ask",
            "up_bid_size_contracts",
            "up_ask_size_contracts",
            "down_bid_size_contracts",
            "down_ask_size_contracts",
            "market_mid_up",
            "market_mid_down",
            "market_spread_up_abs",
            "market_spread_down_abs",
            "last_trade_price",
            "last_trade_size_contracts",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, to_decimal(value, field_name=field_name))

        object.__setattr__(
            self,
            "composite_contributing_venues",
            tuple(self.composite_contributing_venues),
        )
        object.__setattr__(
            self,
            "composite_per_venue_mids",
            MappingProxyType(
                {
                    venue_id: (
                        None
                        if mid_price is None
                        else to_decimal(
                            mid_price,
                            field_name=f"composite_per_venue_mids[{venue_id}]",
                        )
                    )
                    for venue_id, mid_price in self.composite_per_venue_mids.items()
                }
            ),
        )
        object.__setattr__(
            self,
            "composite_per_venue_ages",
            MappingProxyType(dict(self.composite_per_venue_ages)),
        )
        object.__setattr__(
            self,
            "quality_diagnostics",
            tuple(sorted(set(self.quality_diagnostics))),
        )

    def to_dict(self) -> dict[str, object]:
        """Materialize the record as native Python values."""

        row: dict[str, object] = {}
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(value, MappingProxyType):
                row[field.name] = dict(value)
            elif isinstance(value, tuple):
                row[field.name] = tuple(value)
            else:
                row[field.name] = value
        return row

    def to_storage_dict(self) -> dict[str, Any]:
        """Materialize the row with JSON-friendly values."""

        return {key: _serialize_value(value) for key, value in self.to_dict().items()}

    @classmethod
    def from_storage_dict(cls, row: dict[str, Any]) -> "SnapshotRecord":
        """Parse a storage row back into a typed snapshot record."""

        cleaned = dict(row)
        datetime_fields = {
            "snapshot_ts",
            "window_start_ts",
            "window_end_ts",
            "chainlink_open_anchor_ts",
            "chainlink_settle_ts",
            "chainlink_current_ts",
            "polymarket_quote_event_ts",
            "polymarket_quote_recv_ts",
            "created_ts",
        }
        decimal_fields = {
            "chainlink_open_anchor_price",
            "chainlink_settle_price",
            "chainlink_current_price",
            "composite_now_price",
            "composite_quality_score",
            "composite_dispersion_abs_usd",
            "composite_dispersion_bps",
            "up_bid",
            "up_ask",
            "down_bid",
            "down_ask",
            "up_bid_size_contracts",
            "up_ask_size_contracts",
            "down_bid_size_contracts",
            "down_ask_size_contracts",
            "market_mid_up",
            "market_mid_down",
            "market_spread_up_abs",
            "market_spread_down_abs",
            "last_trade_price",
            "last_trade_size_contracts",
        }
        tuple_fields = {"composite_contributing_venues", "quality_diagnostics"}
        mapping_fields = {"composite_per_venue_mids", "composite_per_venue_ages"}

        for field_name in datetime_fields:
            value = cleaned.get(field_name)
            if value is not None:
                cleaned[field_name] = parse_utc(str(value))

        for field_name in decimal_fields:
            value = cleaned.get(field_name)
            if value is not None:
                cleaned[field_name] = Decimal(str(value))

        for field_name in tuple_fields:
            value = cleaned.get(field_name, ())
            cleaned[field_name] = tuple(value or ())

        for field_name in mapping_fields:
            cleaned[field_name] = dict(cleaned.get(field_name, {}))

        return cls(**cleaned)


def _serialize_value(value: object) -> Any:
    if isinstance(value, datetime):
        return format_utc(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    return value


__all__ = [
    "SCHEMA_VERSION",
    "SnapshotRecord",
]
