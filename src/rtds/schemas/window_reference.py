"""Canonical persisted window-reference rows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from rtds.core.time import ensure_utc, format_utc, parse_utc

SCHEMA_VERSION = "0.2.0"


@dataclass(slots=True, frozen=True)
class WindowReferenceRecord:
    """Persisted row joining canonical windows, venue mapping, and oracle assignment."""

    window_id: str
    asset_id: str
    window_start_ts: datetime
    window_end_ts: datetime
    oracle_feed_id: str
    polymarket_market_id: str | None
    polymarket_event_id: str | None
    polymarket_slug: str | None
    clob_token_id_up: str | None
    clob_token_id_down: str | None
    listing_discovered_ts: datetime | None
    market_active_flag: bool | None
    market_closed_flag: bool | None
    mapping_status: str
    mapping_confidence: str
    mapping_method: str
    chainlink_open_anchor_price: Decimal | None
    chainlink_open_anchor_ts: datetime | None
    chainlink_open_anchor_event_id: str | None
    chainlink_open_anchor_source: str | None
    chainlink_open_anchor_method: str
    chainlink_open_anchor_confidence: str
    chainlink_open_anchor_status: str
    chainlink_open_anchor_offset_ms: int | None
    chainlink_settle_price: Decimal | None
    chainlink_settle_ts: datetime | None
    chainlink_settle_event_id: str | None
    chainlink_settle_source: str | None
    chainlink_settle_method: str
    chainlink_settle_confidence: str
    chainlink_settle_status: str
    chainlink_settle_offset_ms: int | None
    resolved_up: bool | None
    settle_minus_open: Decimal | None
    outcome_status: str
    assignment_status: str
    assignment_diagnostics: tuple[str, ...]
    notes: str | None
    schema_version: str
    normalizer_version: str
    mapping_version: str
    anchor_assignment_version: str
    created_ts: datetime
    updated_ts: datetime

    def __post_init__(self) -> None:
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
        object.__setattr__(self, "assignment_diagnostics", tuple(self.assignment_diagnostics))

        for field_name in (
            "listing_discovered_ts",
            "chainlink_open_anchor_ts",
            "chainlink_settle_ts",
            "created_ts",
            "updated_ts",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    ensure_utc(value, field_name=field_name),
                )

    @property
    def date_utc(self) -> date:
        """UTC partition date derived from the canonical window start."""

        return self.window_start_ts.date()

    def to_dict(self) -> dict[str, object]:
        """Materialize the record as native Python values."""

        return asdict(self)

    def to_storage_dict(self) -> dict[str, Any]:
        """Materialize the row with JSON-friendly values and explicit partition date."""

        row = self.to_dict()
        row["date_utc"] = self.date_utc.isoformat()
        return {key: _serialize_value(value) for key, value in row.items()}

    @classmethod
    def from_storage_dict(cls, row: dict[str, Any]) -> "WindowReferenceRecord":
        """Parse a storage row back into a typed window-reference record."""

        cleaned = dict(row)
        cleaned.pop("date_utc", None)

        datetime_fields = {
            "window_start_ts",
            "window_end_ts",
            "listing_discovered_ts",
            "chainlink_open_anchor_ts",
            "chainlink_settle_ts",
            "created_ts",
            "updated_ts",
        }
        decimal_fields = {
            "chainlink_open_anchor_price",
            "chainlink_settle_price",
            "settle_minus_open",
        }
        tuple_fields = {"assignment_diagnostics"}

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
            if value is None:
                cleaned[field_name] = ()
            else:
                cleaned[field_name] = tuple(str(item) for item in value)

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
    "WindowReferenceRecord",
]
