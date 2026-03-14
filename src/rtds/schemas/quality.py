"""Quality state schemas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal

SCHEMA_VERSION = "0.1.0"


def _freeze_mapping(mapping: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType(dict(mapping))


@dataclass(slots=True, frozen=True)
class SourceFreshnessState:
    """Trust state for one source at one snapshot timestamp."""

    source_id: str
    as_of_ts: datetime
    last_event_ts: datetime | None
    last_event_age_ms: int | None
    stale_flag: bool
    missing_flag: bool
    usable_flag: bool
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        object.__setattr__(self, "as_of_ts", ensure_utc(self.as_of_ts, field_name="as_of_ts"))
        if self.last_event_ts is not None:
            object.__setattr__(
                self,
                "last_event_ts",
                ensure_utc(self.last_event_ts, field_name="last_event_ts"),
            )
        if self.last_event_age_ms is not None and self.last_event_age_ms < 0:
            raise ValueError("last_event_age_ms must be non-negative")
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


@dataclass(slots=True, frozen=True)
class CompositeDispersionState:
    """Cross-venue exchange quote trust state at one snapshot timestamp."""

    as_of_ts: datetime
    contributing_venue_count: int
    trusted_venue_count: int
    contributing_venue_ids: tuple[str, ...]
    trusted_venue_ids: tuple[str, ...]
    per_venue_age_ms: Mapping[str, int | None]
    per_venue_mid_price: Mapping[str, Decimal | None]
    dispersion_abs_usd: Decimal | None
    dispersion_bps: Decimal | None
    outlier_venue_ids: tuple[str, ...]
    insufficient_venues_flag: bool
    usable_flag: bool
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_ts", ensure_utc(self.as_of_ts, field_name="as_of_ts"))
        if self.contributing_venue_count < 0:
            raise ValueError("contributing_venue_count must be non-negative")
        if self.trusted_venue_count < 0:
            raise ValueError("trusted_venue_count must be non-negative")
        if self.trusted_venue_count > self.contributing_venue_count:
            raise ValueError("trusted_venue_count must be <= contributing_venue_count")
        object.__setattr__(
            self,
            "contributing_venue_ids",
            tuple(self.contributing_venue_ids),
        )
        object.__setattr__(
            self,
            "trusted_venue_ids",
            tuple(self.trusted_venue_ids),
        )
        object.__setattr__(
            self,
            "outlier_venue_ids",
            tuple(sorted(set(self.outlier_venue_ids))),
        )
        object.__setattr__(
            self,
            "per_venue_age_ms",
            _freeze_mapping(dict(self.per_venue_age_ms)),
        )
        object.__setattr__(
            self,
            "per_venue_mid_price",
            _freeze_mapping(
                {
                    venue_id: (
                        None
                        if mid_price is None
                        else to_decimal(mid_price, field_name=f"per_venue_mid_price[{venue_id}]")
                    )
                    for venue_id, mid_price in self.per_venue_mid_price.items()
                }
            ),
        )
        if self.dispersion_abs_usd is not None:
            object.__setattr__(
                self,
                "dispersion_abs_usd",
                to_decimal(self.dispersion_abs_usd, field_name="dispersion_abs_usd"),
            )
        if self.dispersion_bps is not None:
            object.__setattr__(
                self,
                "dispersion_bps",
                to_decimal(self.dispersion_bps, field_name="dispersion_bps"),
            )
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


@dataclass(slots=True, frozen=True)
class ChainlinkQualityState:
    """Chainlink/RTDS liveness and gap state at one snapshot timestamp."""

    as_of_ts: datetime
    last_event_ts: datetime | None
    current_age_ms: int | None
    stale_flag: bool
    missing_flag: bool
    silence_flag: bool
    gap_flag: bool
    last_inter_tick_gap_ms: int | None
    max_observed_gap_ms: int | None
    usable_flag: bool
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_ts", ensure_utc(self.as_of_ts, field_name="as_of_ts"))
        if self.last_event_ts is not None:
            object.__setattr__(
                self,
                "last_event_ts",
                ensure_utc(self.last_event_ts, field_name="last_event_ts"),
            )
        for field_name in ("current_age_ms", "last_inter_tick_gap_ms", "max_observed_gap_ms"):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


__all__ = [
    "SCHEMA_VERSION",
    "ChainlinkQualityState",
    "CompositeDispersionState",
    "SourceFreshnessState",
]
