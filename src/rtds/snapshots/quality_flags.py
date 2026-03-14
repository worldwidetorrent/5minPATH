"""Snapshot quality flags."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from rtds.core.time import ensure_utc
from rtds.features.composite_nowcast import CompositeNowcast
from rtds.schemas.normalized import PolymarketQuote
from rtds.schemas.quality import (
    ChainlinkQualityState,
    CompositeDispersionState,
    SourceFreshnessState,
)
from rtds.schemas.window_reference import WindowReferenceRecord


@dataclass(slots=True, frozen=True)
class SnapshotQualityFlags:
    """Snapshot-level trust flags derived from already-normalized inputs."""

    as_of_ts: datetime
    exchange_quality_usable_flag: bool
    chainlink_quality_usable_flag: bool
    polymarket_quote_usable_flag: bool
    reference_complete_flag: bool
    market_active_flag: bool
    snapshot_usable_flag: bool
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_ts", ensure_utc(self.as_of_ts, field_name="as_of_ts"))
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


def derive_snapshot_quality_flags(
    *,
    window_reference: WindowReferenceRecord,
    exchange_quality: CompositeDispersionState,
    composite_nowcast: CompositeNowcast,
    chainlink_quality: ChainlinkQualityState,
    polymarket_quote_freshness: SourceFreshnessState,
    polymarket_quote: PolymarketQuote | None,
) -> SnapshotQualityFlags:
    """Derive snapshot trust flags from reference, market state, and quality state."""

    diagnostics: list[str] = []
    market_active_flag = bool(window_reference.market_active_flag) and not bool(
        window_reference.market_closed_flag
    )
    if not market_active_flag:
        diagnostics.append("inactive_market")

    reference_complete_flag = (
        window_reference.mapping_status == "mapped"
        and window_reference.polymarket_market_id is not None
        and window_reference.chainlink_open_anchor_price is not None
        and window_reference.assignment_status != "ambiguous"
    )
    if not reference_complete_flag:
        diagnostics.append("reference_incomplete")

    exchange_quality_usable_flag = (
        exchange_quality.usable_flag
        and not composite_nowcast.composite_missing_flag
        and composite_nowcast.composite_now_price is not None
    )
    if not exchange_quality_usable_flag:
        diagnostics.append("exchange_composite_unusable")

    chainlink_quality_usable_flag = chainlink_quality.usable_flag
    if not chainlink_quality_usable_flag:
        diagnostics.append("chainlink_unusable")

    polymarket_quote_usable_flag = (
        polymarket_quote is not None
        and not polymarket_quote_freshness.stale_flag
        and not polymarket_quote_freshness.missing_flag
        and polymarket_quote.quote_completeness_flag
        and not polymarket_quote.crossed_market_flag
        and polymarket_quote.normalization_status == "normalized"
    )
    if not polymarket_quote_usable_flag:
        if polymarket_quote_freshness.missing_flag or polymarket_quote is None:
            diagnostics.append("polymarket_quote_missing")
        elif polymarket_quote_freshness.stale_flag:
            diagnostics.append("polymarket_quote_stale")
        else:
            diagnostics.append("polymarket_quote_unusable")

    diagnostics.extend(exchange_quality.diagnostics)
    diagnostics.extend(chainlink_quality.diagnostics)
    diagnostics.extend(polymarket_quote_freshness.diagnostics)
    diagnostics.extend(composite_nowcast.diagnostics)

    return SnapshotQualityFlags(
        as_of_ts=composite_nowcast.as_of_ts,
        exchange_quality_usable_flag=exchange_quality_usable_flag,
        chainlink_quality_usable_flag=chainlink_quality_usable_flag,
        polymarket_quote_usable_flag=polymarket_quote_usable_flag,
        reference_complete_flag=reference_complete_flag,
        market_active_flag=market_active_flag,
        snapshot_usable_flag=(
            market_active_flag
            and reference_complete_flag
            and exchange_quality_usable_flag
            and chainlink_quality_usable_flag
            and polymarket_quote_usable_flag
        ),
        diagnostics=tuple(diagnostics),
    )


__all__ = [
    "SnapshotQualityFlags",
    "derive_snapshot_quality_flags",
]
