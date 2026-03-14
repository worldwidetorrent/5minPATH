"""Replay snapshot builder."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from rtds.core.enums import SnapshotOrigin
from rtds.core.time import ensure_utc, utc_now
from rtds.features.composite_nowcast import FEATURE_VERSION, CompositeNowcast
from rtds.mapping.anchor_assignment import ChainlinkTick
from rtds.schemas.normalized import PolymarketQuote
from rtds.schemas.quality import (
    ChainlinkQualityState,
    CompositeDispersionState,
    SourceFreshnessState,
)
from rtds.schemas.snapshot import SCHEMA_VERSION, SnapshotRecord
from rtds.schemas.window_reference import WindowReferenceRecord
from rtds.snapshots.quality_flags import derive_snapshot_quality_flags


@dataclass(slots=True, frozen=True)
class SnapshotBuildInput:
    """All precomputed state required to build one replay snapshot row."""

    window_reference: WindowReferenceRecord
    snapshot_ts: datetime
    chainlink_current_tick: ChainlinkTick | None
    composite_nowcast: CompositeNowcast
    exchange_quality: CompositeDispersionState
    polymarket_quote: PolymarketQuote | None
    polymarket_quote_freshness: SourceFreshnessState
    chainlink_quality: ChainlinkQualityState
    snapshot_origin: SnapshotOrigin | str = SnapshotOrigin.FIXED_1S
    created_ts: datetime | None = None


def build_snapshot_row(build_input: SnapshotBuildInput) -> SnapshotRecord:
    """Assemble one deterministic snapshot row from already-defined state."""

    snapshot_ts = ensure_utc(build_input.snapshot_ts, field_name="snapshot_ts")
    created_ts = (
        utc_now()
        if build_input.created_ts is None
        else ensure_utc(build_input.created_ts, field_name="created_ts")
    )
    snapshot_origin = (
        build_input.snapshot_origin
        if isinstance(build_input.snapshot_origin, SnapshotOrigin)
        else SnapshotOrigin(build_input.snapshot_origin)
    )

    _validate_snapshot_alignment(build_input, snapshot_ts=snapshot_ts)

    window_reference = build_input.window_reference
    if window_reference.polymarket_market_id is None:
        raise ValueError("snapshot building requires a mapped polymarket_market_id")

    quality_flags = derive_snapshot_quality_flags(
        window_reference=window_reference,
        exchange_quality=build_input.exchange_quality,
        composite_nowcast=build_input.composite_nowcast,
        chainlink_quality=build_input.chainlink_quality,
        polymarket_quote_freshness=build_input.polymarket_quote_freshness,
        polymarket_quote=build_input.polymarket_quote,
    )

    chainlink_current_tick = build_input.chainlink_current_tick
    polymarket_quote = build_input.polymarket_quote
    return SnapshotRecord(
        snapshot_ts=snapshot_ts,
        window_id=window_reference.window_id,
        asset_id=window_reference.asset_id,
        polymarket_market_id=window_reference.polymarket_market_id,
        snapshot_origin=snapshot_origin.value,
        window_start_ts=window_reference.window_start_ts,
        window_end_ts=window_reference.window_end_ts,
        polymarket_event_id=window_reference.polymarket_event_id,
        polymarket_slug=window_reference.polymarket_slug,
        mapping_status=window_reference.mapping_status,
        assignment_status=window_reference.assignment_status,
        market_active_flag=window_reference.market_active_flag,
        market_closed_flag=window_reference.market_closed_flag,
        oracle_feed_id=window_reference.oracle_feed_id,
        chainlink_open_anchor_price=window_reference.chainlink_open_anchor_price,
        chainlink_open_anchor_ts=window_reference.chainlink_open_anchor_ts,
        chainlink_settle_price=window_reference.chainlink_settle_price,
        chainlink_settle_ts=window_reference.chainlink_settle_ts,
        chainlink_current_price=(
            None if chainlink_current_tick is None else chainlink_current_tick.price
        ),
        chainlink_current_ts=(
            None if chainlink_current_tick is None else chainlink_current_tick.event_ts
        ),
        chainlink_current_age_ms=build_input.chainlink_quality.current_age_ms,
        composite_now_price=build_input.composite_nowcast.composite_now_price,
        composite_method=build_input.composite_nowcast.composite_method,
        composite_quality_score=build_input.composite_nowcast.quality_score,
        composite_missing_flag=build_input.composite_nowcast.composite_missing_flag,
        composite_contributing_venue_count=build_input.composite_nowcast.contributing_venue_count,
        composite_contributing_venues=build_input.composite_nowcast.contributing_venues,
        composite_per_venue_mids=build_input.composite_nowcast.per_venue_mids,
        composite_per_venue_ages=build_input.composite_nowcast.per_venue_ages,
        composite_dispersion_abs_usd=build_input.composite_nowcast.dispersion_abs_usd,
        composite_dispersion_bps=build_input.composite_nowcast.dispersion_bps,
        polymarket_quote_event_ts=None if polymarket_quote is None else polymarket_quote.event_ts,
        polymarket_quote_recv_ts=None if polymarket_quote is None else polymarket_quote.recv_ts,
        polymarket_quote_age_ms=build_input.polymarket_quote_freshness.last_event_age_ms,
        up_bid=None if polymarket_quote is None else polymarket_quote.up_bid,
        up_ask=None if polymarket_quote is None else polymarket_quote.up_ask,
        down_bid=None if polymarket_quote is None else polymarket_quote.down_bid,
        down_ask=None if polymarket_quote is None else polymarket_quote.down_ask,
        up_bid_size_contracts=(
            None if polymarket_quote is None else polymarket_quote.up_bid_size_contracts
        ),
        up_ask_size_contracts=(
            None if polymarket_quote is None else polymarket_quote.up_ask_size_contracts
        ),
        down_bid_size_contracts=(
            None if polymarket_quote is None else polymarket_quote.down_bid_size_contracts
        ),
        down_ask_size_contracts=(
            None if polymarket_quote is None else polymarket_quote.down_ask_size_contracts
        ),
        market_mid_up=None if polymarket_quote is None else polymarket_quote.market_mid_up,
        market_mid_down=None if polymarket_quote is None else polymarket_quote.market_mid_down,
        market_spread_up_abs=(
            None if polymarket_quote is None else polymarket_quote.market_spread_up_abs
        ),
        market_spread_down_abs=(
            None if polymarket_quote is None else polymarket_quote.market_spread_down_abs
        ),
        last_trade_price=None if polymarket_quote is None else polymarket_quote.last_trade_price,
        last_trade_size_contracts=(
            None if polymarket_quote is None else polymarket_quote.last_trade_size_contracts
        ),
        exchange_quality_usable_flag=quality_flags.exchange_quality_usable_flag,
        chainlink_quality_usable_flag=quality_flags.chainlink_quality_usable_flag,
        polymarket_quote_usable_flag=quality_flags.polymarket_quote_usable_flag,
        reference_complete_flag=quality_flags.reference_complete_flag,
        snapshot_usable_flag=quality_flags.snapshot_usable_flag,
        quality_diagnostics=quality_flags.diagnostics,
        schema_version=SCHEMA_VERSION,
        feature_version=FEATURE_VERSION,
        created_ts=created_ts,
    )


def _validate_snapshot_alignment(
    build_input: SnapshotBuildInput,
    *,
    snapshot_ts: datetime,
) -> None:
    if not (
        build_input.window_reference.window_start_ts
        <= snapshot_ts
        < build_input.window_reference.window_end_ts
    ):
        raise ValueError("snapshot_ts must fall within the canonical window bounds")

    if build_input.composite_nowcast.as_of_ts != snapshot_ts:
        raise ValueError("composite_nowcast.as_of_ts must equal snapshot_ts")
    if build_input.exchange_quality.as_of_ts != snapshot_ts:
        raise ValueError("exchange_quality.as_of_ts must equal snapshot_ts")
    if build_input.polymarket_quote_freshness.as_of_ts != snapshot_ts:
        raise ValueError("polymarket_quote_freshness.as_of_ts must equal snapshot_ts")
    if build_input.chainlink_quality.as_of_ts != snapshot_ts:
        raise ValueError("chainlink_quality.as_of_ts must equal snapshot_ts")


__all__ = [
    "SnapshotBuildInput",
    "build_snapshot_row",
]
