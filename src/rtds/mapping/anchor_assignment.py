"""Deterministic Chainlink boundary assignment for mapped 5-minute windows.

Phase-1 policy is explicit and testable:

- search ticks in a +/-10s band around the target boundary
- prefer an exact boundary tick
- otherwise choose the first tick after the boundary
- otherwise choose the last tick before the boundary
- do not interpolate

Confidence is based on absolute offset from the boundary:

- `high` for exact ticks or offsets <= 1000 ms
- `medium` for offsets <= 3000 ms
- `low` for offsets <= 10000 ms
- `none` for missing or ambiguous assignments

If there is no tick within +/-3000 ms of the boundary, the assignment carries a
`boundary_silence_gap` diagnostic even if it still resolves inside the wider
search band.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from rtds.core.enums import AssetCode, ConfidenceLevel
from rtds.core.ids import build_oracle_feed_id
from rtds.core.time import ensure_utc
from rtds.core.units import validate_usd_price
from rtds.mapping.market_mapper import WindowMarketMappingRecord
from rtds.schemas.window_reference import (
    SCHEMA_VERSION as WINDOW_REFERENCE_SCHEMA_VERSION,
)
from rtds.schemas.window_reference import WindowReferenceRecord

SCHEMA_VERSION = WINDOW_REFERENCE_SCHEMA_VERSION
ANCHOR_ASSIGNMENT_VERSION = "0.1.0"
DEFAULT_ORACLE_FEED_ID = str(build_oracle_feed_id(AssetCode.BTC))

EXACT_BOUNDARY_METHOD = "exact_boundary"
FIRST_AFTER_BOUNDARY_METHOD = "first_after_boundary"
LAST_BEFORE_BOUNDARY_METHOD = "last_before_boundary"
MISSING_METHOD = "missing"

ASSIGNED_STATUS = "assigned"
MISSING_STATUS = "missing"
AMBIGUOUS_STATUS = "ambiguous"

HIGH_CONFIDENCE_MAX_OFFSET_MS = 1_000
MEDIUM_CONFIDENCE_MAX_OFFSET_MS = 3_000
LOW_CONFIDENCE_MAX_OFFSET_MS = 10_000

DEFAULT_OPEN_TOLERANCE_MS = LOW_CONFIDENCE_MAX_OFFSET_MS
DEFAULT_SETTLE_TOLERANCE_MS = LOW_CONFIDENCE_MAX_OFFSET_MS
DEFAULT_BOUNDARY_SILENCE_GAP_MS = MEDIUM_CONFIDENCE_MAX_OFFSET_MS


@dataclass(slots=True, frozen=True)
class ChainlinkTick:
    """Normalized Chainlink/RTDS tick used for boundary assignment."""

    event_id: str
    event_ts: datetime
    price: Decimal
    recv_ts: datetime | None = None
    oracle_feed_id: str = DEFAULT_ORACLE_FEED_ID
    round_id: str | None = None

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise ValueError("event_id must not be empty")
        object.__setattr__(
            self,
            "event_ts",
            ensure_utc(self.event_ts, field_name="event_ts"),
        )
        object.__setattr__(
            self,
            "price",
            Decimal(validate_usd_price(self.price, field_name="price")),
        )
        if self.recv_ts is not None:
            object.__setattr__(
                self,
                "recv_ts",
                ensure_utc(self.recv_ts, field_name="recv_ts"),
            )


@dataclass(slots=True, frozen=True)
class BoundaryAssignmentPolicy:
    """Exact numeric policy for one boundary assignment pass."""

    tolerance_ms: int = LOW_CONFIDENCE_MAX_OFFSET_MS
    silence_gap_ms: int = DEFAULT_BOUNDARY_SILENCE_GAP_MS
    high_confidence_max_offset_ms: int = HIGH_CONFIDENCE_MAX_OFFSET_MS
    medium_confidence_max_offset_ms: int = MEDIUM_CONFIDENCE_MAX_OFFSET_MS
    low_confidence_max_offset_ms: int = LOW_CONFIDENCE_MAX_OFFSET_MS

    def __post_init__(self) -> None:
        if self.tolerance_ms <= 0:
            raise ValueError("tolerance_ms must be positive")
        if self.silence_gap_ms < 0:
            raise ValueError("silence_gap_ms must be non-negative")
        if self.high_confidence_max_offset_ms < 0:
            raise ValueError("high_confidence_max_offset_ms must be non-negative")
        if self.high_confidence_max_offset_ms > self.medium_confidence_max_offset_ms:
            raise ValueError("high confidence cutoff must be <= medium cutoff")
        if self.medium_confidence_max_offset_ms > self.low_confidence_max_offset_ms:
            raise ValueError("medium confidence cutoff must be <= low cutoff")
        if self.low_confidence_max_offset_ms > self.tolerance_ms:
            raise ValueError("low confidence cutoff must be <= tolerance_ms")


@dataclass(slots=True, frozen=True)
class BoundaryAssignment:
    """Resolved boundary assignment plus diagnostics."""

    price: Decimal | None
    ts: datetime | None
    event_id: str | None
    round_id: str | None
    method: str
    confidence: str
    status: str
    offset_ms: int | None
    diagnostics: tuple[str, ...]


DEFAULT_OPEN_POLICY = BoundaryAssignmentPolicy(
    tolerance_ms=DEFAULT_OPEN_TOLERANCE_MS,
)
DEFAULT_SETTLE_POLICY = BoundaryAssignmentPolicy(
    tolerance_ms=DEFAULT_SETTLE_TOLERANCE_MS,
)


def assign_window_reference(
    mapping_record: WindowMarketMappingRecord,
    ticks: Iterable[ChainlinkTick],
    *,
    open_policy: BoundaryAssignmentPolicy = DEFAULT_OPEN_POLICY,
    settle_policy: BoundaryAssignmentPolicy = DEFAULT_SETTLE_POLICY,
    oracle_feed_id: str = DEFAULT_ORACLE_FEED_ID,
) -> WindowReferenceRecord:
    """Assign Chainlink open and settle fields for one accepted mapping row."""

    normalized_ticks = _normalize_ticks(ticks)
    return _assign_window_reference_from_sorted_ticks(
        mapping_record,
        normalized_ticks=normalized_ticks,
        open_policy=open_policy,
        settle_policy=settle_policy,
        oracle_feed_id=oracle_feed_id,
    )


def assign_window_references(
    mapping_records: Iterable[WindowMarketMappingRecord],
    ticks: Iterable[ChainlinkTick],
    *,
    open_policy: BoundaryAssignmentPolicy = DEFAULT_OPEN_POLICY,
    settle_policy: BoundaryAssignmentPolicy = DEFAULT_SETTLE_POLICY,
    oracle_feed_id: str = DEFAULT_ORACLE_FEED_ID,
) -> list[WindowReferenceRecord]:
    """Assign Chainlink fields for a batch of accepted mapping rows."""

    normalized_ticks = _normalize_ticks(ticks)
    return [
        _assign_window_reference_from_sorted_ticks(
            record,
            normalized_ticks=normalized_ticks,
            open_policy=open_policy,
            settle_policy=settle_policy,
            oracle_feed_id=oracle_feed_id,
        )
        for record in mapping_records
    ]


def assign_open_anchor(
    boundary_ts: datetime,
    ticks: Iterable[ChainlinkTick],
    *,
    policy: BoundaryAssignmentPolicy = DEFAULT_OPEN_POLICY,
) -> BoundaryAssignment:
    """Assign the canonical open anchor at a window start boundary."""

    return _assign_boundary(boundary_ts, _normalize_ticks(ticks), policy=policy)


def assign_settlement(
    boundary_ts: datetime,
    ticks: Iterable[ChainlinkTick],
    *,
    policy: BoundaryAssignmentPolicy = DEFAULT_SETTLE_POLICY,
) -> BoundaryAssignment:
    """Assign the canonical settlement tick at a window end boundary."""

    return _assign_boundary(boundary_ts, _normalize_ticks(ticks), policy=policy)


def _assign_window_reference_from_sorted_ticks(
    mapping_record: WindowMarketMappingRecord,
    *,
    normalized_ticks: list[ChainlinkTick],
    open_policy: BoundaryAssignmentPolicy,
    settle_policy: BoundaryAssignmentPolicy,
    oracle_feed_id: str,
) -> WindowReferenceRecord:
    if mapping_record.mapping_status != "mapped":
        raise ValueError("anchor assignment requires a mapping record with mapping_status='mapped'")

    open_assignment = _assign_boundary(
        mapping_record.window_start_ts,
        normalized_ticks,
        policy=open_policy,
    )
    settle_assignment = _assign_boundary(
        mapping_record.window_end_ts,
        normalized_ticks,
        policy=settle_policy,
    )

    diagnostics = tuple(
        sorted(set(open_assignment.diagnostics + settle_assignment.diagnostics))
    )
    outcome_status = _derive_outcome_status(open_assignment, settle_assignment)
    assignment_status = _derive_assignment_status(open_assignment, settle_assignment)
    settle_minus_open = _compute_settle_minus_open(open_assignment, settle_assignment)
    resolved_up = _resolve_up(open_assignment, settle_assignment)

    return WindowReferenceRecord(
        window_id=mapping_record.window_id,
        asset_id=mapping_record.asset_id,
        window_start_ts=mapping_record.window_start_ts,
        window_end_ts=mapping_record.window_end_ts,
        oracle_feed_id=oracle_feed_id,
        polymarket_market_id=mapping_record.polymarket_market_id,
        polymarket_event_id=mapping_record.polymarket_event_id,
        polymarket_slug=mapping_record.polymarket_slug,
        clob_token_id_up=mapping_record.clob_token_id_up,
        clob_token_id_down=mapping_record.clob_token_id_down,
        listing_discovered_ts=mapping_record.listing_discovered_ts,
        market_active_flag=mapping_record.market_active_flag,
        market_closed_flag=mapping_record.market_closed_flag,
        mapping_status=mapping_record.mapping_status,
        mapping_confidence=mapping_record.mapping_confidence,
        mapping_method=mapping_record.mapping_method,
        chainlink_open_anchor_price=open_assignment.price,
        chainlink_open_anchor_ts=open_assignment.ts,
        chainlink_open_anchor_event_id=open_assignment.event_id,
        chainlink_open_anchor_method=open_assignment.method,
        chainlink_open_anchor_confidence=open_assignment.confidence,
        chainlink_open_anchor_status=open_assignment.status,
        chainlink_open_anchor_offset_ms=open_assignment.offset_ms,
        chainlink_settle_price=settle_assignment.price,
        chainlink_settle_ts=settle_assignment.ts,
        chainlink_settle_event_id=settle_assignment.event_id,
        chainlink_settle_method=settle_assignment.method,
        chainlink_settle_confidence=settle_assignment.confidence,
        chainlink_settle_status=settle_assignment.status,
        chainlink_settle_offset_ms=settle_assignment.offset_ms,
        resolved_up=resolved_up,
        settle_minus_open=settle_minus_open,
        outcome_status=outcome_status,
        assignment_status=assignment_status,
        assignment_diagnostics=diagnostics,
        notes=mapping_record.notes,
        schema_version=SCHEMA_VERSION,
        normalizer_version=mapping_record.normalizer_version,
        mapping_version=mapping_record.mapping_version,
        anchor_assignment_version=ANCHOR_ASSIGNMENT_VERSION,
        created_ts=mapping_record.created_ts,
        updated_ts=mapping_record.updated_ts,
    )


def _assign_boundary(
    boundary_ts: datetime,
    ticks: Iterable[ChainlinkTick],
    *,
    policy: BoundaryAssignmentPolicy,
) -> BoundaryAssignment:
    normalized_boundary_ts = ensure_utc(boundary_ts, field_name="boundary_ts")
    normalized_ticks = list(ticks) if isinstance(ticks, list) else _normalize_ticks(ticks)
    diagnostics: list[str] = []

    if not any(
        abs(_offset_ms(normalized_boundary_ts, tick.event_ts)) <= policy.silence_gap_ms
        for tick in normalized_ticks
    ):
        diagnostics.append("boundary_silence_gap")

    exact_ticks = [
        tick for tick in normalized_ticks if tick.event_ts == normalized_boundary_ts
    ]
    if exact_ticks:
        return _build_assignment(
            exact_ticks,
            boundary_ts=normalized_boundary_ts,
            method=EXACT_BOUNDARY_METHOD,
            diagnostics=diagnostics,
            policy=policy,
        )

    after_ticks = [
        tick
        for tick in normalized_ticks
        if 0 < _offset_ms(normalized_boundary_ts, tick.event_ts) <= policy.tolerance_ms
    ]
    if after_ticks:
        earliest_ts = min(tick.event_ts for tick in after_ticks)
        return _build_assignment(
            [tick for tick in after_ticks if tick.event_ts == earliest_ts],
            boundary_ts=normalized_boundary_ts,
            method=FIRST_AFTER_BOUNDARY_METHOD,
            diagnostics=diagnostics,
            policy=policy,
        )

    before_ticks = [
        tick
        for tick in normalized_ticks
        if 0 > _offset_ms(normalized_boundary_ts, tick.event_ts) >= -policy.tolerance_ms
    ]
    if before_ticks:
        latest_ts = max(tick.event_ts for tick in before_ticks)
        return _build_assignment(
            [tick for tick in before_ticks if tick.event_ts == latest_ts],
            boundary_ts=normalized_boundary_ts,
            method=LAST_BEFORE_BOUNDARY_METHOD,
            diagnostics=diagnostics,
            policy=policy,
        )

    return BoundaryAssignment(
        price=None,
        ts=None,
        event_id=None,
        round_id=None,
        method=MISSING_METHOD,
        confidence=ConfidenceLevel.NONE.value,
        status=MISSING_STATUS,
        offset_ms=None,
        diagnostics=tuple(diagnostics + ["no_tick_within_tolerance"]),
    )


def _build_assignment(
    candidate_ticks: list[ChainlinkTick],
    *,
    boundary_ts: datetime,
    method: str,
    diagnostics: list[str],
    policy: BoundaryAssignmentPolicy,
) -> BoundaryAssignment:
    chosen_ts = candidate_ticks[0].event_ts
    offset_ms = _offset_ms(boundary_ts, chosen_ts)
    candidate_prices = {tick.price for tick in candidate_ticks}

    if len(candidate_prices) > 1:
        return BoundaryAssignment(
            price=None,
            ts=None,
            event_id=None,
            round_id=None,
            method=method,
            confidence=ConfidenceLevel.NONE.value,
            status=AMBIGUOUS_STATUS,
            offset_ms=offset_ms,
            diagnostics=tuple(diagnostics + ["conflicting_ticks_at_selected_ts"]),
        )

    chosen_tick = min(
        candidate_ticks,
        key=lambda tick: (tick.event_id, tick.round_id or ""),
    )
    assignment_diagnostics = list(diagnostics)
    if len(candidate_ticks) > 1:
        assignment_diagnostics.append("duplicate_ticks_same_price")

    return BoundaryAssignment(
        price=chosen_tick.price,
        ts=chosen_tick.event_ts,
        event_id=chosen_tick.event_id,
        round_id=chosen_tick.round_id,
        method=method,
        confidence=_confidence_for_offset(abs(offset_ms), policy=policy),
        status=ASSIGNED_STATUS,
        offset_ms=offset_ms,
        diagnostics=tuple(assignment_diagnostics),
    )


def _normalize_ticks(ticks: Iterable[ChainlinkTick]) -> list[ChainlinkTick]:
    return sorted(
        list(ticks),
        key=lambda tick: (tick.event_ts, tick.event_id, tick.round_id or ""),
    )


def _offset_ms(boundary_ts: datetime, event_ts: datetime) -> int:
    delta = ensure_utc(event_ts, field_name="event_ts") - ensure_utc(
        boundary_ts,
        field_name="boundary_ts",
    )
    return int(delta.total_seconds() * 1000)


def _confidence_for_offset(offset_ms: int, *, policy: BoundaryAssignmentPolicy) -> str:
    if offset_ms <= policy.high_confidence_max_offset_ms:
        return ConfidenceLevel.HIGH.value
    if offset_ms <= policy.medium_confidence_max_offset_ms:
        return ConfidenceLevel.MEDIUM.value
    if offset_ms <= policy.low_confidence_max_offset_ms:
        return ConfidenceLevel.LOW.value
    return ConfidenceLevel.NONE.value


def _derive_outcome_status(
    open_assignment: BoundaryAssignment,
    settle_assignment: BoundaryAssignment,
) -> str:
    if open_assignment.status == AMBIGUOUS_STATUS or settle_assignment.status == AMBIGUOUS_STATUS:
        return "ambiguous"
    if open_assignment.price is None:
        return "missing_anchor"
    if settle_assignment.price is None:
        return "missing_settle"
    return "resolved"


def _derive_assignment_status(
    open_assignment: BoundaryAssignment,
    settle_assignment: BoundaryAssignment,
) -> str:
    if open_assignment.status == AMBIGUOUS_STATUS or settle_assignment.status == AMBIGUOUS_STATUS:
        return "ambiguous"
    if open_assignment.status == ASSIGNED_STATUS and settle_assignment.status == ASSIGNED_STATUS:
        return "complete"
    if open_assignment.status == MISSING_STATUS and settle_assignment.status == MISSING_STATUS:
        return "open_and_settle_missing"
    if open_assignment.status == MISSING_STATUS:
        return "open_missing"
    if settle_assignment.status == MISSING_STATUS:
        return "settle_missing"
    return "incomplete"


def _compute_settle_minus_open(
    open_assignment: BoundaryAssignment,
    settle_assignment: BoundaryAssignment,
) -> Decimal | None:
    if open_assignment.price is None or settle_assignment.price is None:
        return None
    return settle_assignment.price - open_assignment.price


def _resolve_up(
    open_assignment: BoundaryAssignment,
    settle_assignment: BoundaryAssignment,
) -> bool | None:
    if open_assignment.price is None or settle_assignment.price is None:
        return None
    return settle_assignment.price > open_assignment.price


__all__ = [
    "AMBIGUOUS_STATUS",
    "ANCHOR_ASSIGNMENT_VERSION",
    "ASSIGNED_STATUS",
    "BoundaryAssignment",
    "BoundaryAssignmentPolicy",
    "ChainlinkTick",
    "DEFAULT_BOUNDARY_SILENCE_GAP_MS",
    "DEFAULT_OPEN_POLICY",
    "DEFAULT_OPEN_TOLERANCE_MS",
    "DEFAULT_ORACLE_FEED_ID",
    "DEFAULT_SETTLE_POLICY",
    "DEFAULT_SETTLE_TOLERANCE_MS",
    "EXACT_BOUNDARY_METHOD",
    "FIRST_AFTER_BOUNDARY_METHOD",
    "HIGH_CONFIDENCE_MAX_OFFSET_MS",
    "LAST_BEFORE_BOUNDARY_METHOD",
    "LOW_CONFIDENCE_MAX_OFFSET_MS",
    "MEDIUM_CONFIDENCE_MAX_OFFSET_MS",
    "MISSING_METHOD",
    "MISSING_STATUS",
    "SCHEMA_VERSION",
    "WindowReferenceRecord",
    "assign_open_anchor",
    "assign_settlement",
    "assign_window_reference",
    "assign_window_references",
]
