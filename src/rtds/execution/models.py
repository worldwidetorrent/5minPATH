"""Frozen execution-v0 sidecar models.

`decision_ts` is fixed to the `snapshot_ts` of the triggering executable-state row.
It is not evaluation completion time and not artifact write time.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from typing import Any

from rtds.core.time import ensure_utc, format_utc, format_utc_compact
from rtds.core.units import to_decimal, validate_contract_price, validate_size
from rtds.execution.enums import NoTradeReason, OrderState, PolicyMode, Side
from rtds.execution.version import SCHEMA_VERSION

STATE_SOURCE_LIVE = "live_state"
STATE_SOURCE_REPLAY = "replay_tail"

BOOK_SIDE_BID = "bid"
BOOK_SIDE_ASK = "ask"

CALIBRATION_SUPPORT_SUFFICIENT = "sufficient"
CALIBRATION_SUPPORT_THIN = "thin"
CALIBRATION_SUPPORT_MERGE_REQUIRED = "merge_required"
OUTCOME_STATUS_RESOLVED = "resolved"
OUTCOME_STATUS_UNRESOLVED = "unresolved"


def _validate_state_source_kind(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {STATE_SOURCE_LIVE, STATE_SOURCE_REPLAY}:
        raise ValueError(f"unsupported state_source_kind: {value}")
    return normalized


def _validate_book_side(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {BOOK_SIDE_BID, BOOK_SIDE_ASK}:
        raise ValueError(f"unsupported book side: {value}")
    return normalized


def _validate_support_flag(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized not in {
        CALIBRATION_SUPPORT_SUFFICIENT,
        CALIBRATION_SUPPORT_THIN,
        CALIBRATION_SUPPORT_MERGE_REQUIRED,
    }:
        raise ValueError(f"unsupported calibration_support_flag: {value}")
    return normalized


def _validate_outcome_status(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {OUTCOME_STATUS_RESOLVED, OUTCOME_STATUS_UNRESOLVED}:
        raise ValueError(f"unsupported outcome_status: {value}")
    return normalized


def _validate_transition_name(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        raise ValueError("transition_name must be non-empty when provided")
    return normalized


def _serialize_fingerprint_value(value: object) -> Any:
    if isinstance(value, datetime):
        return format_utc(value, timespec="milliseconds")
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (Side, PolicyMode, OrderState, NoTradeReason)):
        return value.value
    if isinstance(value, dict):
        return {key: _serialize_fingerprint_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_serialize_fingerprint_value(item) for item in value]
    return value


@dataclass(slots=True, frozen=True)
class ExecutableStateView:
    """Exact normalized executable-state row used for one shadow decision."""

    session_id: str
    state_source_kind: str
    snapshot_ts: datetime
    window_id: str
    window_start_ts: datetime
    window_end_ts: datetime
    seconds_remaining: int
    polymarket_market_id: str
    polymarket_slug: str | None
    clob_token_id_up: str | None
    clob_token_id_down: str | None
    window_quality_regime: str
    chainlink_confidence_state: str
    volatility_regime: str
    fair_value_base: Decimal | None
    calibrated_fair_value_base: Decimal | None
    calibration_bucket: str | None
    calibration_support_flag: str | None
    quote_source: str
    quote_event_ts: datetime | None
    quote_recv_ts: datetime | None
    quote_age_ms: int | None
    up_bid_price: Decimal | None
    up_ask_price: Decimal | None
    down_bid_price: Decimal | None
    down_ask_price: Decimal | None
    up_bid_size_contracts: Decimal | None
    up_ask_size_contracts: Decimal | None
    down_bid_size_contracts: Decimal | None
    down_ask_size_contracts: Decimal | None
    up_spread_abs: Decimal | None = None
    down_spread_abs: Decimal | None = None
    market_actionable_flag: bool = True
    state_fingerprint: str | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "state_source_kind",
            _validate_state_source_kind(self.state_source_kind),
        )
        for field_name in ("snapshot_ts", "window_start_ts", "window_end_ts"):
            object.__setattr__(
                self,
                field_name,
                ensure_utc(getattr(self, field_name), field_name=field_name),
            )
        for field_name in ("quote_event_ts", "quote_recv_ts"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    ensure_utc(value, field_name=field_name),
                )
        if self.seconds_remaining < 0:
            raise ValueError("seconds_remaining must be non-negative")
        if self.quote_age_ms is not None and self.quote_age_ms < 0:
            raise ValueError("quote_age_ms must be non-negative")
        for field_name in (
            "fair_value_base",
            "calibrated_fair_value_base",
            "up_bid_price",
            "up_ask_price",
            "down_bid_price",
            "down_ask_price",
            "up_spread_abs",
            "down_spread_abs",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    validate_contract_price(value, field_name=field_name),
                )
        for field_name in (
            "up_bid_size_contracts",
            "up_ask_size_contracts",
            "down_bid_size_contracts",
            "down_ask_size_contracts",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    to_decimal(validate_size(value, field_name=field_name), field_name=field_name),
                )
        object.__setattr__(
            self,
            "calibration_support_flag",
            _validate_support_flag(self.calibration_support_flag),
        )
        expected_fingerprint = build_state_fingerprint(self)
        if self.state_fingerprint is not None and self.state_fingerprint != expected_fingerprint:
            raise ValueError("state_fingerprint does not match executable state contents")
        object.__setattr__(self, "state_fingerprint", expected_fingerprint)

    def price_for(self, *, side: Side, book_side: str) -> Decimal | None:
        normalized_book_side = _validate_book_side(book_side)
        return getattr(self, f"{side.value}_{normalized_book_side}_price")

    def size_for(self, *, side: Side, book_side: str) -> Decimal | None:
        normalized_book_side = _validate_book_side(book_side)
        return getattr(self, f"{side.value}_{normalized_book_side}_size_contracts")

    def spread_for(self, *, side: Side) -> Decimal | None:
        return getattr(self, f"{side.value}_spread_abs")


@dataclass(slots=True, frozen=True)
class TradabilityCheck:
    """Immediate tradability result for one intended taker decision."""

    policy_mode: PolicyMode
    intended_side: Side | None
    intended_book_side: str
    intended_entry_price: Decimal | None
    displayed_entry_size_contracts: Decimal | None
    target_size_contracts: Decimal
    selected_net_edge: Decimal | None
    selected_spread_abs: Decimal | None
    quote_age_ms: int | None
    is_actionable: bool
    no_trade_reason: NoTradeReason | None
    book_side_present: bool = True
    freshness_passed: bool = True
    size_coverage_passed: bool = True
    spread_passed: bool = True
    edge_threshold_passed: bool = True
    policy_check_passed: bool = True
    market_actionable_passed: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        object.__setattr__(self, "intended_book_side", _validate_book_side(self.intended_book_side))
        if self.intended_side is not None:
            object.__setattr__(self, "intended_side", Side(self.intended_side))
        if self.intended_entry_price is not None:
            object.__setattr__(
                self,
                "intended_entry_price",
                validate_contract_price(
                    self.intended_entry_price,
                    field_name="intended_entry_price",
                ),
            )
        if self.selected_net_edge is not None:
            object.__setattr__(
                self,
                "selected_net_edge",
                to_decimal(self.selected_net_edge, field_name="selected_net_edge"),
            )
        if self.selected_spread_abs is not None:
            object.__setattr__(
                self,
                "selected_spread_abs",
                validate_contract_price(
                    self.selected_spread_abs,
                    field_name="selected_spread_abs",
                ),
            )
        if self.displayed_entry_size_contracts is not None:
            object.__setattr__(
                self,
                "displayed_entry_size_contracts",
                to_decimal(
                    validate_size(
                        self.displayed_entry_size_contracts,
                        field_name="displayed_entry_size_contracts",
                    ),
                    field_name="displayed_entry_size_contracts",
                ),
            )
        object.__setattr__(
            self,
            "target_size_contracts",
            to_decimal(
                validate_size(self.target_size_contracts, field_name="target_size_contracts"),
                field_name="target_size_contracts",
            ),
        )
        if self.quote_age_ms is not None and self.quote_age_ms < 0:
            raise ValueError("quote_age_ms must be non-negative")
        if self.no_trade_reason is not None:
            object.__setattr__(self, "no_trade_reason", NoTradeReason(self.no_trade_reason))
        if self.is_actionable and self.no_trade_reason is not None:
            raise ValueError("actionable tradability checks cannot carry no_trade_reason")
        if not self.is_actionable and self.no_trade_reason is None:
            raise ValueError("non-actionable tradability checks require no_trade_reason")
        if self.is_actionable and not all(
            (
                self.book_side_present,
                self.freshness_passed,
                self.size_coverage_passed,
                self.spread_passed,
                self.edge_threshold_passed,
                self.policy_check_passed,
                self.market_actionable_passed,
            )
        ):
            raise ValueError("actionable tradability checks require all component checks to pass")


@dataclass(slots=True, frozen=True)
class ShadowDecision:
    """One append-only shadow decision.

    `decision_ts` is fixed to `executable_state.snapshot_ts`.
    """

    executable_state: ExecutableStateView
    policy_mode: PolicyMode
    tradability_check: TradabilityCheck
    decision_ts: datetime
    intended_side: Side | None
    decision_id: str | None = None
    state_fingerprint: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        object.__setattr__(
            self,
            "decision_ts",
            ensure_utc(self.decision_ts, field_name="decision_ts"),
        )
        if self.intended_side is not None:
            object.__setattr__(self, "intended_side", Side(self.intended_side))
        if self.decision_ts != self.executable_state.snapshot_ts:
            raise ValueError("decision_ts must equal executable_state.snapshot_ts")
        expected_fingerprint = self.executable_state.state_fingerprint
        if self.state_fingerprint is not None and self.state_fingerprint != expected_fingerprint:
            raise ValueError("decision state_fingerprint does not match executable_state")
        object.__setattr__(self, "state_fingerprint", expected_fingerprint)
        expected_decision_id = build_decision_id(
            session_id=self.executable_state.session_id,
            window_id=self.executable_state.window_id,
            decision_ts=self.decision_ts,
            side=self.intended_side,
            policy_mode=self.policy_mode,
        )
        if self.decision_id is not None and self.decision_id != expected_decision_id:
            raise ValueError("decision_id does not match deterministic execution-v0 contract")
        object.__setattr__(self, "decision_id", expected_decision_id)


@dataclass(slots=True, frozen=True)
class ShadowOrderState:
    """Append-only shadow record derived from one decision."""

    decision: ShadowDecision
    order_state: OrderState
    updated_ts: datetime
    transition_name: str | None = None
    transition_index: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_state", OrderState(self.order_state))
        object.__setattr__(
            self,
            "updated_ts",
            ensure_utc(self.updated_ts, field_name="updated_ts"),
        )
        object.__setattr__(
            self,
            "transition_name",
            _validate_transition_name(self.transition_name),
        )
        if self.transition_index < 0:
            raise ValueError("transition_index must be non-negative")


@dataclass(slots=True, frozen=True)
class ShadowOutcome:
    """Outcome-level execution-gap record derived from one shadow decision."""

    decision: ShadowDecision
    order_state: OrderState
    outcome_ts: datetime
    outcome_status: str
    resolved_up: bool | None
    replay_expected_pnl: Decimal | None
    replay_expected_roi: Decimal | None
    shadow_realized_pnl: Decimal | None
    shadow_realized_roi: Decimal | None
    pnl_divergence_vs_replay: Decimal | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_state", OrderState(self.order_state))
        object.__setattr__(
            self,
            "outcome_ts",
            ensure_utc(self.outcome_ts, field_name="outcome_ts"),
        )
        object.__setattr__(
            self,
            "outcome_status",
            _validate_outcome_status(self.outcome_status),
        )
        for field_name in (
            "replay_expected_pnl",
            "replay_expected_roi",
            "shadow_realized_pnl",
            "shadow_realized_roi",
            "pnl_divergence_vs_replay",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    to_decimal(value, field_name=field_name),
                )
        if (
            self.replay_expected_pnl is not None
            and self.shadow_realized_pnl is not None
            and self.pnl_divergence_vs_replay is not None
        ):
            expected_delta = self.shadow_realized_pnl - self.replay_expected_pnl
            if self.pnl_divergence_vs_replay != expected_delta:
                raise ValueError(
                    "pnl_divergence_vs_replay must equal "
                    "shadow_realized_pnl - replay_expected_pnl"
                )


@dataclass(slots=True, frozen=True)
class ShadowSummary:
    """Aggregate shadow-sidecar summary for one session and policy mode."""

    session_id: str
    policy_mode: PolicyMode
    decision_count: int
    actionable_decision_count: int
    no_trade_count: int
    order_state_counts: dict[str, int]
    no_trade_reason_counts: dict[str, int]
    written_decision_count: int = 0
    order_state_transition_count: int = 0
    reject_rate_by_reason: dict[str, Decimal] | None = None
    tradability_pass_rate: Decimal | None = None
    freshness_pass_rate: Decimal | None = None
    size_coverage_pass_rate: Decimal | None = None
    spread_pass_rate: Decimal | None = None
    replay_expected_pnl: Decimal | None = None
    shadow_realized_pnl: Decimal | None = None
    pnl_divergence_vs_replay: Decimal | None = None
    first_decision_ts: datetime | None = None
    last_decision_ts: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        if (
            self.decision_count < 0
            or self.actionable_decision_count < 0
            or self.no_trade_count < 0
            or self.written_decision_count < 0
            or self.order_state_transition_count < 0
        ):
            raise ValueError("summary counts must be non-negative")
        if self.first_decision_ts is not None:
            object.__setattr__(
                self,
                "first_decision_ts",
                ensure_utc(self.first_decision_ts, field_name="first_decision_ts"),
            )
        if self.last_decision_ts is not None:
            object.__setattr__(
                self,
                "last_decision_ts",
                ensure_utc(self.last_decision_ts, field_name="last_decision_ts"),
            )
        object.__setattr__(
            self,
            "order_state_counts",
            dict(sorted(self.order_state_counts.items())),
        )
        object.__setattr__(
            self,
            "no_trade_reason_counts",
            dict(sorted(self.no_trade_reason_counts.items())),
        )
        object.__setattr__(
            self,
            "reject_rate_by_reason",
            dict(sorted((self.reject_rate_by_reason or {}).items())),
        )
        for field_name in (
            "tradability_pass_rate",
            "freshness_pass_rate",
            "size_coverage_pass_rate",
            "spread_pass_rate",
            "replay_expected_pnl",
            "shadow_realized_pnl",
            "pnl_divergence_vs_replay",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    to_decimal(value, field_name=field_name),
                )


@dataclass(slots=True, frozen=True)
class ShadowVsReplaySummary:
    """Structured execution-gap summary for one reconciled shadow session."""

    session_id: str
    policy_mode: PolicyMode
    decision_count: int
    actionable_decision_count: int
    reconciled_decision_count: int
    replay_expected_pnl: Decimal
    shadow_realized_pnl: Decimal
    pnl_divergence_vs_replay: Decimal
    reject_rate_by_reason: dict[str, Decimal]
    tradability_pass_rate: Decimal | None = None
    freshness_pass_rate: Decimal | None = None
    size_coverage_pass_rate: Decimal | None = None
    spread_pass_rate: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        if (
            self.decision_count < 0
            or self.actionable_decision_count < 0
            or self.reconciled_decision_count < 0
        ):
            raise ValueError("comparison counts must be non-negative")
        for field_name in (
            "replay_expected_pnl",
            "shadow_realized_pnl",
            "pnl_divergence_vs_replay",
            "tradability_pass_rate",
            "freshness_pass_rate",
            "size_coverage_pass_rate",
            "spread_pass_rate",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, to_decimal(value, field_name=field_name))
        object.__setattr__(
            self,
            "reject_rate_by_reason",
            dict(sorted(self.reject_rate_by_reason.items())),
        )


def build_state_fingerprint(executable_state: ExecutableStateView) -> str:
    """Build a deterministic fingerprint for the exact executable-state row used."""

    payload = {
        field.name: _serialize_fingerprint_value(getattr(executable_state, field.name))
        for field in fields(executable_state)
        if field.name != "state_fingerprint"
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def build_decision_id(
    *,
    session_id: str,
    window_id: str,
    decision_ts: datetime,
    side: Side | None,
    policy_mode: PolicyMode,
) -> str:
    """Build the deterministic execution-v0 decision identifier."""

    side_token = "none" if side is None else Side(side).value
    return (
        "shadowdec:"
        f"{session_id}:"
        f"{window_id}:"
        f"{format_utc_compact(ensure_utc(decision_ts), include_millis=True)}:"
        f"{side_token}:"
        f"{PolicyMode(policy_mode).value}"
    )


__all__ = [
    "BOOK_SIDE_ASK",
    "BOOK_SIDE_BID",
    "CALIBRATION_SUPPORT_MERGE_REQUIRED",
    "CALIBRATION_SUPPORT_SUFFICIENT",
    "CALIBRATION_SUPPORT_THIN",
    "ExecutableStateView",
    "OUTCOME_STATUS_RESOLVED",
    "OUTCOME_STATUS_UNRESOLVED",
    "STATE_SOURCE_LIVE",
    "STATE_SOURCE_REPLAY",
    "ShadowDecision",
    "ShadowOutcome",
    "ShadowOrderState",
    "ShadowSummary",
    "ShadowVsReplaySummary",
    "TradabilityCheck",
    "build_decision_id",
    "build_state_fingerprint",
]
