"""Offline reconciliation for shadow decisions versus replay expectations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal
from rtds.execution.enums import OrderState, Side
from rtds.execution.models import (
    OUTCOME_STATUS_RESOLVED,
    OUTCOME_STATUS_UNRESOLVED,
    ShadowDecision,
    ShadowOutcome,
    ShadowVsReplaySummary,
)
from rtds.execution.summary import build_shadow_vs_replay_summary


@dataclass(slots=True, frozen=True)
class ReplayExpectation:
    """External replay expectation keyed to one shadow decision."""

    decision_id: str
    replay_expected_pnl: Decimal
    replay_expected_roi: Decimal | None = None

    def __post_init__(self) -> None:
        if not str(self.decision_id).strip():
            raise ValueError("decision_id must be non-empty")
        object.__setattr__(
            self,
            "replay_expected_pnl",
            to_decimal(self.replay_expected_pnl, field_name="replay_expected_pnl"),
        )
        if self.replay_expected_roi is not None:
            object.__setattr__(
                self,
                "replay_expected_roi",
                to_decimal(self.replay_expected_roi, field_name="replay_expected_roi"),
            )


@dataclass(slots=True, frozen=True)
class WindowResolution:
    """Resolved market outcome for one execution window."""

    window_id: str
    outcome_ts: datetime
    outcome_status: str
    resolved_up: bool | None

    def __post_init__(self) -> None:
        if not str(self.window_id).strip():
            raise ValueError("window_id must be non-empty")
        object.__setattr__(
            self,
            "outcome_ts",
            ensure_utc(self.outcome_ts, field_name="outcome_ts"),
        )
        normalized_status = str(self.outcome_status).strip().lower()
        if normalized_status not in {OUTCOME_STATUS_RESOLVED, OUTCOME_STATUS_UNRESOLVED}:
            raise ValueError(f"unsupported outcome_status: {self.outcome_status}")
        object.__setattr__(self, "outcome_status", normalized_status)


@dataclass(slots=True, frozen=True)
class ReconciliationResult:
    """Wave-two shadow reconciliation outputs."""

    outcomes: tuple[ShadowOutcome, ...]
    comparison_summary: ShadowVsReplaySummary


def reconcile_shadow_decisions(
    *,
    ledger,
    replay_expectations: Iterable[ReplayExpectation] = (),
    window_resolutions: Iterable[WindowResolution] = (),
) -> ReconciliationResult:
    """Reconcile shadow decisions against replay expectations and resolved outcomes."""

    expectations_by_id = {
        expectation.decision_id: expectation for expectation in replay_expectations
    }
    resolutions_by_window = {
        resolution.window_id: resolution for resolution in window_resolutions
    }
    existing_outcome_ids = {outcome.decision.decision_id for outcome in ledger.outcomes}
    outcomes: list[ShadowOutcome] = []
    for decision in ledger.decisions:
        resolution = resolutions_by_window.get(decision.executable_state.window_id)
        replay_expectation = expectations_by_id.get(decision.decision_id)
        outcome = _build_shadow_outcome(
            decision=decision,
            resolution=resolution,
            replay_expectation=replay_expectation,
        )
        outcomes.append(outcome)
        if decision.decision_id not in existing_outcome_ids:
            ledger.record_outcome(outcome)
            existing_outcome_ids.add(decision.decision_id)
    return ReconciliationResult(
        outcomes=tuple(outcomes),
        comparison_summary=build_shadow_vs_replay_summary(ledger=ledger, outcomes=outcomes),
    )


def _build_shadow_outcome(
    *,
    decision: ShadowDecision,
    resolution: WindowResolution | None,
    replay_expectation: ReplayExpectation | None,
) -> ShadowOutcome:
    if resolution is None:
        outcome_ts = decision.decision_ts
        outcome_status = OUTCOME_STATUS_UNRESOLVED
        resolved_up = None
    else:
        outcome_ts = resolution.outcome_ts
        outcome_status = resolution.outcome_status
        resolved_up = resolution.resolved_up
    shadow_realized_pnl = _shadow_realized_pnl(
        decision=decision,
        outcome_status=outcome_status,
        resolved_up=resolved_up,
    )
    shadow_realized_roi = _shadow_realized_roi(
        decision=decision,
        shadow_realized_pnl=shadow_realized_pnl,
    )
    replay_expected_pnl = (
        None if replay_expectation is None else replay_expectation.replay_expected_pnl
    )
    replay_expected_roi = (
        None if replay_expectation is None else replay_expectation.replay_expected_roi
    )
    divergence = (
        None
        if replay_expected_pnl is None or shadow_realized_pnl is None
        else shadow_realized_pnl - replay_expected_pnl
    )
    return ShadowOutcome(
        decision=decision,
        order_state=(
            OrderState.ELIGIBLE_RECORDED
            if decision.tradability_check.is_actionable
            else OrderState.NO_TRADE_RECORDED
        ),
        outcome_ts=outcome_ts,
        outcome_status=outcome_status,
        resolved_up=resolved_up,
        replay_expected_pnl=replay_expected_pnl,
        replay_expected_roi=replay_expected_roi,
        shadow_realized_pnl=shadow_realized_pnl,
        shadow_realized_roi=shadow_realized_roi,
        pnl_divergence_vs_replay=divergence,
    )


def _shadow_realized_pnl(
    *,
    decision: ShadowDecision,
    outcome_status: str,
    resolved_up: bool | None,
) -> Decimal | None:
    if not decision.tradability_check.is_actionable:
        return Decimal("0")
    if outcome_status != OUTCOME_STATUS_RESOLVED:
        return None
    if decision.intended_side is None or decision.tradability_check.intended_entry_price is None:
        return None
    exit_price = _exit_price(decision.intended_side, resolved_up=resolved_up)
    size = decision.tradability_check.target_size_contracts
    return (exit_price - decision.tradability_check.intended_entry_price) * size


def _shadow_realized_roi(
    *,
    decision: ShadowDecision,
    shadow_realized_pnl: Decimal | None,
) -> Decimal | None:
    if shadow_realized_pnl is None:
        return None
    if not decision.tradability_check.is_actionable:
        return Decimal("0")
    if decision.tradability_check.intended_entry_price is None:
        return None
    invested = (
        decision.tradability_check.intended_entry_price
        * decision.tradability_check.target_size_contracts
    )
    if invested == 0:
        return Decimal("0")
    return shadow_realized_pnl / invested


def _exit_price(side: Side, *, resolved_up: bool | None) -> Decimal:
    normalized_side = Side(side)
    if normalized_side == Side.UP:
        return Decimal("1") if resolved_up is True else Decimal("0")
    return Decimal("1") if resolved_up is False else Decimal("0")


__all__ = [
    "ReconciliationResult",
    "ReplayExpectation",
    "WindowResolution",
    "reconcile_shadow_decisions",
]
