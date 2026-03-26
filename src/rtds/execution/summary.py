"""Structured shadow summary metrics for execution-v0 evidence."""

from __future__ import annotations

from decimal import Decimal

from rtds.execution.models import ShadowOutcome, ShadowSummary, ShadowVsReplaySummary


def build_shadow_summary(ledger) -> ShadowSummary:
    """Build the structured shadow summary from a ledger snapshot."""

    decisions = list(ledger.decisions)
    outcomes = list(ledger.outcomes)
    decision_count = len(decisions)
    actionable_count = sum(int(decision.tradability_check.is_actionable) for decision in decisions)
    no_trade_count = decision_count - actionable_count
    order_state_counts = _count_by_attr(ledger.order_states, attr="order_state")
    no_trade_reason_counts = _count_no_trade_reasons(decisions)
    reject_rate_by_reason = {
        reason: _rate(count, decision_count)
        for reason, count in no_trade_reason_counts.items()
    }
    first_ts = None if not decisions else min(decision.decision_ts for decision in decisions)
    last_ts = None if not decisions else max(decision.decision_ts for decision in decisions)
    replay_expected_total, shadow_realized_total, divergence_total = _outcome_totals(outcomes)
    return ShadowSummary(
        session_id=ledger.session_id,
        policy_mode=ledger.policy_mode,
        decision_count=decision_count,
        actionable_decision_count=actionable_count,
        no_trade_count=no_trade_count,
        written_decision_count=ledger.written_decision_count,
        order_state_transition_count=len(ledger.order_states),
        order_state_counts=order_state_counts,
        no_trade_reason_counts=no_trade_reason_counts,
        reject_rate_by_reason=reject_rate_by_reason,
        tradability_pass_rate=_rate(actionable_count, decision_count),
        freshness_pass_rate=_rate(
            sum(int(decision.tradability_check.freshness_passed) for decision in decisions),
            decision_count,
        ),
        size_coverage_pass_rate=_rate(
            sum(int(decision.tradability_check.size_coverage_passed) for decision in decisions),
            decision_count,
        ),
        spread_pass_rate=_rate(
            sum(int(decision.tradability_check.spread_passed) for decision in decisions),
            decision_count,
        ),
        replay_expected_pnl=replay_expected_total,
        shadow_realized_pnl=shadow_realized_total,
        pnl_divergence_vs_replay=divergence_total,
        first_decision_ts=first_ts,
        last_decision_ts=last_ts,
    )


def build_shadow_vs_replay_summary(
    *,
    ledger,
    outcomes: tuple[ShadowOutcome, ...] | list[ShadowOutcome] | None = None,
) -> ShadowVsReplaySummary:
    """Build a structured execution-gap comparison from decisions plus outcomes."""

    decisions = list(ledger.decisions)
    resolved_outcomes = list(ledger.outcomes if outcomes is None else outcomes)
    no_trade_reason_counts = _count_no_trade_reasons(decisions)
    return ShadowVsReplaySummary(
        session_id=ledger.session_id,
        policy_mode=ledger.policy_mode,
        decision_count=len(decisions),
        actionable_decision_count=sum(
            int(decision.tradability_check.is_actionable) for decision in decisions
        ),
        reconciled_decision_count=len(resolved_outcomes),
        replay_expected_pnl=sum(
            (outcome.replay_expected_pnl or Decimal("0")) for outcome in resolved_outcomes
        ),
        shadow_realized_pnl=sum(
            (outcome.shadow_realized_pnl or Decimal("0")) for outcome in resolved_outcomes
        ),
        pnl_divergence_vs_replay=sum(
            (outcome.pnl_divergence_vs_replay or Decimal("0")) for outcome in resolved_outcomes
        ),
        reject_rate_by_reason={
            reason: _rate(count, len(decisions))
            for reason, count in no_trade_reason_counts.items()
        },
        tradability_pass_rate=_rate(
            sum(int(decision.tradability_check.is_actionable) for decision in decisions),
            len(decisions),
        ),
        freshness_pass_rate=_rate(
            sum(int(decision.tradability_check.freshness_passed) for decision in decisions),
            len(decisions),
        ),
        size_coverage_pass_rate=_rate(
            sum(int(decision.tradability_check.size_coverage_passed) for decision in decisions),
            len(decisions),
        ),
        spread_pass_rate=_rate(
            sum(int(decision.tradability_check.spread_passed) for decision in decisions),
            len(decisions),
        ),
    )


def _count_by_attr(rows, *, attr: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = getattr(row, attr)
        if hasattr(key, "value"):
            key = key.value
        else:
            key = str(key)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _count_no_trade_reasons(decisions) -> dict[str, int]:
    counts: dict[str, int] = {}
    for decision in decisions:
        reason = decision.tradability_check.no_trade_reason
        if reason is None:
            continue
        counts[reason.value] = counts.get(reason.value, 0) + 1
    return counts


def _rate(numerator: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return Decimal(numerator) / Decimal(denominator)


def _outcome_totals(
    outcomes: list[ShadowOutcome],
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if not outcomes:
        return None, None, None
    replay_expected_total = sum(
        (outcome.replay_expected_pnl or Decimal("0")) for outcome in outcomes
    )
    shadow_realized_total = sum(
        (outcome.shadow_realized_pnl or Decimal("0")) for outcome in outcomes
    )
    divergence_total = sum(
        (outcome.pnl_divergence_vs_replay or Decimal("0")) for outcome in outcomes
    )
    return replay_expected_total, shadow_realized_total, divergence_total


__all__ = [
    "build_shadow_summary",
    "build_shadow_vs_replay_summary",
]
