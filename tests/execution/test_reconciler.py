from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from rtds.execution.enums import PolicyMode
from rtds.execution.ledger import ShadowLedger
from rtds.execution.reconciler import (
    ReplayExpectation,
    WindowResolution,
    reconcile_shadow_decisions,
)
from rtds.execution.summary import build_shadow_summary
from tests.execution.support import build_shadow_decision, replace_state


def test_reconciler_builds_outcomes_and_comparison_summary() -> None:
    actionable = build_shadow_decision(actionable=True)
    blocked = build_shadow_decision(
        actionable=False,
        state=replace_state(
            actionable.executable_state,
            snapshot_ts=datetime(2026, 3, 26, 0, 0, 1, tzinfo=UTC),
        ),
    )
    ledger = ShadowLedger(
        session_id=actionable.executable_state.session_id,
        policy_mode=PolicyMode.BASELINE,
    )
    ledger.record_decision_seen(actionable)
    ledger.record_decision_written(actionable)
    ledger.record_decision_seen(blocked)
    ledger.record_decision_written(blocked)

    result = reconcile_shadow_decisions(
        ledger=ledger,
        replay_expectations=[
            ReplayExpectation(
                decision_id=actionable.decision_id,
                replay_expected_pnl=Decimal("0.30"),
                replay_expected_roi=Decimal("0.05"),
            ),
            ReplayExpectation(
                decision_id=blocked.decision_id,
                replay_expected_pnl=Decimal("0.20"),
                replay_expected_roi=Decimal("0.04"),
            ),
        ],
        window_resolutions=[
            WindowResolution(
                window_id=actionable.executable_state.window_id,
                outcome_ts=actionable.decision_ts,
                outcome_status="resolved",
                resolved_up=True,
            )
        ],
    )

    assert len(result.outcomes) == 2
    actionable_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.decision.decision_id == actionable.decision_id
    )
    blocked_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.decision.decision_id == blocked.decision_id
    )
    assert actionable_outcome.shadow_realized_pnl == Decimal("4.40")
    assert actionable_outcome.pnl_divergence_vs_replay == Decimal("4.10")
    assert blocked_outcome.shadow_realized_pnl == Decimal("0")
    assert blocked_outcome.pnl_divergence_vs_replay == Decimal("-0.20")

    comparison = result.comparison_summary
    assert comparison.decision_count == 2
    assert comparison.reconciled_decision_count == 2
    assert comparison.replay_expected_pnl == Decimal("0.50")
    assert comparison.shadow_realized_pnl == Decimal("4.40")
    assert comparison.pnl_divergence_vs_replay == Decimal("3.90")


def test_build_shadow_summary_includes_structured_rates() -> None:
    actionable = build_shadow_decision(actionable=True)
    blocked = build_shadow_decision(
        actionable=False,
        state=replace_state(
            actionable.executable_state,
            snapshot_ts=datetime(2026, 3, 26, 0, 0, 1, tzinfo=UTC),
        ),
    )
    ledger = ShadowLedger(
        session_id=actionable.executable_state.session_id,
        policy_mode=PolicyMode.BASELINE,
    )
    ledger.record_decision_seen(actionable)
    ledger.record_decision_written(actionable)
    ledger.record_decision_seen(blocked)
    ledger.record_decision_written(blocked)

    summary = build_shadow_summary(ledger)

    assert summary.decision_count == 2
    assert summary.written_decision_count == 2
    assert summary.order_state_transition_count == 4
    assert summary.tradability_pass_rate == Decimal("0.5")
    assert summary.freshness_pass_rate == Decimal("1")
    assert summary.size_coverage_pass_rate == Decimal("1")
    assert summary.reject_rate_by_reason == {"edge_below_threshold": Decimal("0.5")}
