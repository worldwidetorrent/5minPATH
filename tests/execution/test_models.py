from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from rtds.execution.enums import NoTradeReason, OrderState, PolicyMode, Side
from rtds.execution.models import (
    BOOK_SIDE_ASK,
    ShadowDecision,
    ShadowOrderState,
    ShadowSummary,
    TradabilityCheck,
    build_decision_id,
)
from tests.execution.support import build_state_view


def test_executable_state_fingerprint_is_deterministic_and_changes_with_contents() -> None:
    state_a = build_state_view()
    state_b = build_state_view()
    state_c = build_state_view(up_ask_price=Decimal("0.57"))

    assert state_a.state_fingerprint == state_b.state_fingerprint
    assert state_a.state_fingerprint != state_c.state_fingerprint


def test_shadow_decision_uses_snapshot_timestamp_and_deterministic_id() -> None:
    state = build_state_view()
    tradability_check = TradabilityCheck(
        policy_mode=PolicyMode.BASELINE,
        intended_side=Side.UP,
        intended_book_side=BOOK_SIDE_ASK,
        intended_entry_price=Decimal("0.56"),
        displayed_entry_size_contracts=Decimal("25"),
        target_size_contracts=Decimal("10"),
        selected_net_edge=Decimal("0.05"),
        selected_spread_abs=Decimal("0.02"),
        quote_age_ms=12,
        is_actionable=True,
        no_trade_reason=None,
    )

    decision = ShadowDecision(
        executable_state=state,
        policy_mode=PolicyMode.BASELINE,
        tradability_check=tradability_check,
        decision_ts=state.snapshot_ts,
        intended_side=Side.UP,
    )

    assert decision.state_fingerprint == state.state_fingerprint
    assert decision.decision_id == build_decision_id(
        session_id=state.session_id,
        window_id=state.window_id,
        decision_ts=state.snapshot_ts,
        side=Side.UP,
        policy_mode=PolicyMode.BASELINE,
    )

    with pytest.raises(ValueError, match="decision_ts must equal executable_state.snapshot_ts"):
        ShadowDecision(
            executable_state=state,
            policy_mode=PolicyMode.BASELINE,
            tradability_check=tradability_check,
            decision_ts=datetime(2026, 3, 26, 0, 0, 1, tzinfo=UTC),
            intended_side=Side.UP,
        )


def test_non_actionable_tradability_requires_reason() -> None:
    with pytest.raises(ValueError, match="require no_trade_reason"):
        TradabilityCheck(
            policy_mode=PolicyMode.BASELINE,
            intended_side=Side.UP,
            intended_book_side=BOOK_SIDE_ASK,
            intended_entry_price=None,
            displayed_entry_size_contracts=None,
            target_size_contracts=Decimal("10"),
            selected_net_edge=None,
            selected_spread_abs=None,
            quote_age_ms=12,
            is_actionable=False,
            no_trade_reason=None,
        )


def test_actionable_tradability_cannot_carry_reject_reason() -> None:
    with pytest.raises(ValueError, match="cannot carry no_trade_reason"):
        TradabilityCheck(
            policy_mode=PolicyMode.BASELINE,
            intended_side=Side.UP,
            intended_book_side=BOOK_SIDE_ASK,
            intended_entry_price=Decimal("0.56"),
            displayed_entry_size_contracts=Decimal("25"),
            target_size_contracts=Decimal("10"),
            selected_net_edge=Decimal("0.05"),
            selected_spread_abs=Decimal("0.02"),
            quote_age_ms=12,
            is_actionable=True,
            no_trade_reason=NoTradeReason.QUOTE_STALE,
        )


def test_shadow_order_state_and_summary_use_strict_enums_and_sorted_counts() -> None:
    decision = ShadowDecision(
        executable_state=build_state_view(),
        policy_mode=PolicyMode.BASELINE,
        tradability_check=TradabilityCheck(
            policy_mode=PolicyMode.BASELINE,
            intended_side=Side.UP,
            intended_book_side=BOOK_SIDE_ASK,
            intended_entry_price=Decimal("0.56"),
            displayed_entry_size_contracts=Decimal("25"),
            target_size_contracts=Decimal("10"),
            selected_net_edge=Decimal("0.05"),
            selected_spread_abs=Decimal("0.02"),
            quote_age_ms=12,
            is_actionable=True,
            no_trade_reason=None,
        ),
        decision_ts=build_state_view().snapshot_ts,
        intended_side=Side.UP,
    )
    order_state = ShadowOrderState(
        decision=decision,
        order_state=OrderState.ELIGIBLE_RECORDED,
        updated_ts=decision.decision_ts,
    )
    summary = ShadowSummary(
        session_id=decision.executable_state.session_id,
        policy_mode=PolicyMode.BASELINE,
        decision_count=1,
        actionable_decision_count=1,
        no_trade_count=0,
        order_state_counts={"z": 1, "a": 2},
        no_trade_reason_counts={"z": 1, "a": 2},
    )

    assert order_state.order_state == OrderState.ELIGIBLE_RECORDED
    assert list(summary.order_state_counts) == ["a", "z"]
    assert list(summary.no_trade_reason_counts) == ["a", "z"]
