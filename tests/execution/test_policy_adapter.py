from __future__ import annotations

from decimal import Decimal

from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.policy_adapter import PolicyEvaluationInput, evaluate_policy_decision
from rtds.execution.sizing import SIZE_MODE_FIXED_CONTRACTS, SIZE_MODE_FIXED_NOTIONAL, SizingPolicy
from tests.execution.support import build_state_view


def test_policy_adapter_returns_actionable_shadow_decision() -> None:
    decision = evaluate_policy_decision(
        PolicyEvaluationInput(
            executable_state=build_state_view(),
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts=Decimal("10"),
            ),
            min_net_edge=Decimal("0.03"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            policy_name="good_only_baseline",
            policy_role="baseline",
        )
    )

    assert decision.eligible is True
    assert decision.intended_side == Side.UP
    assert decision.requested_size_contracts == Decimal("10")
    assert decision.intended_size_contracts == Decimal("10")
    assert decision.intended_entry_price == Decimal("0.56")
    assert decision.primary_decision_reason is None
    assert decision.shadow_decision.tradability_check.is_actionable is True


def test_policy_adapter_propagates_policy_block_reason() -> None:
    decision = evaluate_policy_decision(
        PolicyEvaluationInput(
            executable_state=build_state_view(),
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts=Decimal("10"),
            ),
            min_net_edge=Decimal("0.03"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_blocked=True,
        )
    )

    assert decision.eligible is False
    assert decision.primary_decision_reason == NoTradeReason.POLICY_BLOCKED
    assert (
        decision.shadow_decision.tradability_check.no_trade_reason
        == NoTradeReason.POLICY_BLOCKED
    )


def test_policy_adapter_uses_raw_fair_value_when_calibration_support_is_thin() -> None:
    decision = evaluate_policy_decision(
        PolicyEvaluationInput(
            executable_state=build_state_view(
                fair_value_base=Decimal("0.58"),
                calibrated_fair_value_base=Decimal("0.90"),
                calibration_support_flag="thin",
            ),
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts=Decimal("10"),
            ),
            min_net_edge=Decimal("0.01"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            policy_name="good_only_baseline",
            policy_role="baseline",
        )
    )

    assert decision.selected_fair_value_base == Decimal("0.58")
    assert decision.selected_net_edge == Decimal("0.02")


def test_policy_adapter_returns_sizing_zero_for_unpriceable_fixed_notional() -> None:
    decision = evaluate_policy_decision(
        PolicyEvaluationInput(
            executable_state=build_state_view(
                up_ask_size_contracts=Decimal("0"),
            ),
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_NOTIONAL,
                fixed_notional_value=Decimal("10"),
            ),
            min_net_edge=Decimal("0.03"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            policy_name="good_only_baseline",
            policy_role="baseline",
        )
    )

    assert decision.eligible is False
    assert decision.primary_decision_reason == NoTradeReason.SIZING_ZERO
