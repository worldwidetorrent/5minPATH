from __future__ import annotations

from decimal import Decimal

from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.tradability import TradabilityPolicy, evaluate_tradability
from tests.execution.support import build_state_view


def test_tradability_rejects_missing_book_side_before_other_checks() -> None:
    state = build_state_view(up_ask_price=None, up_ask_size_contracts=None)

    result = evaluate_tradability(
        executable_state=state,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("10"),
            max_quote_age_ms=10,
            max_spread_abs=Decimal("0.01"),
            min_net_edge=Decimal("0.50"),
        ),
        selected_net_edge=Decimal("0.01"),
    )

    assert result.tradability_check.is_actionable is False
    assert result.tradability_check.no_trade_reason == NoTradeReason.MISSING_BOOK_SIDE


def test_tradability_rejects_stale_quotes() -> None:
    state = build_state_view(quote_age_ms=50)

    result = evaluate_tradability(
        executable_state=state,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("10"),
            max_quote_age_ms=10,
        ),
        selected_net_edge=Decimal("0.05"),
    )

    assert result.tradability_check.no_trade_reason == NoTradeReason.QUOTE_STALE


def test_tradability_rejects_wide_spread_before_size_and_edge() -> None:
    state = build_state_view(up_spread_abs=Decimal("0.05"))

    result = evaluate_tradability(
        executable_state=state,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("30"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            min_net_edge=Decimal("0.50"),
        ),
        selected_net_edge=Decimal("0.01"),
    )

    assert result.tradability_check.no_trade_reason == NoTradeReason.SPREAD_TOO_WIDE


def test_tradability_rejects_insufficient_size_then_edge_then_accepts() -> None:
    state = build_state_view()

    too_large = evaluate_tradability(
        executable_state=state,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("30"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            min_net_edge=Decimal("0.02"),
        ),
        selected_net_edge=Decimal("0.05"),
    )
    weak_edge = evaluate_tradability(
        executable_state=state,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("10"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            min_net_edge=Decimal("0.06"),
        ),
        selected_net_edge=Decimal("0.05"),
    )
    actionable = evaluate_tradability(
        executable_state=state,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("10"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            min_net_edge=Decimal("0.02"),
        ),
        selected_net_edge=Decimal("0.05"),
    )

    assert too_large.tradability_check.no_trade_reason == NoTradeReason.INSUFFICIENT_SIZE
    assert weak_edge.tradability_check.no_trade_reason == NoTradeReason.EDGE_BELOW_THRESHOLD
    assert actionable.tradability_check.is_actionable is True
    assert actionable.tradability_check.no_trade_reason is None


def test_tradability_rejects_non_actionable_market() -> None:
    state = build_state_view(market_actionable_flag=False)

    result = evaluate_tradability(
        executable_state=state,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("10"),
        ),
        selected_net_edge=Decimal("0.05"),
    )

    assert result.tradability_check.no_trade_reason == NoTradeReason.MARKET_NOT_ACTIONABLE
