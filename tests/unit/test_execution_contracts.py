from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from rtds.execution.adapters import (
    ADAPTER_ROLE_LIVE_STATE,
    ADAPTER_ROLE_REPLAY_TAIL,
    AdapterDescriptor,
    assert_live_state_adapter,
)
from rtds.execution.book_pricer import (
    ACTION_SELL,
    build_executable_book_context,
    resolve_intended_book_side,
)
from rtds.execution.enums import NoTradeReason, OrderState, PolicyMode, Side
from rtds.execution.models import (
    BOOK_SIDE_ASK,
    BOOK_SIDE_BID,
    ExecutableStateView,
    ShadowDecision,
    ShadowOrderState,
    TradabilityCheck,
    build_decision_id,
)
from rtds.execution.sizing import SizingInput, cap_size_to_displayed_liquidity
from rtds.execution.tradability import TradabilityPolicy, evaluate_tradability


def _build_state_view(*, up_ask_price: Decimal = Decimal("0.56")) -> ExecutableStateView:
    return ExecutableStateView(
        session_id="20260325T120000000Z",
        state_source_kind="live_state",
        snapshot_ts=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
        window_id="btc-5m-20260325T120000Z",
        window_start_ts=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
        window_end_ts=datetime(2026, 3, 25, 12, 5, tzinfo=UTC),
        seconds_remaining=221,
        polymarket_market_id="0xabc123",
        polymarket_slug="btc-updown-5m-1770000000",
        clob_token_id_up="token-up",
        clob_token_id_down="token-down",
        window_quality_regime="good",
        chainlink_confidence_state="high",
        volatility_regime="mid_vol",
        fair_value_base=Decimal("0.58"),
        calibrated_fair_value_base=Decimal("0.61"),
        calibration_bucket="far_up",
        calibration_support_flag="sufficient",
        quote_source="polymarket",
        quote_event_ts=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
        quote_recv_ts=datetime(2026, 3, 25, 12, 0, 0, 2000, tzinfo=UTC),
        quote_age_ms=12,
        up_bid_price=Decimal("0.54"),
        up_ask_price=up_ask_price,
        down_bid_price=Decimal("0.44"),
        down_ask_price=Decimal("0.46"),
        up_bid_size_contracts=Decimal("90"),
        up_ask_size_contracts=Decimal("25"),
        down_bid_size_contracts=Decimal("75"),
        down_ask_size_contracts=Decimal("30"),
        up_spread_abs=Decimal("0.02"),
        down_spread_abs=Decimal("0.02"),
    )


def test_state_fingerprint_is_deterministic_and_changes_with_state() -> None:
    state_view_a = _build_state_view()
    state_view_b = _build_state_view()
    state_view_c = _build_state_view(up_ask_price=Decimal("0.57"))

    assert state_view_a.state_fingerprint == state_view_b.state_fingerprint
    assert state_view_a.state_fingerprint != state_view_c.state_fingerprint


def test_shadow_decision_uses_snapshot_timestamp_and_deterministic_id() -> None:
    state_view = _build_state_view()
    tradability_check = TradabilityCheck(
        policy_mode=PolicyMode.BASELINE,
        intended_side=Side.UP,
        intended_book_side=BOOK_SIDE_ASK,
        intended_entry_price=Decimal("0.56"),
        displayed_entry_size_contracts=Decimal("25"),
        target_size_contracts=Decimal("10"),
        selected_net_edge=Decimal("0.03"),
        selected_spread_abs=Decimal("0.02"),
        quote_age_ms=12,
        is_actionable=True,
        no_trade_reason=None,
    )

    decision = ShadowDecision(
        executable_state=state_view,
        policy_mode=PolicyMode.BASELINE,
        tradability_check=tradability_check,
        decision_ts=state_view.snapshot_ts,
        intended_side=Side.UP,
    )

    assert decision.decision_ts == state_view.snapshot_ts
    assert decision.state_fingerprint == state_view.state_fingerprint
    assert decision.decision_id == build_decision_id(
        session_id=state_view.session_id,
        window_id=state_view.window_id,
        decision_ts=state_view.snapshot_ts,
        side=Side.UP,
        policy_mode=PolicyMode.BASELINE,
    )

    with pytest.raises(ValueError, match="decision_ts must equal executable_state.snapshot_ts"):
        ShadowDecision(
            executable_state=state_view,
            policy_mode=PolicyMode.BASELINE,
            tradability_check=tradability_check,
            decision_ts=datetime(2026, 3, 25, 12, 0, 1, tzinfo=UTC),
            intended_side=Side.UP,
        )


def test_non_actionable_tradability_requires_strict_reason() -> None:
    with pytest.raises(ValueError, match="require no_trade_reason"):
        TradabilityCheck(
            policy_mode=PolicyMode.EXPLORATORY,
            intended_side=Side.DOWN,
            intended_book_side=BOOK_SIDE_ASK,
            intended_entry_price=None,
            displayed_entry_size_contracts=None,
            target_size_contracts=Decimal("5"),
            selected_net_edge=None,
            selected_spread_abs=None,
            quote_age_ms=1200,
            is_actionable=False,
            no_trade_reason=None,
        )

    blocked = TradabilityCheck(
        policy_mode=PolicyMode.EXPLORATORY,
        intended_side=Side.DOWN,
        intended_book_side=BOOK_SIDE_ASK,
        intended_entry_price=None,
        displayed_entry_size_contracts=None,
        target_size_contracts=Decimal("5"),
        selected_net_edge=None,
        selected_spread_abs=None,
        quote_age_ms=1200,
        is_actionable=False,
        no_trade_reason=NoTradeReason.QUOTE_STALE,
    )

    assert blocked.no_trade_reason == NoTradeReason.QUOTE_STALE


def test_adapter_descriptor_enforces_live_vs_replay_boundary() -> None:
    live_descriptor = AdapterDescriptor(
        adapter_name="live-polymarket-state",
        adapter_role=ADAPTER_ROLE_LIVE_STATE,
        production_safe=True,
    )
    replay_descriptor = AdapterDescriptor(
        adapter_name="debug-replay-tail",
        adapter_role=ADAPTER_ROLE_REPLAY_TAIL,
        production_safe=False,
    )

    assert_live_state_adapter(live_descriptor)

    with pytest.raises(ValueError, match="live_state adapter"):
        assert_live_state_adapter(replay_descriptor)


def test_sizing_caps_to_displayed_ask_liquidity() -> None:
    state_view = _build_state_view()

    capped_size = cap_size_to_displayed_liquidity(
        SizingInput(
            executable_state=state_view,
            contract_side=Side.UP,
            target_size_contracts=Decimal("40"),
        )
    )
    uncapped_size = cap_size_to_displayed_liquidity(
        SizingInput(
            executable_state=state_view,
            contract_side=Side.DOWN,
            target_size_contracts=Decimal("12"),
        )
    )

    assert capped_size == Decimal("25")
    assert uncapped_size == Decimal("12")


def test_shadow_order_state_uses_strict_enum() -> None:
    state_view = _build_state_view()
    tradability_check = TradabilityCheck(
        policy_mode=PolicyMode.BASELINE,
        intended_side=Side.UP,
        intended_book_side=BOOK_SIDE_ASK,
        intended_entry_price=Decimal("0.56"),
        displayed_entry_size_contracts=Decimal("25"),
        target_size_contracts=Decimal("10"),
        selected_net_edge=Decimal("0.03"),
        selected_spread_abs=Decimal("0.02"),
        quote_age_ms=12,
        is_actionable=True,
        no_trade_reason=None,
    )
    decision = ShadowDecision(
        executable_state=state_view,
        policy_mode=PolicyMode.BASELINE,
        tradability_check=tradability_check,
        decision_ts=state_view.snapshot_ts,
        intended_side=Side.UP,
    )

    order_state = ShadowOrderState(
        decision=decision,
        order_state=OrderState.ELIGIBLE_RECORDED,
        updated_ts=state_view.snapshot_ts,
    )

    assert order_state.order_state == OrderState.ELIGIBLE_RECORDED


def test_book_pricer_freezes_buy_and_sell_side_mapping() -> None:
    state_view = _build_state_view()

    buy_down = build_executable_book_context(
        executable_state=state_view,
        intended_side=Side.DOWN,
        target_size_contracts=Decimal("8"),
    )
    sell_up = build_executable_book_context(
        executable_state=state_view,
        intended_side=Side.UP,
        target_size_contracts=Decimal("8"),
        aggressive_action=ACTION_SELL,
    )

    assert resolve_intended_book_side(aggressive_action="buy") == BOOK_SIDE_ASK
    assert resolve_intended_book_side(aggressive_action="sell") == BOOK_SIDE_BID
    assert buy_down.intended_book_side == BOOK_SIDE_ASK
    assert buy_down.intended_entry_price == Decimal("0.46")
    assert sell_up.intended_book_side == BOOK_SIDE_BID
    assert sell_up.intended_entry_price == Decimal("0.54")


def test_book_pricer_exposes_top_of_book_and_slippage_metric() -> None:
    state_view = _build_state_view()

    book_context = build_executable_book_context(
        executable_state=state_view,
        intended_side=Side.UP,
        target_size_contracts=Decimal("10"),
        intended_entry_price=Decimal("0.58"),
    )

    assert book_context.top_bid_at_decision == Decimal("0.54")
    assert book_context.top_ask_at_decision == Decimal("0.56")
    assert book_context.intended_entry_price == Decimal("0.58")
    assert book_context.spread_at_decision == Decimal("0.02")
    assert book_context.entry_slippage_vs_top_of_book == Decimal("0.02")


def test_tradability_rejects_stale_quotes_with_primary_reason() -> None:
    state_view = _build_state_view()

    result = evaluate_tradability(
        executable_state=state_view,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("10"),
            max_quote_age_ms=10,
            max_spread_abs=Decimal("0.03"),
            min_net_edge=Decimal("0.01"),
        ),
        selected_net_edge=Decimal("0.05"),
    )

    assert result.tradability_check.is_actionable is False
    assert result.tradability_check.no_trade_reason == NoTradeReason.QUOTE_STALE


def test_tradability_rejects_missing_book_side_before_other_checks() -> None:
    state_view = replace(
        _build_state_view(),
        up_ask_price=None,
        up_ask_size_contracts=None,
        state_fingerprint=None,
    )

    result = evaluate_tradability(
        executable_state=state_view,
        intended_side=Side.UP,
        tradability_policy=TradabilityPolicy(
            policy_mode=PolicyMode.BASELINE,
            target_size_contracts=Decimal("10"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
        ),
        selected_net_edge=Decimal("0.05"),
    )

    assert result.tradability_check.is_actionable is False
    assert result.tradability_check.no_trade_reason == NoTradeReason.MISSING_BOOK_SIDE


def test_tradability_rejects_size_then_edge_then_accepts() -> None:
    state_view = _build_state_view()

    too_large = evaluate_tradability(
        executable_state=state_view,
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
        executable_state=state_view,
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
        executable_state=state_view,
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
