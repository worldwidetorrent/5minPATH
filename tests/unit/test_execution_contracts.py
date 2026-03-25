from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from rtds.execution.adapters import (
    ADAPTER_ROLE_LIVE_STATE,
    ADAPTER_ROLE_REPLAY_TAIL,
    AdapterDescriptor,
    assert_live_state_adapter,
)
from rtds.execution.sizing import SizingInput, cap_size_to_displayed_liquidity
from rtds.schemas.execution import (
    CONTRACT_SIDE_DOWN,
    CONTRACT_SIDE_UP,
    STATE_SOURCE_LIVE,
    STATE_SOURCE_REPLAY,
    SUPPORT_FLAG_THIN,
    ExecutionBookState,
    ExecutionDecisionContext,
    ExecutionFairValueState,
    ExecutionRuntimeState,
    build_taker_intended_terms,
)


def _build_runtime_state() -> ExecutionRuntimeState:
    return ExecutionRuntimeState(
        context=ExecutionDecisionContext(
            state_source_kind=STATE_SOURCE_LIVE,
            decision_ts=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
            asset_id="BTC",
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
        ),
        fair_value=ExecutionFairValueState(
            fair_value_base=Decimal("0.58"),
            calibrated_fair_value_base=Decimal("0.61"),
            calibration_bucket="far_up",
            calibration_support_flag=SUPPORT_FLAG_THIN,
        ),
        book_state=ExecutionBookState(
            quote_event_ts=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
            quote_recv_ts=datetime(2026, 3, 25, 12, 0, 0, 2000, tzinfo=UTC),
            quote_age_ms=12,
            quote_source="polymarket",
            up_bid=Decimal("0.54"),
            up_ask=Decimal("0.56"),
            down_bid=Decimal("0.44"),
            down_ask=Decimal("0.46"),
            up_bid_size_contracts=Decimal("90"),
            up_ask_size_contracts=Decimal("25"),
            down_bid_size_contracts=Decimal("75"),
            down_ask_size_contracts=Decimal("30"),
            market_spread_up_abs=Decimal("0.02"),
            market_spread_down_abs=Decimal("0.02"),
        ),
    )


def test_execution_state_source_kind_marks_production_safety() -> None:
    live_context = _build_runtime_state().context
    replay_context = ExecutionDecisionContext(
        state_source_kind=STATE_SOURCE_REPLAY,
        decision_ts=live_context.decision_ts,
        asset_id=live_context.asset_id,
        window_id=live_context.window_id,
        window_start_ts=live_context.window_start_ts,
        window_end_ts=live_context.window_end_ts,
        seconds_remaining=live_context.seconds_remaining,
        polymarket_market_id=live_context.polymarket_market_id,
        polymarket_slug=live_context.polymarket_slug,
        clob_token_id_up=live_context.clob_token_id_up,
        clob_token_id_down=live_context.clob_token_id_down,
        window_quality_regime=live_context.window_quality_regime,
        chainlink_confidence_state=live_context.chainlink_confidence_state,
        volatility_regime=live_context.volatility_regime,
    )

    assert live_context.production_safe is True
    assert replay_context.production_safe is False


def test_build_taker_intended_terms_uses_correct_ask_side() -> None:
    runtime_state = _build_runtime_state()

    up_terms = build_taker_intended_terms(
        runtime_state,
        contract_side=CONTRACT_SIDE_UP,
        target_size_contracts=Decimal("10"),
    )
    down_terms = build_taker_intended_terms(
        runtime_state,
        contract_side=CONTRACT_SIDE_DOWN,
        target_size_contracts=Decimal("35"),
    )

    assert up_terms.book_side == "ask"
    assert up_terms.intended_price == Decimal("0.56")
    assert up_terms.displayed_size_contracts == Decimal("25")
    assert up_terms.enough_displayed_size is True

    assert down_terms.intended_price == Decimal("0.46")
    assert down_terms.displayed_size_contracts == Decimal("30")
    assert down_terms.enough_displayed_size is False


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

    with pytest.raises(ValueError, match="replay_tail adapters must remain non-production"):
        AdapterDescriptor(
            adapter_name="bad-replay",
            adapter_role=ADAPTER_ROLE_REPLAY_TAIL,
            production_safe=True,
        )


def test_sizing_caps_to_displayed_ask_liquidity() -> None:
    runtime_state = _build_runtime_state()

    capped_size = cap_size_to_displayed_liquidity(
        SizingInput(
            runtime_state=runtime_state,
            contract_side=CONTRACT_SIDE_UP,
            target_size_contracts=Decimal("40"),
        )
    )
    uncapped_size = cap_size_to_displayed_liquidity(
        SizingInput(
            runtime_state=runtime_state,
            contract_side=CONTRACT_SIDE_DOWN,
            target_size_contracts=Decimal("12"),
        )
    )

    assert capped_size == Decimal("25")
    assert uncapped_size == Decimal("12")
