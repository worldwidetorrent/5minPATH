from __future__ import annotations

from collections import deque
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from rtds.execution.adapters import ADAPTER_ROLE_LIVE_STATE, AdapterDescriptor
from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.models import (
    BOOK_SIDE_ASK,
    ExecutableStateView,
    ShadowDecision,
    TradabilityCheck,
)


def build_state_view(
    *,
    session_id: str = "20260326T000000000Z",
    snapshot_ts: datetime | None = None,
    fair_value_base: Decimal | None = Decimal("0.58"),
    calibrated_fair_value_base: Decimal | None = Decimal("0.61"),
    calibration_support_flag: str | None = "sufficient",
    market_actionable_flag: bool = True,
    up_ask_price: Decimal | None = Decimal("0.56"),
    down_ask_price: Decimal | None = Decimal("0.46"),
    up_ask_size_contracts: Decimal | None = Decimal("25"),
    down_ask_size_contracts: Decimal | None = Decimal("25"),
    quote_age_ms: int | None = 12,
    up_spread_abs: Decimal | None = Decimal("0.02"),
    down_spread_abs: Decimal | None = Decimal("0.02"),
) -> ExecutableStateView:
    decision_ts = snapshot_ts or datetime(2026, 3, 26, 0, 0, tzinfo=UTC)
    return ExecutableStateView(
        session_id=session_id,
        state_source_kind="live_state",
        snapshot_ts=decision_ts,
        window_id="btc-5m-20260326T000000Z",
        window_start_ts=datetime(2026, 3, 26, 0, 0, tzinfo=UTC),
        window_end_ts=datetime(2026, 3, 26, 0, 5, tzinfo=UTC),
        seconds_remaining=240,
        polymarket_market_id="0xshadow",
        polymarket_slug="btc-updown-5m-1770000600",
        clob_token_id_up="up-token",
        clob_token_id_down="down-token",
        window_quality_regime="good",
        chainlink_confidence_state="high",
        volatility_regime="mid_vol",
        fair_value_base=fair_value_base,
        calibrated_fair_value_base=calibrated_fair_value_base,
        calibration_bucket="far_up",
        calibration_support_flag=calibration_support_flag,
        quote_source="polymarket",
        quote_event_ts=decision_ts,
        quote_recv_ts=decision_ts,
        quote_age_ms=quote_age_ms,
        up_bid_price=Decimal("0.54"),
        up_ask_price=up_ask_price,
        down_bid_price=Decimal("0.44"),
        down_ask_price=down_ask_price,
        up_bid_size_contracts=Decimal("40"),
        up_ask_size_contracts=up_ask_size_contracts,
        down_bid_size_contracts=Decimal("40"),
        down_ask_size_contracts=down_ask_size_contracts,
        up_spread_abs=up_spread_abs,
        down_spread_abs=down_spread_abs,
        market_actionable_flag=market_actionable_flag,
    )


def build_tradability_check(
    *,
    actionable: bool,
    intended_side: Side = Side.UP,
    reason: NoTradeReason | None = None,
) -> TradabilityCheck:
    return TradabilityCheck(
        policy_mode=PolicyMode.BASELINE,
        intended_side=intended_side,
        intended_book_side=BOOK_SIDE_ASK,
        intended_entry_price=Decimal("0.56") if actionable else None,
        displayed_entry_size_contracts=Decimal("25") if actionable else None,
        target_size_contracts=Decimal("10"),
        selected_net_edge=Decimal("0.05") if actionable else None,
        selected_spread_abs=Decimal("0.02") if actionable else None,
        quote_age_ms=12,
        is_actionable=actionable,
        no_trade_reason=None if actionable else (reason or NoTradeReason.EDGE_BELOW_THRESHOLD),
    )


def build_shadow_decision(
    *,
    actionable: bool,
    state: ExecutableStateView | None = None,
    intended_side: Side = Side.UP,
    reason: NoTradeReason | None = None,
) -> ShadowDecision:
    state_view = state or build_state_view()
    return ShadowDecision(
        executable_state=state_view,
        policy_mode=PolicyMode.BASELINE,
        tradability_check=build_tradability_check(
            actionable=actionable,
            intended_side=intended_side,
            reason=reason,
        ),
        decision_ts=state_view.snapshot_ts,
        intended_side=intended_side if state_view.fair_value_base is not None else None,
    )


class FakeLiveAdapter:
    descriptor = AdapterDescriptor(
        adapter_name="fake-live",
        adapter_role=ADAPTER_ROLE_LIVE_STATE,
        production_safe=True,
    )

    def __init__(self, states: list[ExecutableStateView | Exception]) -> None:
        self._states = deque(states)
        self.closed = False

    def read_state(self) -> ExecutableStateView | None:
        if not self._states:
            return None
        value = self._states.popleft()
        if isinstance(value, Exception):
            raise value
        return value

    def close(self) -> None:
        self.closed = True


def replace_state(
    state: ExecutableStateView,
    /,
    **changes: object,
) -> ExecutableStateView:
    updates = dict(changes)
    if "state_fingerprint" not in updates:
        updates["state_fingerprint"] = None
    return replace(state, **updates)
