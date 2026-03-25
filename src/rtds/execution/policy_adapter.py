"""Deterministic policy-to-shadow-decision adapter for execution v0."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from rtds.core.units import to_decimal, validate_contract_price
from rtds.execution.book_pricer import ACTION_BUY, build_executable_book_context
from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.models import (
    BOOK_SIDE_ASK,
    ExecutableStateView,
    ShadowDecision,
    TradabilityCheck,
)
from rtds.execution.sizing import SizingPolicy, evaluate_sizing
from rtds.execution.tradability import TradabilityPolicy, evaluate_tradability


@dataclass(slots=True, frozen=True)
class PolicyEvaluationInput:
    """Frozen policy-evaluation input for the shadow sidecar."""

    executable_state: ExecutableStateView
    policy_mode: PolicyMode
    sizing_policy: SizingPolicy
    min_net_edge: Decimal
    max_quote_age_ms: int | None
    max_spread_abs: Decimal | None
    policy_name: str
    policy_role: str
    policy_blocked: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        object.__setattr__(
            self,
            "min_net_edge",
            to_decimal(self.min_net_edge, field_name="min_net_edge"),
        )
        if self.max_quote_age_ms is not None and self.max_quote_age_ms < 0:
            raise ValueError("max_quote_age_ms must be non-negative")
        if self.max_spread_abs is not None:
            object.__setattr__(
                self,
                "max_spread_abs",
                validate_contract_price(self.max_spread_abs, field_name="max_spread_abs"),
            )


@dataclass(slots=True, frozen=True)
class PolicyDecision:
    """Deterministic policy + sizing + tradability result."""

    policy_name: str
    policy_role: str
    policy_mode: PolicyMode
    selected_fair_value_base: Decimal | None
    selected_net_edge: Decimal | None
    eligible: bool
    intended_side: Side | None
    requested_size_contracts: Decimal
    intended_size_contracts: Decimal
    intended_entry_price: Decimal | None
    primary_decision_reason: NoTradeReason | None
    tradability_check: TradabilityCheck
    shadow_decision: ShadowDecision
    no_trade_reason: NoTradeReason | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        if self.intended_side is not None:
            object.__setattr__(self, "intended_side", Side(self.intended_side))
        if self.selected_fair_value_base is not None:
            object.__setattr__(
                self,
                "selected_fair_value_base",
                validate_contract_price(
                    self.selected_fair_value_base,
                    field_name="selected_fair_value_base",
                ),
            )
        if self.selected_net_edge is not None:
            object.__setattr__(
                self,
                "selected_net_edge",
                to_decimal(self.selected_net_edge, field_name="selected_net_edge"),
            )
        object.__setattr__(
            self,
            "requested_size_contracts",
            to_decimal(self.requested_size_contracts, field_name="requested_size_contracts"),
        )
        object.__setattr__(
            self,
            "intended_size_contracts",
            to_decimal(self.intended_size_contracts, field_name="intended_size_contracts"),
        )
        if self.intended_entry_price is not None:
            object.__setattr__(
                self,
                "intended_entry_price",
                validate_contract_price(
                    self.intended_entry_price,
                    field_name="intended_entry_price",
                ),
            )
        if self.primary_decision_reason is not None:
            object.__setattr__(
                self,
                "primary_decision_reason",
                NoTradeReason(self.primary_decision_reason),
            )
        if self.no_trade_reason is not None:
            object.__setattr__(self, "no_trade_reason", NoTradeReason(self.no_trade_reason))
        if self.eligible != self.tradability_check.is_actionable:
            raise ValueError("eligible must match tradability_check.is_actionable")


def evaluate_policy_decision(policy_input: PolicyEvaluationInput) -> PolicyDecision:
    """Turn one executable-state row into one deterministic shadow decision."""

    selected_fair_value = _select_fair_value_base(policy_input.executable_state)
    intended_side, selected_net_edge = _select_side_and_edge(
        executable_state=policy_input.executable_state,
        selected_fair_value_base=selected_fair_value,
    )
    if intended_side is None:
        return _build_rejected_policy_decision(
            policy_input=policy_input,
            intended_side=None,
            selected_fair_value_base=selected_fair_value,
            selected_net_edge=selected_net_edge,
            requested_size_contracts=Decimal("0"),
            intended_size_contracts=Decimal("0"),
            intended_entry_price=None,
            primary_reason=NoTradeReason.INVALID_STATE,
        )
    preview_context = build_executable_book_context(
        executable_state=policy_input.executable_state,
        intended_side=intended_side,
        target_size_contracts=Decimal("0"),
        aggressive_action=ACTION_BUY,
    )
    sizing_decision = evaluate_sizing(
        book_context=preview_context,
        sizing_policy=policy_input.sizing_policy,
    )
    if sizing_decision.intended_size_contracts <= 0:
        return _build_rejected_policy_decision(
            policy_input=policy_input,
            intended_side=intended_side,
            selected_fair_value_base=selected_fair_value,
            selected_net_edge=selected_net_edge,
            requested_size_contracts=sizing_decision.requested_size_contracts,
            intended_size_contracts=sizing_decision.intended_size_contracts,
            intended_entry_price=sizing_decision.intended_entry_price,
            primary_reason=NoTradeReason.SIZING_ZERO,
        )
    tradability_result = evaluate_tradability(
        executable_state=policy_input.executable_state,
        intended_side=intended_side,
        tradability_policy=TradabilityPolicy(
            policy_mode=policy_input.policy_mode,
            target_size_contracts=sizing_decision.intended_size_contracts,
            min_net_edge=policy_input.min_net_edge,
            max_quote_age_ms=policy_input.max_quote_age_ms,
            max_spread_abs=policy_input.max_spread_abs,
            policy_blocked=policy_input.policy_blocked,
        ),
        selected_net_edge=selected_net_edge,
        intended_entry_price=sizing_decision.intended_entry_price,
    )
    shadow_decision = ShadowDecision(
        executable_state=policy_input.executable_state,
        policy_mode=policy_input.policy_mode,
        tradability_check=tradability_result.tradability_check,
        decision_ts=policy_input.executable_state.snapshot_ts,
        intended_side=intended_side,
    )
    primary_reason = tradability_result.tradability_check.no_trade_reason
    return PolicyDecision(
        policy_name=policy_input.policy_name,
        policy_role=policy_input.policy_role,
        policy_mode=policy_input.policy_mode,
        selected_fair_value_base=selected_fair_value,
        selected_net_edge=selected_net_edge,
        eligible=tradability_result.tradability_check.is_actionable,
        intended_side=intended_side,
        requested_size_contracts=sizing_decision.requested_size_contracts,
        intended_size_contracts=sizing_decision.intended_size_contracts,
        intended_entry_price=sizing_decision.intended_entry_price,
        primary_decision_reason=primary_reason,
        tradability_check=tradability_result.tradability_check,
        shadow_decision=shadow_decision,
        no_trade_reason=primary_reason,
    )


def _select_fair_value_base(executable_state: ExecutableStateView) -> Decimal | None:
    if (
        executable_state.calibrated_fair_value_base is not None
        and executable_state.calibration_support_flag == "sufficient"
    ):
        return executable_state.calibrated_fair_value_base
    return executable_state.fair_value_base


def _select_side_and_edge(
    *,
    executable_state: ExecutableStateView,
    selected_fair_value_base: Decimal | None,
) -> tuple[Side | None, Decimal | None]:
    if selected_fair_value_base is None:
        return None, None
    up_edge = _net_edge_for_side(
        executable_state=executable_state,
        selected_fair_value_base=selected_fair_value_base,
        side=Side.UP,
    )
    down_edge = _net_edge_for_side(
        executable_state=executable_state,
        selected_fair_value_base=selected_fair_value_base,
        side=Side.DOWN,
    )
    if up_edge is None and down_edge is None:
        return None, None
    if down_edge is None:
        return Side.UP, up_edge
    if up_edge is None:
        return Side.DOWN, down_edge
    if up_edge >= down_edge:
        return Side.UP, up_edge
    return Side.DOWN, down_edge


def _net_edge_for_side(
    *,
    executable_state: ExecutableStateView,
    selected_fair_value_base: Decimal,
    side: Side,
) -> Decimal | None:
    ask_price = executable_state.price_for(side=side, book_side=BOOK_SIDE_ASK)
    if ask_price is None:
        return None
    if side == Side.UP:
        return selected_fair_value_base - ask_price
    return (Decimal("1") - selected_fair_value_base) - ask_price


def _build_rejected_policy_decision(
    *,
    policy_input: PolicyEvaluationInput,
    intended_side: Side | None,
    selected_fair_value_base: Decimal | None,
    selected_net_edge: Decimal | None,
    requested_size_contracts: Decimal,
    intended_size_contracts: Decimal,
    intended_entry_price: Decimal | None,
    primary_reason: NoTradeReason,
) -> PolicyDecision:
    tradability_check = TradabilityCheck(
        policy_mode=policy_input.policy_mode,
        intended_side=intended_side,
        intended_book_side=BOOK_SIDE_ASK,
        intended_entry_price=intended_entry_price,
        displayed_entry_size_contracts=None,
        target_size_contracts=intended_size_contracts,
        selected_net_edge=selected_net_edge,
        selected_spread_abs=None,
        quote_age_ms=policy_input.executable_state.quote_age_ms,
        is_actionable=False,
        no_trade_reason=primary_reason,
    )
    shadow_decision = ShadowDecision(
        executable_state=policy_input.executable_state,
        policy_mode=policy_input.policy_mode,
        tradability_check=tradability_check,
        decision_ts=policy_input.executable_state.snapshot_ts,
        intended_side=intended_side,
    )
    return PolicyDecision(
        policy_name=policy_input.policy_name,
        policy_role=policy_input.policy_role,
        policy_mode=policy_input.policy_mode,
        selected_fair_value_base=selected_fair_value_base,
        selected_net_edge=selected_net_edge,
        eligible=False,
        intended_side=intended_side,
        requested_size_contracts=requested_size_contracts,
        intended_size_contracts=intended_size_contracts,
        intended_entry_price=intended_entry_price,
        primary_decision_reason=primary_reason,
        tradability_check=tradability_check,
        shadow_decision=shadow_decision,
        no_trade_reason=primary_reason,
    )


__all__ = [
    "PolicyDecision",
    "PolicyEvaluationInput",
    "evaluate_policy_decision",
]
