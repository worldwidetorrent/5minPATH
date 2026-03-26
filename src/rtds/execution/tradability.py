"""Tradability kernel for execution v0 shadow decisions."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from rtds.core.units import to_decimal, validate_contract_price, validate_size
from rtds.execution.book_pricer import (
    ACTION_BUY,
    ExecutableBookContext,
    build_executable_book_context,
)
from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.models import ExecutableStateView, TradabilityCheck


@dataclass(slots=True, frozen=True)
class TradabilityPolicy:
    """Frozen inputs for one tradability evaluation."""

    policy_mode: PolicyMode
    target_size_contracts: Decimal
    min_net_edge: Decimal | None = None
    max_quote_age_ms: int | None = None
    max_spread_abs: Decimal | None = None
    policy_blocked: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_mode", PolicyMode(self.policy_mode))
        object.__setattr__(
            self,
            "target_size_contracts",
            to_decimal(
                validate_size(self.target_size_contracts, field_name="target_size_contracts"),
                field_name="target_size_contracts",
            ),
        )
        if self.min_net_edge is not None:
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
class TradabilityKernelResult:
    """Deterministic tradability output for one executable-state row."""

    book_context: ExecutableBookContext
    tradability_check: TradabilityCheck


def evaluate_tradability(
    *,
    executable_state: ExecutableStateView,
    intended_side: Side,
    tradability_policy: TradabilityPolicy,
    selected_net_edge: Decimal | str | int | float | None = None,
    aggressive_action: str = ACTION_BUY,
    intended_entry_price: Decimal | str | int | float | None = None,
) -> TradabilityKernelResult:
    """Build book context and strict primary reject reason for one decision."""

    normalized_side = Side(intended_side)
    normalized_edge = (
        None
        if selected_net_edge is None
        else to_decimal(selected_net_edge, field_name="selected_net_edge")
    )
    book_context = build_executable_book_context(
        executable_state=executable_state,
        intended_side=normalized_side,
        target_size_contracts=tradability_policy.target_size_contracts,
        aggressive_action=aggressive_action,
        intended_entry_price=intended_entry_price,
    )
    reject_reason = _primary_reject_reason(
        executable_state=executable_state,
        book_context=book_context,
        tradability_policy=tradability_policy,
        selected_net_edge=normalized_edge,
    )
    component_checks = _component_checks(
        executable_state=executable_state,
        book_context=book_context,
        tradability_policy=tradability_policy,
        selected_net_edge=normalized_edge,
    )
    tradability_check = TradabilityCheck(
        policy_mode=tradability_policy.policy_mode,
        intended_side=normalized_side,
        intended_book_side=book_context.intended_book_side,
        intended_entry_price=book_context.intended_entry_price,
        displayed_entry_size_contracts=book_context.intended_displayed_size_contracts,
        target_size_contracts=tradability_policy.target_size_contracts,
        selected_net_edge=normalized_edge,
        selected_spread_abs=book_context.spread_at_decision,
        quote_age_ms=book_context.quote_age_ms,
        book_side_present=component_checks["book_side_present"],
        freshness_passed=component_checks["freshness_passed"],
        size_coverage_passed=component_checks["size_coverage_passed"],
        spread_passed=component_checks["spread_passed"],
        edge_threshold_passed=component_checks["edge_threshold_passed"],
        policy_check_passed=component_checks["policy_check_passed"],
        market_actionable_passed=component_checks["market_actionable_passed"],
        is_actionable=reject_reason is None,
        no_trade_reason=reject_reason,
    )
    return TradabilityKernelResult(
        book_context=book_context,
        tradability_check=tradability_check,
    )


def _primary_reject_reason(
    *,
    executable_state: ExecutableStateView,
    book_context: ExecutableBookContext,
    tradability_policy: TradabilityPolicy,
    selected_net_edge: Decimal | None,
) -> NoTradeReason | None:
    if tradability_policy.policy_blocked:
        return NoTradeReason.POLICY_BLOCKED
    if not executable_state.market_actionable_flag:
        return NoTradeReason.MARKET_NOT_ACTIONABLE
    if tradability_policy.target_size_contracts <= 0:
        return NoTradeReason.SIZING_ZERO
    if (
        book_context.intended_entry_price is None
        or book_context.intended_displayed_size_contracts is None
    ):
        return NoTradeReason.MISSING_BOOK_SIDE
    if (
        tradability_policy.max_quote_age_ms is not None
        and (
            book_context.quote_age_ms is None
            or book_context.quote_age_ms > tradability_policy.max_quote_age_ms
        )
    ):
        return NoTradeReason.QUOTE_STALE
    if (
        tradability_policy.max_spread_abs is not None
        and (
            book_context.spread_at_decision is None
            or book_context.spread_at_decision > tradability_policy.max_spread_abs
        )
    ):
        return NoTradeReason.SPREAD_TOO_WIDE
    if book_context.intended_displayed_size_contracts < tradability_policy.target_size_contracts:
        return NoTradeReason.INSUFFICIENT_SIZE
    if (
        tradability_policy.min_net_edge is not None
        and (
            selected_net_edge is None
            or selected_net_edge < tradability_policy.min_net_edge
        )
    ):
        return NoTradeReason.EDGE_BELOW_THRESHOLD
    return None


def _component_checks(
    *,
    executable_state: ExecutableStateView,
    book_context: ExecutableBookContext,
    tradability_policy: TradabilityPolicy,
    selected_net_edge: Decimal | None,
) -> dict[str, bool]:
    book_side_present = (
        book_context.intended_entry_price is not None
        and book_context.intended_displayed_size_contracts is not None
    )
    freshness_passed = (
        tradability_policy.max_quote_age_ms is None
        or (
            book_context.quote_age_ms is not None
            and book_context.quote_age_ms <= tradability_policy.max_quote_age_ms
        )
    )
    size_coverage_passed = (
        book_context.intended_displayed_size_contracts is not None
        and book_context.intended_displayed_size_contracts
        >= tradability_policy.target_size_contracts
    )
    spread_passed = (
        tradability_policy.max_spread_abs is None
        or (
            book_context.spread_at_decision is not None
            and book_context.spread_at_decision <= tradability_policy.max_spread_abs
        )
    )
    edge_threshold_passed = (
        tradability_policy.min_net_edge is None
        or (
            selected_net_edge is not None
            and selected_net_edge >= tradability_policy.min_net_edge
        )
    )
    return {
        "book_side_present": book_side_present,
        "freshness_passed": freshness_passed,
        "size_coverage_passed": size_coverage_passed,
        "spread_passed": spread_passed,
        "edge_threshold_passed": edge_threshold_passed,
        "policy_check_passed": not tradability_policy.policy_blocked,
        "market_actionable_passed": executable_state.market_actionable_flag,
    }


__all__ = [
    "TradabilityKernelResult",
    "TradabilityPolicy",
    "evaluate_tradability",
]
