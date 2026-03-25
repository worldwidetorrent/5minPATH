"""Venue-neutral policy input/output contracts for execution v0.

This module is part of the frozen execution boundary:
- no raw venue payloads
- no SDK client objects
- core policy wiring consumes only normalized internal execution state
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from rtds.execution.enums import NoTradeReason
from rtds.execution.models import ExecutableStateView, ShadowDecision, TradabilityCheck


@dataclass(slots=True, frozen=True)
class PolicyEvaluationInput:
    """Frozen policy-evaluation input for the shadow sidecar."""

    executable_state: ExecutableStateView
    min_net_edge: Decimal
    target_trade_size_contracts: Decimal
    policy_name: str
    policy_role: str


@dataclass(slots=True, frozen=True)
class PolicyDecision:
    """Policy decision contract emitted before any execution audit is recorded."""

    policy_name: str
    policy_role: str
    eligible: bool
    tradability_check: TradabilityCheck
    shadow_decision: ShadowDecision | None
    no_trade_reason: NoTradeReason | None


__all__ = [
    "PolicyDecision",
    "PolicyEvaluationInput",
]
