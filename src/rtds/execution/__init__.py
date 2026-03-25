"""Execution-v0 shadow-sidecar boundaries."""

from rtds.execution.adapters import (
    ADAPTER_ROLE_LIVE_STATE,
    ADAPTER_ROLE_REPLAY_TAIL,
    AdapterDescriptor,
    ExecutionStateAdapter,
    assert_live_state_adapter,
)
from rtds.execution.book_pricer import (
    ACTION_BUY,
    ACTION_SELL,
    ExecutableBookContext,
    build_executable_book_context,
    resolve_intended_book_side,
)
from rtds.execution.enums import NoTradeReason, OrderState, PolicyMode, Side
from rtds.execution.models import (
    ExecutableStateView,
    ShadowDecision,
    ShadowOrderState,
    ShadowSummary,
    TradabilityCheck,
    build_decision_id,
    build_state_fingerprint,
)
from rtds.execution.policy_adapter import PolicyDecision, PolicyEvaluationInput
from rtds.execution.sizing import SizingInput, cap_size_to_displayed_liquidity
from rtds.execution.tradability import (
    TradabilityKernelResult,
    TradabilityPolicy,
    evaluate_tradability,
)
from rtds.execution.version import SCHEMA_VERSION

__all__ = [
    "ADAPTER_ROLE_LIVE_STATE",
    "ADAPTER_ROLE_REPLAY_TAIL",
    "ACTION_BUY",
    "ACTION_SELL",
    "AdapterDescriptor",
    "ExecutableBookContext",
    "ExecutionStateAdapter",
    "ExecutableStateView",
    "NoTradeReason",
    "OrderState",
    "PolicyDecision",
    "PolicyEvaluationInput",
    "PolicyMode",
    "SCHEMA_VERSION",
    "ShadowDecision",
    "ShadowOrderState",
    "ShadowSummary",
    "SizingInput",
    "Side",
    "TradabilityCheck",
    "TradabilityKernelResult",
    "TradabilityPolicy",
    "assert_live_state_adapter",
    "build_executable_book_context",
    "build_decision_id",
    "build_state_fingerprint",
    "cap_size_to_displayed_liquidity",
    "evaluate_tradability",
    "resolve_intended_book_side",
]
