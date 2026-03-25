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
from rtds.execution.ledger import (
    LEDGER_STATE_SEEN,
    LEDGER_STATE_WRITTEN,
    LedgerEvent,
    ShadowLedger,
)
from rtds.execution.models import (
    ExecutableStateView,
    ShadowDecision,
    ShadowOrderState,
    ShadowSummary,
    TradabilityCheck,
    build_decision_id,
    build_state_fingerprint,
)
from rtds.execution.policy_adapter import (
    PolicyDecision,
    PolicyEvaluationInput,
    evaluate_policy_decision,
)
from rtds.execution.shadow_engine import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IDLE_SLEEP_SECONDS,
    DEFAULT_RECENT_DECISION_BUFFER_SIZE,
    ShadowEngine,
    ShadowEngineConfig,
    ShadowEngineStats,
)
from rtds.execution.sizing import (
    SIZE_MODE_FIXED_CONTRACTS,
    SIZE_MODE_FIXED_NOTIONAL,
    SizingDecision,
    SizingInput,
    SizingPolicy,
    cap_size_to_displayed_liquidity,
    evaluate_sizing,
)
from rtds.execution.tradability import (
    TradabilityKernelResult,
    TradabilityPolicy,
    evaluate_tradability,
)
from rtds.execution.version import SCHEMA_VERSION
from rtds.execution.writer import (
    SHADOW_DECISIONS_FILENAME,
    SHADOW_ROOT_DIRNAME,
    SHADOW_SUMMARY_FILENAME,
    ShadowArtifactPaths,
    ShadowArtifactWriter,
    shadow_artifact_paths,
)

__all__ = [
    "ADAPTER_ROLE_LIVE_STATE",
    "ADAPTER_ROLE_REPLAY_TAIL",
    "ACTION_BUY",
    "ACTION_SELL",
    "AdapterDescriptor",
    "ExecutableBookContext",
    "ExecutionStateAdapter",
    "ExecutableStateView",
    "DEFAULT_HEARTBEAT_INTERVAL_SECONDS",
    "DEFAULT_IDLE_SLEEP_SECONDS",
    "DEFAULT_RECENT_DECISION_BUFFER_SIZE",
    "LEDGER_STATE_SEEN",
    "LEDGER_STATE_WRITTEN",
    "LedgerEvent",
    "NoTradeReason",
    "OrderState",
    "PolicyDecision",
    "PolicyEvaluationInput",
    "PolicyMode",
    "SCHEMA_VERSION",
    "SHADOW_DECISIONS_FILENAME",
    "SHADOW_ROOT_DIRNAME",
    "SHADOW_SUMMARY_FILENAME",
    "SIZE_MODE_FIXED_CONTRACTS",
    "SIZE_MODE_FIXED_NOTIONAL",
    "ShadowEngine",
    "ShadowEngineConfig",
    "ShadowEngineStats",
    "ShadowDecision",
    "ShadowArtifactPaths",
    "ShadowArtifactWriter",
    "ShadowLedger",
    "ShadowOrderState",
    "ShadowSummary",
    "SizingDecision",
    "SizingInput",
    "SizingPolicy",
    "Side",
    "TradabilityCheck",
    "TradabilityKernelResult",
    "TradabilityPolicy",
    "assert_live_state_adapter",
    "build_executable_book_context",
    "build_decision_id",
    "build_state_fingerprint",
    "cap_size_to_displayed_liquidity",
    "evaluate_policy_decision",
    "evaluate_sizing",
    "evaluate_tradability",
    "resolve_intended_book_side",
    "shadow_artifact_paths",
]
