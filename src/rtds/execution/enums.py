"""Strict execution-v0 enums."""

from __future__ import annotations

from enum import StrEnum


class Side(StrEnum):
    """Binary contract side."""

    UP = "up"
    DOWN = "down"


class PolicyMode(StrEnum):
    """Frozen shadow policy modes for execution v0."""

    BASELINE = "baseline"
    EXPLORATORY = "exploratory"
    CONTEXT_GATED = "context_gated"


class OrderState(StrEnum):
    """Append-only shadow order lifecycle states."""

    ELIGIBLE_RECORDED = "eligible_recorded"
    NO_TRADE_RECORDED = "no_trade_recorded"
    ERROR_RECORDED = "error_recorded"


class NoTradeReason(StrEnum):
    """Exclusive no-trade reasons for execution v0."""

    POLICY_BLOCKED = "policy_blocked"
    EDGE_BELOW_THRESHOLD = "edge_below_threshold"
    QUOTE_STALE = "quote_stale"
    INSUFFICIENT_SIZE = "insufficient_size"
    SPREAD_TOO_WIDE = "spread_too_wide"
    MISSING_BOOK_SIDE = "missing_book_side"
    INVALID_STATE = "invalid_state"
    MARKET_NOT_ACTIONABLE = "market_not_actionable"
    SIZING_ZERO = "sizing_zero"
    UNKNOWN_ERROR = "unknown_error"


__all__ = [
    "NoTradeReason",
    "OrderState",
    "PolicyMode",
    "Side",
]
