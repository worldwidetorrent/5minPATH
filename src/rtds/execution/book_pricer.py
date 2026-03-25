"""Deterministic book-pricing kernel for execution v0."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal, validate_contract_price, validate_size
from rtds.execution.enums import Side
from rtds.execution.models import (
    BOOK_SIDE_ASK,
    BOOK_SIDE_BID,
    ExecutableStateView,
)

ACTION_BUY = "buy"
ACTION_SELL = "sell"


def _validate_aggressive_action(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {ACTION_BUY, ACTION_SELL}:
        raise ValueError(f"unsupported aggressive_action: {value}")
    return normalized


@dataclass(slots=True, frozen=True)
class ExecutableBookContext:
    """Exact top-of-book context used for one shadow tradability evaluation."""

    decision_ts: object
    intended_side: Side
    aggressive_action: str
    intended_book_side: str
    top_bid_at_decision: Decimal | None
    top_ask_at_decision: Decimal | None
    intended_entry_price: Decimal | None
    intended_displayed_size_contracts: Decimal | None
    spread_at_decision: Decimal | None
    quote_event_ts: object | None
    quote_recv_ts: object | None
    quote_age_ms: int | None
    target_size_contracts: Decimal
    state_fingerprint: str
    entry_slippage_vs_top_of_book: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "decision_ts",
            ensure_utc(self.decision_ts, field_name="decision_ts"),
        )
        object.__setattr__(self, "intended_side", Side(self.intended_side))
        object.__setattr__(
            self,
            "aggressive_action",
            _validate_aggressive_action(self.aggressive_action),
        )
        if self.quote_event_ts is not None:
            object.__setattr__(
                self,
                "quote_event_ts",
                ensure_utc(self.quote_event_ts, field_name="quote_event_ts"),
            )
        if self.quote_recv_ts is not None:
            object.__setattr__(
                self,
                "quote_recv_ts",
                ensure_utc(self.quote_recv_ts, field_name="quote_recv_ts"),
            )
        if self.quote_age_ms is not None and self.quote_age_ms < 0:
            raise ValueError("quote_age_ms must be non-negative")
        for field_name in (
            "top_bid_at_decision",
            "top_ask_at_decision",
            "intended_entry_price",
            "spread_at_decision",
            "entry_slippage_vs_top_of_book",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    validate_contract_price(value, field_name=field_name),
                )
        if self.intended_displayed_size_contracts is not None:
            object.__setattr__(
                self,
                "intended_displayed_size_contracts",
                to_decimal(
                    validate_size(
                        self.intended_displayed_size_contracts,
                        field_name="intended_displayed_size_contracts",
                    ),
                    field_name="intended_displayed_size_contracts",
                ),
            )
        object.__setattr__(
            self,
            "target_size_contracts",
            to_decimal(
                validate_size(self.target_size_contracts, field_name="target_size_contracts"),
                field_name="target_size_contracts",
            ),
        )


def resolve_intended_book_side(*, aggressive_action: str) -> str:
    """Map the taker action into the executable side of book.

    Frozen execution-v0 mapping:
    - buy Up   -> up_ask
    - buy Down -> down_ask
    - sell Up  -> up_bid
    - sell Down-> down_bid
    """

    normalized_action = _validate_aggressive_action(aggressive_action)
    if normalized_action == ACTION_BUY:
        return BOOK_SIDE_ASK
    return BOOK_SIDE_BID


def build_executable_book_context(
    *,
    executable_state: ExecutableStateView,
    intended_side: Side,
    target_size_contracts: Decimal | str | int | float,
    aggressive_action: str = ACTION_BUY,
    intended_entry_price: Decimal | str | int | float | None = None,
) -> ExecutableBookContext:
    """Build deterministic executable book context for one state row."""

    normalized_side = Side(intended_side)
    normalized_action = _validate_aggressive_action(aggressive_action)
    normalized_target_size = to_decimal(
        validate_size(target_size_contracts, field_name="target_size_contracts"),
        field_name="target_size_contracts",
    )
    intended_book_side = resolve_intended_book_side(aggressive_action=normalized_action)
    top_bid = executable_state.price_for(side=normalized_side, book_side=BOOK_SIDE_BID)
    top_ask = executable_state.price_for(side=normalized_side, book_side=BOOK_SIDE_ASK)
    top_of_book_price = executable_state.price_for(
        side=normalized_side,
        book_side=intended_book_side,
    )
    displayed_size = executable_state.size_for(
        side=normalized_side,
        book_side=intended_book_side,
    )
    selected_entry_price = top_of_book_price
    if intended_entry_price is not None:
        selected_entry_price = validate_contract_price(
            intended_entry_price,
            field_name="intended_entry_price",
        )
    slippage_vs_top = None
    if selected_entry_price is not None and top_of_book_price is not None:
        slippage_vs_top = validate_contract_price(
            abs(selected_entry_price - top_of_book_price),
            field_name="entry_slippage_vs_top_of_book",
        )
    return ExecutableBookContext(
        decision_ts=executable_state.snapshot_ts,
        intended_side=normalized_side,
        aggressive_action=normalized_action,
        intended_book_side=intended_book_side,
        top_bid_at_decision=top_bid,
        top_ask_at_decision=top_ask,
        intended_entry_price=selected_entry_price,
        intended_displayed_size_contracts=displayed_size,
        spread_at_decision=executable_state.spread_for(side=normalized_side),
        quote_event_ts=executable_state.quote_event_ts,
        quote_recv_ts=executable_state.quote_recv_ts,
        quote_age_ms=executable_state.quote_age_ms,
        target_size_contracts=normalized_target_size,
        state_fingerprint=executable_state.state_fingerprint,
        entry_slippage_vs_top_of_book=slippage_vs_top,
    )


__all__ = [
    "ACTION_BUY",
    "ACTION_SELL",
    "ExecutableBookContext",
    "build_executable_book_context",
    "resolve_intended_book_side",
]
