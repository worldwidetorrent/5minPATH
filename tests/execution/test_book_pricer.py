from __future__ import annotations

from decimal import Decimal

from rtds.execution.book_pricer import (
    ACTION_SELL,
    build_executable_book_context,
    resolve_intended_book_side,
)
from rtds.execution.enums import Side
from rtds.execution.models import BOOK_SIDE_ASK, BOOK_SIDE_BID
from tests.execution.support import build_state_view


def test_book_pricer_freezes_buy_and_sell_side_mapping() -> None:
    state = build_state_view()

    buy_down = build_executable_book_context(
        executable_state=state,
        intended_side=Side.DOWN,
        target_size_contracts=Decimal("8"),
    )
    sell_up = build_executable_book_context(
        executable_state=state,
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


def test_book_pricer_exposes_top_of_book_size_spread_and_slippage() -> None:
    state = build_state_view()

    context = build_executable_book_context(
        executable_state=state,
        intended_side=Side.UP,
        target_size_contracts=Decimal("10"),
        intended_entry_price=Decimal("0.58"),
    )

    assert context.top_bid_at_decision == Decimal("0.54")
    assert context.top_ask_at_decision == Decimal("0.56")
    assert context.intended_displayed_size_contracts == Decimal("25")
    assert context.spread_at_decision == Decimal("0.02")
    assert context.entry_slippage_vs_top_of_book == Decimal("0.02")
    assert context.decision_ts == state.snapshot_ts
    assert context.state_fingerprint == state.state_fingerprint


def test_book_pricer_preserves_missing_book_side_in_context() -> None:
    state = build_state_view(up_ask_price=None, up_ask_size_contracts=None)

    context = build_executable_book_context(
        executable_state=state,
        intended_side=Side.UP,
        target_size_contracts=Decimal("10"),
    )

    assert context.intended_entry_price is None
    assert context.intended_displayed_size_contracts is None
    assert context.entry_slippage_vs_top_of_book is None
