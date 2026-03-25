"""Execution-v0 sizing boundary."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from rtds.core.units import to_decimal, validate_size
from rtds.execution.book_pricer import ExecutableBookContext
from rtds.execution.enums import Side
from rtds.execution.models import BOOK_SIDE_ASK, ExecutableStateView

SIZE_MODE_FIXED_CONTRACTS = "fixed_contracts"
SIZE_MODE_FIXED_NOTIONAL = "fixed_notional"


def _validate_size_mode(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {SIZE_MODE_FIXED_CONTRACTS, SIZE_MODE_FIXED_NOTIONAL}:
        raise ValueError(f"unsupported size_mode: {value}")
    return normalized


@dataclass(slots=True, frozen=True)
class SizingInput:
    """Minimal size-selection input for taker-only shadow execution."""

    executable_state: ExecutableStateView
    contract_side: Side
    target_size_contracts: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_side", Side(self.contract_side))
        object.__setattr__(
            self,
            "target_size_contracts",
            to_decimal(
                validate_size(self.target_size_contracts, field_name="target_size_contracts"),
                field_name="target_size_contracts",
            ),
        )


@dataclass(slots=True, frozen=True)
class SizingPolicy:
    """Frozen execution-v0 sizing policy."""

    size_mode: str
    fixed_size_contracts: Decimal | None = None
    fixed_notional_value: Decimal | None = None

    def __post_init__(self) -> None:
        normalized_mode = _validate_size_mode(self.size_mode)
        object.__setattr__(self, "size_mode", normalized_mode)
        if self.fixed_size_contracts is not None:
            object.__setattr__(
                self,
                "fixed_size_contracts",
                to_decimal(
                    validate_size(self.fixed_size_contracts, field_name="fixed_size_contracts"),
                    field_name="fixed_size_contracts",
                ),
            )
        if self.fixed_notional_value is not None:
            object.__setattr__(
                self,
                "fixed_notional_value",
                to_decimal(
                    validate_size(
                        self.fixed_notional_value,
                        field_name="fixed_notional_value",
                    ),
                    field_name="fixed_notional_value",
                ),
            )
        if normalized_mode == SIZE_MODE_FIXED_CONTRACTS and self.fixed_size_contracts is None:
            raise ValueError("fixed_contracts mode requires fixed_size_contracts")
        if normalized_mode == SIZE_MODE_FIXED_NOTIONAL and self.fixed_notional_value is None:
            raise ValueError("fixed_notional mode requires fixed_notional_value")


@dataclass(slots=True, frozen=True)
class SizingDecision:
    """Deterministic sizing result before tradability gating."""

    size_mode: str
    intended_side: Side
    intended_entry_price: Decimal | None
    displayed_size_contracts: Decimal
    requested_size_contracts: Decimal
    intended_size_contracts: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "size_mode", _validate_size_mode(self.size_mode))
        object.__setattr__(self, "intended_side", Side(self.intended_side))
        for field_name in (
            "displayed_size_contracts",
            "requested_size_contracts",
            "intended_size_contracts",
        ):
            object.__setattr__(
                self,
                field_name,
                to_decimal(
                    validate_size(getattr(self, field_name), field_name=field_name),
                    field_name=field_name,
                ),
            )
        if self.intended_entry_price is not None:
            object.__setattr__(
                self,
                "intended_entry_price",
                to_decimal(self.intended_entry_price, field_name="intended_entry_price"),
            )


def cap_size_to_displayed_liquidity(sizing_input: SizingInput) -> Decimal:
    """Cap intended size to the displayed ask liquidity for the selected side."""

    displayed_size = sizing_input.executable_state.size_for(
        side=sizing_input.contract_side,
        book_side=BOOK_SIDE_ASK,
    )
    if displayed_size is None:
        return Decimal("0")
    if displayed_size < 0:
        raise ValueError("displayed book size must be non-negative")
    return min(sizing_input.target_size_contracts, displayed_size)


def evaluate_sizing(
    *,
    book_context: ExecutableBookContext,
    sizing_policy: SizingPolicy,
) -> SizingDecision:
    """Compute requested size, then cap it by displayed top-of-book liquidity."""

    requested_size = _requested_size_contracts(
        book_context=book_context,
        sizing_policy=sizing_policy,
    )
    displayed_size = book_context.intended_displayed_size_contracts or Decimal("0")
    intended_size = min(requested_size, displayed_size)
    return SizingDecision(
        size_mode=sizing_policy.size_mode,
        intended_side=book_context.intended_side,
        intended_entry_price=book_context.intended_entry_price,
        displayed_size_contracts=displayed_size,
        requested_size_contracts=requested_size,
        intended_size_contracts=intended_size,
    )


def _requested_size_contracts(
    *,
    book_context: ExecutableBookContext,
    sizing_policy: SizingPolicy,
) -> Decimal:
    if sizing_policy.size_mode == SIZE_MODE_FIXED_CONTRACTS:
        return sizing_policy.fixed_size_contracts or Decimal("0")
    entry_price = book_context.intended_entry_price
    if entry_price is None or entry_price <= 0:
        return Decimal("0")
    return (sizing_policy.fixed_notional_value or Decimal("0")) / entry_price


__all__ = [
    "SIZE_MODE_FIXED_CONTRACTS",
    "SIZE_MODE_FIXED_NOTIONAL",
    "SizingDecision",
    "SizingInput",
    "SizingPolicy",
    "cap_size_to_displayed_liquidity",
    "evaluate_sizing",
]
