"""Execution-v0 sizing boundary.

Sizing stays intentionally narrow in v0:
- taker-only
- displayed top-of-book size only
- no queue modeling
- no partial-fill logic

This module consumes only normalized execution-state contracts.
It must not depend on raw venue payloads or SDK-specific client types.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from rtds.core.units import to_decimal, validate_size
from rtds.schemas.execution import (
    CONTRACT_SIDE_DOWN,
    CONTRACT_SIDE_UP,
    ExecutionRuntimeState,
)


def _validate_contract_side(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {CONTRACT_SIDE_UP, CONTRACT_SIDE_DOWN}:
        raise ValueError(f"unsupported contract side: {value}")
    return normalized


@dataclass(slots=True, frozen=True)
class SizingInput:
    """Minimal size-selection input for taker-only shadow execution."""

    runtime_state: ExecutionRuntimeState
    contract_side: str
    target_size_contracts: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_side", _validate_contract_side(self.contract_side))
        object.__setattr__(
            self,
            "target_size_contracts",
            to_decimal(
                validate_size(self.target_size_contracts, field_name="target_size_contracts"),
                field_name="target_size_contracts",
            ),
        )


def cap_size_to_displayed_liquidity(sizing_input: SizingInput) -> Decimal:
    """Cap intended size to the displayed ask liquidity for the selected side."""

    displayed_size = sizing_input.runtime_state.book_state.size_for(
        contract_side=sizing_input.contract_side,
        book_side="ask",
    )
    if displayed_size is None:
        return Decimal("0")
    if displayed_size < 0:
        raise ValueError("displayed book size must be non-negative")
    return min(sizing_input.target_size_contracts, displayed_size)


__all__ = [
    "SizingInput",
    "cap_size_to_displayed_liquidity",
]
