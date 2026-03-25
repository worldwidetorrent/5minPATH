"""Execution-v0 adapter boundaries.

Runtime ownership is intentionally fixed:
- capture remains the primary runtime
- shadow is a secondary observer
- shadow failure must not affect capture
- shadow writes only to its own artifact tree

Production shadow runtime must use a `live_state` adapter only.
`replay_tail` adapters exist for smoke tests, replay debugging, and local validation;
they are explicitly non-production.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from rtds.schemas.execution import (
    STATE_SOURCE_LIVE,
    STATE_SOURCE_REPLAY,
    ExecutionRuntimeState,
)

ADAPTER_ROLE_LIVE_STATE = STATE_SOURCE_LIVE
ADAPTER_ROLE_REPLAY_TAIL = STATE_SOURCE_REPLAY


@dataclass(slots=True, frozen=True)
class AdapterDescriptor:
    """Minimal descriptor for one execution-state adapter."""

    adapter_name: str
    adapter_role: str
    production_safe: bool

    def __post_init__(self) -> None:
        normalized_role = str(self.adapter_role).strip().lower()
        if normalized_role not in {ADAPTER_ROLE_LIVE_STATE, ADAPTER_ROLE_REPLAY_TAIL}:
            raise ValueError(f"unsupported adapter_role: {self.adapter_role}")
        if normalized_role == ADAPTER_ROLE_LIVE_STATE and not self.production_safe:
            raise ValueError("live_state adapters must be marked production_safe")
        if normalized_role == ADAPTER_ROLE_REPLAY_TAIL and self.production_safe:
            raise ValueError("replay_tail adapters must remain non-production")
        object.__setattr__(self, "adapter_role", normalized_role)


class ExecutionStateAdapter(Protocol):
    """Protocol for all execution-v0 state adapters."""

    descriptor: AdapterDescriptor

    def read_state(self) -> ExecutionRuntimeState | None:
        """Return the next normalized execution state or `None` if no state is ready."""

    def close(self) -> None:
        """Release adapter resources without affecting capture."""


def assert_live_state_adapter(descriptor: AdapterDescriptor) -> None:
    """Reject non-production adapters for live shadow runtime wiring."""

    if descriptor.adapter_role != ADAPTER_ROLE_LIVE_STATE:
        raise ValueError(
            "production shadow runtime must use a live_state adapter, "
            f"got {descriptor.adapter_role}"
        )


__all__ = [
    "ADAPTER_ROLE_LIVE_STATE",
    "ADAPTER_ROLE_REPLAY_TAIL",
    "AdapterDescriptor",
    "ExecutionStateAdapter",
    "assert_live_state_adapter",
]
