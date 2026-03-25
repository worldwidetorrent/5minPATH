"""Lower-level execution schema compatibility layer.

The canonical execution-v0 core contract now lives under `rtds.execution`.
This module remains as a lower-level normalized-state helper while the sidecar
is still being built.

Execution v0 is intentionally narrow:
- shadow is a secondary observer
- capture remains the primary runtime
- shadow must remain fail-open relative to capture
- shadow core consumes these internal contracts, never raw venue payloads

The core question for this layer is:
"Was the intended taker price actually there, on the correct side of the book,
at the decision timestamp, with enough displayed size and fresh enough quotes?"
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal, validate_contract_price, validate_size

SCHEMA_VERSION = "0.1.0"

STATE_SOURCE_LIVE = "live_state"
STATE_SOURCE_REPLAY = "replay_tail"

BOOK_SIDE_BID = "bid"
BOOK_SIDE_ASK = "ask"

CONTRACT_SIDE_UP = "up"
CONTRACT_SIDE_DOWN = "down"

SUPPORT_FLAG_SUFFICIENT = "sufficient"
SUPPORT_FLAG_THIN = "thin"
SUPPORT_FLAG_MERGE_REQUIRED = "merge_required"


def _validate_state_source_kind(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {STATE_SOURCE_LIVE, STATE_SOURCE_REPLAY}:
        raise ValueError(f"unsupported execution state source kind: {value}")
    return normalized


def _validate_contract_side(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {CONTRACT_SIDE_UP, CONTRACT_SIDE_DOWN}:
        raise ValueError(f"unsupported contract side: {value}")
    return normalized


def _validate_book_side(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {BOOK_SIDE_BID, BOOK_SIDE_ASK}:
        raise ValueError(f"unsupported book side: {value}")
    return normalized


@dataclass(slots=True, frozen=True)
class ExecutionDecisionContext:
    """Identity and gating context for one shadow-sidecar decision timestamp."""

    state_source_kind: str
    decision_ts: datetime
    asset_id: str
    window_id: str
    window_start_ts: datetime
    window_end_ts: datetime
    seconds_remaining: int
    polymarket_market_id: str
    polymarket_slug: str | None
    clob_token_id_up: str | None
    clob_token_id_down: str | None
    window_quality_regime: str
    chainlink_confidence_state: str
    volatility_regime: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "state_source_kind",
            _validate_state_source_kind(self.state_source_kind),
        )
        object.__setattr__(
            self,
            "decision_ts",
            ensure_utc(self.decision_ts, field_name="decision_ts"),
        )
        object.__setattr__(
            self,
            "window_start_ts",
            ensure_utc(self.window_start_ts, field_name="window_start_ts"),
        )
        object.__setattr__(
            self,
            "window_end_ts",
            ensure_utc(self.window_end_ts, field_name="window_end_ts"),
        )
        if self.seconds_remaining < 0:
            raise ValueError("seconds_remaining must be non-negative")

    @property
    def production_safe(self) -> bool:
        """Return whether this state source kind is valid for production shadow runtime."""

        return self.state_source_kind == STATE_SOURCE_LIVE


@dataclass(slots=True, frozen=True)
class ExecutionFairValueState:
    """Fair-value state already normalized into internal execution terms."""

    fair_value_base: Decimal | None
    calibrated_fair_value_base: Decimal | None
    calibration_bucket: str | None
    calibration_support_flag: str | None

    def __post_init__(self) -> None:
        for field_name in ("fair_value_base", "calibrated_fair_value_base"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    validate_contract_price(value, field_name=field_name),
                )
        if self.calibration_support_flag is not None:
            support_flag = str(self.calibration_support_flag).strip().lower()
            if support_flag not in {
                SUPPORT_FLAG_SUFFICIENT,
                SUPPORT_FLAG_THIN,
                SUPPORT_FLAG_MERGE_REQUIRED,
            }:
                raise ValueError(
                    f"unsupported calibration_support_flag: {self.calibration_support_flag}"
                )
            object.__setattr__(self, "calibration_support_flag", support_flag)


@dataclass(slots=True, frozen=True)
class ExecutionBookState:
    """Venue-neutral executable book state consumed by execution-v0 core logic."""

    quote_event_ts: datetime | None
    quote_recv_ts: datetime | None
    quote_age_ms: int | None
    quote_source: str
    up_bid: Decimal | None
    up_ask: Decimal | None
    down_bid: Decimal | None
    down_ask: Decimal | None
    up_bid_size_contracts: Decimal | None
    up_ask_size_contracts: Decimal | None
    down_bid_size_contracts: Decimal | None
    down_ask_size_contracts: Decimal | None
    market_spread_up_abs: Decimal | None = None
    market_spread_down_abs: Decimal | None = None
    usable_flag: bool = True

    def __post_init__(self) -> None:
        for field_name in ("quote_event_ts", "quote_recv_ts"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    ensure_utc(value, field_name=field_name),
                )
        if self.quote_age_ms is not None and self.quote_age_ms < 0:
            raise ValueError("quote_age_ms must be non-negative")
        for field_name in (
            "up_bid",
            "up_ask",
            "down_bid",
            "down_ask",
            "market_spread_up_abs",
            "market_spread_down_abs",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    validate_contract_price(value, field_name=field_name),
                )
        for field_name in (
            "up_bid_size_contracts",
            "up_ask_size_contracts",
            "down_bid_size_contracts",
            "down_ask_size_contracts",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    to_decimal(validate_size(value, field_name=field_name), field_name=field_name),
                )

    def price_for(self, *, contract_side: str, book_side: str) -> Decimal | None:
        """Return the requested book price using internal side names."""

        normalized_contract_side = _validate_contract_side(contract_side)
        normalized_book_side = _validate_book_side(book_side)
        return getattr(self, f"{normalized_contract_side}_{normalized_book_side}")

    def size_for(self, *, contract_side: str, book_side: str) -> Decimal | None:
        """Return the requested displayed size using internal side names."""

        normalized_contract_side = _validate_contract_side(contract_side)
        normalized_book_side = _validate_book_side(book_side)
        return getattr(self, f"{normalized_contract_side}_{normalized_book_side}_size_contracts")


@dataclass(slots=True, frozen=True)
class ExecutionRuntimeState:
    """Frozen internal state consumed by the execution-v0 shadow sidecar core."""

    context: ExecutionDecisionContext
    fair_value: ExecutionFairValueState
    book_state: ExecutionBookState
    schema_version: str = SCHEMA_VERSION


@dataclass(slots=True, frozen=True)
class IntendedExecutionTerms:
    """Computed taker execution terms at the exact decision timestamp."""

    decision_ts: datetime
    contract_side: str
    book_side: str
    intended_price: Decimal | None
    displayed_size_contracts: Decimal | None
    target_size_contracts: Decimal
    enough_displayed_size: bool
    quote_event_ts: datetime | None
    quote_recv_ts: datetime | None
    quote_age_ms: int | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "decision_ts",
            ensure_utc(self.decision_ts, field_name="decision_ts"),
        )
        object.__setattr__(self, "contract_side", _validate_contract_side(self.contract_side))
        object.__setattr__(self, "book_side", _validate_book_side(self.book_side))
        if self.intended_price is not None:
            object.__setattr__(
                self,
                "intended_price",
                validate_contract_price(self.intended_price, field_name="intended_price"),
            )
        if self.displayed_size_contracts is not None:
            object.__setattr__(
                self,
                "displayed_size_contracts",
                to_decimal(
                    validate_size(
                        self.displayed_size_contracts,
                        field_name="displayed_size_contracts",
                    ),
                    field_name="displayed_size_contracts",
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
        for field_name in ("quote_event_ts", "quote_recv_ts"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    ensure_utc(value, field_name=field_name),
                )
        if self.quote_age_ms is not None and self.quote_age_ms < 0:
            raise ValueError("quote_age_ms must be non-negative")


def build_taker_intended_terms(
    state: ExecutionRuntimeState,
    *,
    contract_side: str,
    target_size_contracts: Decimal | str | int | float,
) -> IntendedExecutionTerms:
    """Build the intended taker terms for the requested contract side.

    Execution-v0 is taker-only. The relevant book side is therefore always `ask`
    for the chosen contract side.
    """

    normalized_contract_side = _validate_contract_side(contract_side)
    normalized_target_size = to_decimal(
        validate_size(target_size_contracts, field_name="target_size_contracts"),
        field_name="target_size_contracts",
    )
    intended_price = state.book_state.price_for(
        contract_side=normalized_contract_side,
        book_side=BOOK_SIDE_ASK,
    )
    displayed_size = state.book_state.size_for(
        contract_side=normalized_contract_side,
        book_side=BOOK_SIDE_ASK,
    )
    enough_size = (
        displayed_size is not None
        and intended_price is not None
        and displayed_size >= normalized_target_size
    )
    return IntendedExecutionTerms(
        decision_ts=state.context.decision_ts,
        contract_side=normalized_contract_side,
        book_side=BOOK_SIDE_ASK,
        intended_price=intended_price,
        displayed_size_contracts=displayed_size,
        target_size_contracts=normalized_target_size,
        enough_displayed_size=enough_size,
        quote_event_ts=state.book_state.quote_event_ts,
        quote_recv_ts=state.book_state.quote_recv_ts,
        quote_age_ms=state.book_state.quote_age_ms,
    )


__all__ = [
    "BOOK_SIDE_ASK",
    "BOOK_SIDE_BID",
    "CONTRACT_SIDE_DOWN",
    "CONTRACT_SIDE_UP",
    "SCHEMA_VERSION",
    "STATE_SOURCE_LIVE",
    "STATE_SOURCE_REPLAY",
    "SUPPORT_FLAG_MERGE_REQUIRED",
    "SUPPORT_FLAG_SUFFICIENT",
    "SUPPORT_FLAG_THIN",
    "ExecutionBookState",
    "ExecutionDecisionContext",
    "ExecutionFairValueState",
    "ExecutionRuntimeState",
    "IntendedExecutionTerms",
    "build_taker_intended_terms",
]
