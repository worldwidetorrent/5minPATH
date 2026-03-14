"""Replay slice generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable

from rtds.core.units import to_decimal, validate_volatility
from rtds.features.executable_edge import ExecutableEdgeEstimate
from rtds.replay.attach_labels import LabeledSnapshotRecord
from rtds.replay.simulate import SIM_NO_TRADE, SimulatedTrade

SECONDS_REMAINING_DIMENSION = "seconds_remaining_bucket"
VOLATILITY_DIMENSION = "volatility_regime"
COMPOSITE_QUALITY_DIMENSION = "composite_quality_state"
CHAINLINK_CONFIDENCE_DIMENSION = "chainlink_confidence_state"
RAW_EDGE_DIMENSION = "raw_edge_bucket"
NET_EDGE_DIMENSION = "net_edge_bucket"
SPREAD_DIMENSION = "spread_bucket"
SIGNAL_DIRECTION_DIMENSION = "signal_direction"

SLICE_DIMENSIONS: tuple[str, ...] = (
    SECONDS_REMAINING_DIMENSION,
    VOLATILITY_DIMENSION,
    COMPOSITE_QUALITY_DIMENSION,
    CHAINLINK_CONFIDENCE_DIMENSION,
    RAW_EDGE_DIMENSION,
    NET_EDGE_DIMENSION,
    SPREAD_DIMENSION,
    SIGNAL_DIRECTION_DIMENSION,
)


@dataclass(slots=True, frozen=True)
class ReplaySlicePolicy:
    """Explicit bucket thresholds for replay slicing."""

    early_window_seconds_min: int = 180
    mid_window_seconds_min: int = 60
    low_vol_threshold: Decimal = Decimal("0.00005")
    high_vol_threshold: Decimal = Decimal("0.00010")
    strong_edge_threshold: Decimal = Decimal("0.03")
    medium_edge_threshold: Decimal = Decimal("0.01")
    tight_spread_threshold: Decimal = Decimal("0.01")
    wide_spread_threshold: Decimal = Decimal("0.03")

    def __post_init__(self) -> None:
        if self.early_window_seconds_min < self.mid_window_seconds_min:
            raise ValueError("early_window_seconds_min must be >= mid_window_seconds_min")
        if self.mid_window_seconds_min < 0:
            raise ValueError("mid_window_seconds_min must be non-negative")

        low_vol = validate_volatility(self.low_vol_threshold, field_name="low_vol_threshold")
        high_vol = validate_volatility(self.high_vol_threshold, field_name="high_vol_threshold")
        if high_vol < low_vol:
            raise ValueError("high_vol_threshold must be >= low_vol_threshold")

        strong_edge = to_decimal(self.strong_edge_threshold, field_name="strong_edge_threshold")
        medium_edge = to_decimal(self.medium_edge_threshold, field_name="medium_edge_threshold")
        if strong_edge < medium_edge:
            raise ValueError("strong_edge_threshold must be >= medium_edge_threshold")

        tight_spread = to_decimal(
            self.tight_spread_threshold,
            field_name="tight_spread_threshold",
        )
        wide_spread = to_decimal(self.wide_spread_threshold, field_name="wide_spread_threshold")
        if wide_spread < tight_spread:
            raise ValueError("wide_spread_threshold must be >= tight_spread_threshold")

        object.__setattr__(self, "low_vol_threshold", low_vol)
        object.__setattr__(self, "high_vol_threshold", high_vol)
        object.__setattr__(self, "strong_edge_threshold", strong_edge)
        object.__setattr__(self, "medium_edge_threshold", medium_edge)
        object.__setattr__(self, "tight_spread_threshold", tight_spread)
        object.__setattr__(self, "wide_spread_threshold", wide_spread)


DEFAULT_REPLAY_SLICE_POLICY = ReplaySlicePolicy()


@dataclass(slots=True, frozen=True)
class ReplaySliceInput:
    """One replay-evaluation row with the extra state needed for slicing."""

    labeled_snapshot: LabeledSnapshotRecord
    executable_edge: ExecutableEdgeEstimate
    simulated_trade: SimulatedTrade
    seconds_remaining: int
    sigma_eff: Decimal | None = None
    composite_quality_state: str | None = None
    chainlink_confidence_state: str | None = None

    def __post_init__(self) -> None:
        if self.seconds_remaining < 0:
            raise ValueError("seconds_remaining must be non-negative")
        if self.sigma_eff is not None:
            object.__setattr__(
                self,
                "sigma_eff",
                validate_volatility(self.sigma_eff, field_name="sigma_eff"),
            )
        snapshot_id = self.labeled_snapshot.snapshot.snapshot_id or ""
        if self.simulated_trade.snapshot_id != snapshot_id:
            raise ValueError("simulated_trade.snapshot_id must match labeled snapshot snapshot_id")


@dataclass(slots=True, frozen=True)
class ReplaySliceResult:
    """Aggregate statistics for one slice bucket."""

    slice_dimension: str
    slice_key: str
    row_count: int
    trade_count: int
    no_trade_count: int
    hit_rate: Decimal
    total_pnl: Decimal
    average_pnl: Decimal
    average_roi: Decimal | None
    average_predicted_edge: Decimal | None
    average_realized_edge: Decimal | None
    realized_minus_predicted_edge: Decimal | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "hit_rate", to_decimal(self.hit_rate, field_name="hit_rate"))
        object.__setattr__(self, "total_pnl", to_decimal(self.total_pnl, field_name="total_pnl"))
        object.__setattr__(
            self,
            "average_pnl",
            to_decimal(self.average_pnl, field_name="average_pnl"),
        )
        for field_name in (
            "average_roi",
            "average_predicted_edge",
            "average_realized_edge",
            "realized_minus_predicted_edge",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, to_decimal(value, field_name=field_name))


@dataclass(slots=True, frozen=True)
class ReplaySliceReport:
    """All slice tables for a replay run."""

    by_dimension: dict[str, tuple[ReplaySliceResult, ...]]


@dataclass(slots=True)
class _SliceAccumulator:
    row_count: int = 0
    trade_count: int = 0
    no_trade_count: int = 0
    win_count: int = 0
    total_pnl: Decimal = field(default_factory=lambda: Decimal("0"))
    total_roi: Decimal = field(default_factory=lambda: Decimal("0"))
    total_predicted_edge: Decimal = field(default_factory=lambda: Decimal("0"))
    predicted_edge_count: int = 0
    total_realized_edge: Decimal = field(default_factory=lambda: Decimal("0"))
    realized_edge_count: int = 0


def generate_replay_slices(
    inputs: Iterable[ReplaySliceInput],
    *,
    policy: ReplaySlicePolicy = DEFAULT_REPLAY_SLICE_POLICY,
) -> ReplaySliceReport:
    """Slice replay results across the core phase-1 research dimensions."""

    accumulators: dict[str, dict[str, _SliceAccumulator]] = {
        dimension: {} for dimension in SLICE_DIMENSIONS
    }

    for slice_input in inputs:
        slice_values = _slice_values(slice_input, policy=policy)
        for dimension, slice_key in slice_values.items():
            dimension_accumulators = accumulators[dimension]
            accumulator = dimension_accumulators.setdefault(slice_key, _SliceAccumulator())
            _update_accumulator(accumulator, slice_input)

    return ReplaySliceReport(
        by_dimension={
            dimension: tuple(
                _finalize_accumulator(dimension, slice_key, accumulator)
                for slice_key, accumulator in sorted(
                    dimension_accumulators.items(),
                    key=lambda item: item[0],
                )
            )
            for dimension, dimension_accumulators in accumulators.items()
        }
    )


def _slice_values(
    slice_input: ReplaySliceInput,
    *,
    policy: ReplaySlicePolicy,
) -> dict[str, str]:
    snapshot = slice_input.labeled_snapshot.snapshot
    trade = slice_input.simulated_trade
    edge = slice_input.executable_edge

    signal_direction = edge.preferred_side or "none"
    selected_raw_edge = _selected_edge(edge, net=False)
    selected_net_edge = _selected_edge(edge, net=True)
    selected_spread = _selected_spread(snapshot, signal_direction=signal_direction)

    return {
        SECONDS_REMAINING_DIMENSION: _seconds_remaining_bucket(
            slice_input.seconds_remaining,
            policy=policy,
        ),
        VOLATILITY_DIMENSION: _volatility_regime(slice_input.sigma_eff, policy=policy),
        COMPOSITE_QUALITY_DIMENSION: (
            slice_input.composite_quality_state
            or _derive_composite_quality_state(snapshot)
        ),
        CHAINLINK_CONFIDENCE_DIMENSION: (
            slice_input.chainlink_confidence_state
            or _derive_chainlink_confidence_state(slice_input)
        ),
        RAW_EDGE_DIMENSION: _edge_bucket(selected_raw_edge, policy=policy),
        NET_EDGE_DIMENSION: _edge_bucket(selected_net_edge, policy=policy),
        SPREAD_DIMENSION: _spread_bucket(selected_spread, policy=policy),
        SIGNAL_DIRECTION_DIMENSION: trade.sim_trade_direction
        if trade.sim_trade_direction != SIM_NO_TRADE
        else signal_direction,
    }


def _update_accumulator(accumulator: _SliceAccumulator, slice_input: ReplaySliceInput) -> None:
    trade = slice_input.simulated_trade
    accumulator.row_count += 1
    accumulator.total_pnl += trade.sim_pnl
    if trade.sim_trade_direction == SIM_NO_TRADE:
        accumulator.no_trade_count += 1
        return

    accumulator.trade_count += 1
    accumulator.total_roi += trade.sim_roi
    if trade.sim_pnl > 0:
        accumulator.win_count += 1
    if trade.predicted_edge_net is not None:
        accumulator.total_predicted_edge += trade.predicted_edge_net
        accumulator.predicted_edge_count += 1
    if trade.realized_edge is not None:
        accumulator.total_realized_edge += trade.realized_edge
        accumulator.realized_edge_count += 1


def _finalize_accumulator(
    dimension: str,
    slice_key: str,
    accumulator: _SliceAccumulator,
) -> ReplaySliceResult:
    average_roi = (
        None
        if accumulator.trade_count == 0
        else accumulator.total_roi / Decimal(accumulator.trade_count)
    )
    average_predicted_edge = (
        None
        if accumulator.predicted_edge_count == 0
        else accumulator.total_predicted_edge / Decimal(accumulator.predicted_edge_count)
    )
    average_realized_edge = (
        None
        if accumulator.realized_edge_count == 0
        else accumulator.total_realized_edge / Decimal(accumulator.realized_edge_count)
    )
    return ReplaySliceResult(
        slice_dimension=dimension,
        slice_key=slice_key,
        row_count=accumulator.row_count,
        trade_count=accumulator.trade_count,
        no_trade_count=accumulator.no_trade_count,
        hit_rate=(
            Decimal("0")
            if accumulator.trade_count == 0
            else Decimal(accumulator.win_count) / Decimal(accumulator.trade_count)
        ),
        total_pnl=accumulator.total_pnl,
        average_pnl=accumulator.total_pnl / Decimal(accumulator.row_count),
        average_roi=average_roi,
        average_predicted_edge=average_predicted_edge,
        average_realized_edge=average_realized_edge,
        realized_minus_predicted_edge=(
            None
            if average_predicted_edge is None or average_realized_edge is None
            else average_realized_edge - average_predicted_edge
        ),
    )


def _seconds_remaining_bucket(
    seconds_remaining: int,
    *,
    policy: ReplaySlicePolicy,
) -> str:
    if seconds_remaining >= policy.early_window_seconds_min:
        return "early_window"
    if seconds_remaining >= policy.mid_window_seconds_min:
        return "mid_window"
    return "late_window"


def _volatility_regime(sigma_eff: Decimal | None, *, policy: ReplaySlicePolicy) -> str:
    if sigma_eff is None:
        return "unknown_vol"
    if sigma_eff < policy.low_vol_threshold:
        return "low_vol"
    if sigma_eff < policy.high_vol_threshold:
        return "mid_vol"
    return "high_vol"


def _derive_composite_quality_state(snapshot) -> str:
    if snapshot.snapshot_usable_flag:
        return "green"
    if snapshot.exchange_quality_usable_flag and snapshot.reference_complete_flag:
        return "yellow"
    return "red"


def _derive_chainlink_confidence_state(slice_input: ReplaySliceInput) -> str:
    snapshot = slice_input.labeled_snapshot.snapshot
    label = slice_input.labeled_snapshot.label
    if snapshot.chainlink_quality_usable_flag and label.label_status == "attached":
        return "high"
    if snapshot.chainlink_quality_usable_flag:
        return "medium"
    if snapshot.reference_complete_flag:
        return "low"
    return "none"


def _selected_edge(edge: ExecutableEdgeEstimate, *, net: bool) -> Decimal | None:
    if edge.preferred_side == "up":
        return edge.edge_up_net if net else edge.edge_up_raw
    if edge.preferred_side == "down":
        return edge.edge_down_net if net else edge.edge_down_raw

    candidates = [
        candidate
        for candidate in (
            edge.edge_up_net if net else edge.edge_up_raw,
            edge.edge_down_net if net else edge.edge_down_raw,
        )
        if candidate is not None
    ]
    return None if not candidates else max(candidates)


def _edge_bucket(value: Decimal | None, *, policy: ReplaySlicePolicy) -> str:
    if value is None:
        return "unknown_edge"
    if value <= 0:
        return "non_positive_edge"
    if value < policy.medium_edge_threshold:
        return "small_positive_edge"
    if value < policy.strong_edge_threshold:
        return "medium_positive_edge"
    return "large_positive_edge"


def _selected_spread(snapshot, *, signal_direction: str) -> Decimal | None:
    if signal_direction in {"up", "buy_up"}:
        return snapshot.market_spread_up_abs
    if signal_direction in {"down", "buy_down"}:
        return snapshot.market_spread_down_abs

    candidates = [
        spread
        for spread in (snapshot.market_spread_up_abs, snapshot.market_spread_down_abs)
        if spread is not None
    ]
    return None if not candidates else min(candidates)


def _spread_bucket(value: Decimal | None, *, policy: ReplaySlicePolicy) -> str:
    if value is None:
        return "unknown_spread"
    if value <= policy.tight_spread_threshold:
        return "tight_spread"
    if value <= policy.wide_spread_threshold:
        return "medium_spread"
    return "wide_spread"


__all__ = [
    "CHAINLINK_CONFIDENCE_DIMENSION",
    "COMPOSITE_QUALITY_DIMENSION",
    "DEFAULT_REPLAY_SLICE_POLICY",
    "NET_EDGE_DIMENSION",
    "RAW_EDGE_DIMENSION",
    "ReplaySliceInput",
    "ReplaySlicePolicy",
    "ReplaySliceReport",
    "ReplaySliceResult",
    "SECONDS_REMAINING_DIMENSION",
    "SIGNAL_DIRECTION_DIMENSION",
    "SLICE_DIMENSIONS",
    "SPREAD_DIMENSION",
    "VOLATILITY_DIMENSION",
    "generate_replay_slices",
]
