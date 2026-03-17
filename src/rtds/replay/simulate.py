"""Replay simulation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from rtds.core.units import to_decimal, validate_contract_price, validate_size
from rtds.features.executable_edge import ExecutableEdgeEstimate
from rtds.replay.attach_labels import LabeledSnapshotRecord

SIMULATION_VERSION = "0.1.0"

SIM_BUY_UP = "buy_up"
SIM_BUY_DOWN = "buy_down"
SIM_NO_TRADE = "no_trade"

SIM_OUTCOME_WIN = "win"
SIM_OUTCOME_LOSS = "loss"
SIM_OUTCOME_NO_TRADE = "no_trade"

NO_TRADE_LABEL_UNRESOLVED = "label_unresolved"
NO_TRADE_ENTRY_RULE_BLOCKED = "entry_rule_blocked"
NO_TRADE_EDGE_UNAVAILABLE = "edge_unavailable"


@dataclass(slots=True, frozen=True)
class FeeCurvePolicy:
    """Simple linear taker fee curve applied to entry notional."""

    taker_fee_rate: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "taker_fee_rate",
            validate_contract_price(self.taker_fee_rate, field_name="taker_fee_rate"),
        )

    def fee_for_entry_notional(self, entry_notional: Decimal) -> Decimal:
        """Return taker fee paid on entry notional."""

        return entry_notional * self.taker_fee_rate


@dataclass(slots=True, frozen=True)
class EntryRulePolicy:
    """Minimal phase-1 entry rules."""

    min_net_edge: Decimal = Decimal("0")
    target_trade_size_contracts: Decimal = Decimal("1")
    allow_buy_up: bool = True
    allow_buy_down: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "min_net_edge",
            to_decimal(self.min_net_edge, field_name="min_net_edge"),
        )
        object.__setattr__(
            self,
            "target_trade_size_contracts",
            to_decimal(
                validate_size(
                    self.target_trade_size_contracts,
                    field_name="target_trade_size_contracts",
                ),
                field_name="target_trade_size_contracts",
            ),
        )


DEFAULT_FEE_CURVE_POLICY = FeeCurvePolicy(taker_fee_rate=Decimal("0"))
DEFAULT_ENTRY_RULE_POLICY = EntryRulePolicy()


@dataclass(slots=True, frozen=True)
class ReplaySimulationInput:
    """One labeled snapshot plus its executable-edge estimate."""

    labeled_snapshot: LabeledSnapshotRecord
    executable_edge: ExecutableEdgeEstimate


@dataclass(slots=True, frozen=True)
class SimulatedTrade:
    """One simulated taker trade decision and realized outcome."""

    snapshot_id: str
    window_id: str
    polymarket_market_id: str
    sim_trade_direction: str
    sim_entry_price: Decimal | None
    sim_exit_price: Decimal | None
    sim_fee_paid: Decimal | None
    sim_slippage_paid: Decimal | None
    sim_pnl: Decimal
    sim_roi: Decimal
    sim_outcome: str
    predicted_edge_net: Decimal | None
    realized_edge: Decimal | None
    no_trade_reason: str | None
    simulation_version: str

    def __post_init__(self) -> None:
        for field_name in (
            "sim_entry_price",
            "sim_exit_price",
            "sim_fee_paid",
            "sim_slippage_paid",
            "predicted_edge_net",
            "realized_edge",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, to_decimal(value, field_name=field_name))
        object.__setattr__(self, "sim_pnl", to_decimal(self.sim_pnl, field_name="sim_pnl"))
        object.__setattr__(self, "sim_roi", to_decimal(self.sim_roi, field_name="sim_roi"))


@dataclass(slots=True, frozen=True)
class ReplaySimulationSummary:
    """Aggregate performance summary for one replay run."""

    snapshot_count: int
    trade_count: int
    hit_rate: Decimal
    total_pnl: Decimal
    average_predicted_edge: Decimal | None
    average_realized_edge: Decimal | None
    realized_minus_predicted_edge: Decimal | None
    simulation_version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "hit_rate", to_decimal(self.hit_rate, field_name="hit_rate"))
        object.__setattr__(self, "total_pnl", to_decimal(self.total_pnl, field_name="total_pnl"))
        for field_name in (
            "average_predicted_edge",
            "average_realized_edge",
            "realized_minus_predicted_edge",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, to_decimal(value, field_name=field_name))


@dataclass(slots=True, frozen=True)
class ReplaySimulationResult:
    """Simulated trade outputs plus aggregate summary."""

    trades: tuple[SimulatedTrade, ...]
    summary: ReplaySimulationSummary


def simulate_snapshot(
    simulation_input: ReplaySimulationInput,
    *,
    fee_curve: FeeCurvePolicy = DEFAULT_FEE_CURVE_POLICY,
    entry_rules: EntryRulePolicy = DEFAULT_ENTRY_RULE_POLICY,
) -> SimulatedTrade:
    """Run the phase-1 taker-only simulation for one labeled snapshot."""

    labeled_snapshot = simulation_input.labeled_snapshot
    snapshot = labeled_snapshot.snapshot
    label = labeled_snapshot.label
    edge = simulation_input.executable_edge

    trade_size = entry_rules.target_trade_size_contracts
    if label.label_status != "attached":
        return _no_trade(
            snapshot_id=snapshot.snapshot_id or "",
            window_id=snapshot.window_id,
            polymarket_market_id=snapshot.polymarket_market_id,
            no_trade_reason=NO_TRADE_LABEL_UNRESOLVED,
        )

    direction, predicted_edge_net, entry_price, slippage_per_contract = _select_trade(
        snapshot=snapshot,
        edge=edge,
        entry_rules=entry_rules,
    )
    if direction is None or predicted_edge_net is None or entry_price is None:
        return _no_trade(
            snapshot_id=snapshot.snapshot_id or "",
            window_id=snapshot.window_id,
            polymarket_market_id=snapshot.polymarket_market_id,
            no_trade_reason=edge.no_trade_reason or NO_TRADE_EDGE_UNAVAILABLE,
        )

    slippage_paid = slippage_per_contract * trade_size
    entry_notional = entry_price * trade_size
    fee_paid = fee_curve.fee_for_entry_notional(entry_notional)
    exit_price = _exit_price(direction=direction, resolved_up=label.resolved_up)
    gross_pnl = (exit_price - entry_price) * trade_size
    sim_pnl = gross_pnl - slippage_paid - fee_paid
    invested_capital = entry_notional + slippage_paid + fee_paid
    sim_roi = Decimal("0") if invested_capital == 0 else sim_pnl / invested_capital
    realized_edge = sim_pnl / trade_size

    return SimulatedTrade(
        snapshot_id=snapshot.snapshot_id or "",
        window_id=snapshot.window_id,
        polymarket_market_id=snapshot.polymarket_market_id,
        sim_trade_direction=direction,
        sim_entry_price=entry_price,
        sim_exit_price=exit_price,
        sim_fee_paid=fee_paid,
        sim_slippage_paid=slippage_paid,
        sim_pnl=sim_pnl,
        sim_roi=sim_roi,
        sim_outcome=SIM_OUTCOME_WIN if sim_pnl > 0 else SIM_OUTCOME_LOSS,
        predicted_edge_net=predicted_edge_net,
        realized_edge=realized_edge,
        no_trade_reason=None,
        simulation_version=SIMULATION_VERSION,
    )


def simulate_replay(
    simulation_inputs: Iterable[ReplaySimulationInput],
    *,
    fee_curve: FeeCurvePolicy = DEFAULT_FEE_CURVE_POLICY,
    entry_rules: EntryRulePolicy = DEFAULT_ENTRY_RULE_POLICY,
) -> ReplaySimulationResult:
    """Run the phase-1 taker-only simulator over a batch of labeled snapshots."""

    trades = tuple(
        simulate_snapshot(
            simulation_input,
            fee_curve=fee_curve,
            entry_rules=entry_rules,
        )
        for simulation_input in simulation_inputs
    )
    return ReplaySimulationResult(
        trades=trades,
        summary=summarize_simulated_trades(trades),
    )


def summarize_simulated_trades(trades: Iterable[SimulatedTrade]) -> ReplaySimulationSummary:
    """Aggregate the canonical replay summary from an existing trade batch."""

    trade_list = tuple(trades)
    traded = [trade for trade in trade_list if trade.sim_trade_direction != SIM_NO_TRADE]
    winning = [trade for trade in traded if trade.sim_pnl > 0]
    predicted_edges = [
        trade.predicted_edge_net
        for trade in traded
        if trade.predicted_edge_net is not None
    ]
    realized_edges = [trade.realized_edge for trade in traded if trade.realized_edge is not None]

    average_predicted_edge = (
        None if not predicted_edges else sum(predicted_edges) / Decimal(len(predicted_edges))
    )
    average_realized_edge = (
        None if not realized_edges else sum(realized_edges) / Decimal(len(realized_edges))
    )

    return ReplaySimulationSummary(
        snapshot_count=len(trade_list),
        trade_count=len(traded),
        hit_rate=Decimal("0") if not traded else Decimal(len(winning)) / Decimal(len(traded)),
        total_pnl=sum((trade.sim_pnl for trade in trade_list), start=Decimal("0")),
        average_predicted_edge=average_predicted_edge,
        average_realized_edge=average_realized_edge,
        realized_minus_predicted_edge=(
            None
            if average_predicted_edge is None or average_realized_edge is None
            else average_realized_edge - average_predicted_edge
        ),
        simulation_version=SIMULATION_VERSION,
    )


def _select_trade(
    *,
    snapshot,
    edge: ExecutableEdgeEstimate,
    entry_rules: EntryRulePolicy,
) -> tuple[str | None, Decimal | None, Decimal | None, Decimal | None]:
    if edge.preferred_side == "up" and entry_rules.allow_buy_up:
        if edge.edge_up_net is None or edge.edge_up_net <= entry_rules.min_net_edge:
            return None, None, None, None
        return SIM_BUY_UP, edge.edge_up_net, snapshot.up_ask, edge.slippage_estimate_up
    if edge.preferred_side == "down" and entry_rules.allow_buy_down:
        if edge.edge_down_net is None or edge.edge_down_net <= entry_rules.min_net_edge:
            return None, None, None, None
        return SIM_BUY_DOWN, edge.edge_down_net, snapshot.down_ask, edge.slippage_estimate_down
    return None, None, None, None


def _exit_price(*, direction: str, resolved_up: bool | None) -> Decimal:
    if direction == SIM_BUY_UP:
        return Decimal("1") if resolved_up is True else Decimal("0")
    if direction == SIM_BUY_DOWN:
        return Decimal("1") if resolved_up is False else Decimal("0")
    raise ValueError("direction must be a trade direction")


def _no_trade(
    *,
    snapshot_id: str,
    window_id: str,
    polymarket_market_id: str,
    no_trade_reason: str,
) -> SimulatedTrade:
    return SimulatedTrade(
        snapshot_id=snapshot_id,
        window_id=window_id,
        polymarket_market_id=polymarket_market_id,
        sim_trade_direction=SIM_NO_TRADE,
        sim_entry_price=None,
        sim_exit_price=None,
        sim_fee_paid=None,
        sim_slippage_paid=None,
        sim_pnl=Decimal("0"),
        sim_roi=Decimal("0"),
        sim_outcome=SIM_OUTCOME_NO_TRADE,
        predicted_edge_net=None,
        realized_edge=None,
        no_trade_reason=no_trade_reason,
        simulation_version=SIMULATION_VERSION,
    )


__all__ = [
    "DEFAULT_ENTRY_RULE_POLICY",
    "DEFAULT_FEE_CURVE_POLICY",
    "NO_TRADE_EDGE_UNAVAILABLE",
    "NO_TRADE_ENTRY_RULE_BLOCKED",
    "NO_TRADE_LABEL_UNRESOLVED",
    "EntryRulePolicy",
    "FeeCurvePolicy",
    "ReplaySimulationInput",
    "ReplaySimulationResult",
    "ReplaySimulationSummary",
    "SIM_BUY_DOWN",
    "SIM_BUY_UP",
    "SIM_NO_TRADE",
    "SIM_OUTCOME_LOSS",
    "SIM_OUTCOME_NO_TRADE",
    "SIM_OUTCOME_WIN",
    "SIMULATION_VERSION",
    "SimulatedTrade",
    "simulate_replay",
    "simulate_snapshot",
    "summarize_simulated_trades",
]
