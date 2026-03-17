"""Policy-stack replay comparison with window-aware inclusion rules."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable, Mapping, Sequence

from rtds.features.executable_edge import EdgeCostPolicy, compute_executable_edge
from rtds.replay.regime_compare import WindowQualityRow
from rtds.replay.simulate import (
    SIM_NO_TRADE,
    SIM_OUTCOME_NO_TRADE,
    EntryRulePolicy,
    ReplaySimulationInput,
    SimulatedTrade,
    simulate_snapshot,
    summarize_simulated_trades,
)
from rtds.replay.slices import (
    DEFAULT_REPLAY_SLICE_POLICY,
    ReplaySliceInput,
    ReplaySlicePolicy,
    classify_replay_slice_input,
)
from rtds.schemas.normalized import PolymarketQuote
from rtds.storage.writer import serialize_value


@dataclass(slots=True, frozen=True)
class PolicyRule:
    """One replay inclusion rule with its own execution assumptions."""

    policy_name: str
    policy_role: str
    window_quality_regime: str
    min_net_edge: Decimal
    target_trade_size_contracts: Decimal
    slippage_multiplier_assumption: Decimal
    max_selected_spread_abs: Decimal | None
    minimum_window_quote_coverage_ratio: float | None
    required_seconds_remaining_buckets: tuple[str, ...]
    required_volatility_regimes: tuple[str, ...]
    required_spread_buckets: tuple[str, ...]
    required_raw_edge_buckets: tuple[str, ...]
    required_net_edge_buckets: tuple[str, ...]
    required_chainlink_confidence_states: tuple[str, ...]
    status: str
    notes: str


@dataclass(slots=True, frozen=True)
class PolicyStack:
    """Ordered replay policy stack."""

    stack_name: str
    stack_role: str
    policy_paths: tuple[str, ...]
    policies: tuple[PolicyRule, ...]


@dataclass(slots=True, frozen=True)
class PolicyStackResult:
    """One evaluated policy stack on one capture session."""

    stack_name: str
    stack_role: str
    snapshot_count: int
    window_count: int
    included_window_verdict_counts: dict[str, int]
    matched_policy_counts: dict[str, int]
    trade_count: int
    hit_rate: Decimal
    average_selected_raw_edge: Decimal | None
    average_selected_net_edge: Decimal | None
    total_pnl: Decimal
    pnl_per_window: Decimal | None
    pnl_per_1000_snapshots: Decimal | None
    pnl_per_100_trades: Decimal | None
    average_roi: Decimal | None


def load_policy_rule(path: str | Path) -> PolicyRule:
    """Load one flat policy yaml with optional JSON-list values."""

    payload = _load_flat_yaml(Path(path))
    return PolicyRule(
        policy_name=str(payload["policy_name"]),
        policy_role=str(payload["policy_role"]),
        window_quality_regime=str(payload["window_quality_regime"]),
        min_net_edge=Decimal(str(payload.get("min_net_edge", "0"))),
        target_trade_size_contracts=Decimal(str(payload.get("target_trade_size_contracts", "1"))),
        slippage_multiplier_assumption=Decimal(
            str(payload.get("slippage_multiplier_assumption", "1"))
        ),
        max_selected_spread_abs=_optional_decimal(payload.get("max_selected_spread_abs")),
        minimum_window_quote_coverage_ratio=_optional_float(
            payload.get("minimum_window_quote_coverage_ratio")
        ),
        required_seconds_remaining_buckets=_tuple_of_str(
            payload.get("required_seconds_remaining_buckets")
        ),
        required_volatility_regimes=_tuple_of_str(payload.get("required_volatility_regimes")),
        required_spread_buckets=_tuple_of_str(payload.get("required_spread_buckets")),
        required_raw_edge_buckets=_tuple_of_str(payload.get("required_raw_edge_buckets")),
        required_net_edge_buckets=_tuple_of_str(payload.get("required_net_edge_buckets")),
        required_chainlink_confidence_states=_tuple_of_str(
            payload.get("required_chainlink_confidence_states")
        ),
        status=str(payload.get("status", "")),
        notes=str(payload.get("notes", "")),
    )


def load_policy_stack(path: str | Path) -> PolicyStack:
    """Load one stack config that points at ordered policy configs."""

    config_path = Path(path)
    payload = _load_flat_yaml(config_path)
    policy_paths_raw = _tuple_of_str(payload.get("policy_paths"))
    policy_paths = tuple(
        str((config_path.parent.parent / policy_path).resolve().relative_to(Path.cwd()))
        if not Path(policy_path).is_absolute()
        and not policy_path.startswith("configs/")
        else policy_path
        for policy_path in policy_paths_raw
    )
    policies = tuple(load_policy_rule(policy_path) for policy_path in policy_paths)
    return PolicyStack(
        stack_name=str(payload["stack_name"]),
        stack_role=str(payload["stack_role"]),
        policy_paths=policy_paths,
        policies=policies,
    )


def build_policy_stack_result(
    evaluation_rows: Sequence[Any],
    *,
    window_quality_by_window: Mapping[str, WindowQualityRow],
    replay_config: Any,
    stack: PolicyStack,
    slice_policy: ReplaySlicePolicy = DEFAULT_REPLAY_SLICE_POLICY,
) -> PolicyStackResult:
    """Apply one ordered stack to replay rows and summarize the result."""

    matched_rows: list[Any] = []
    included_window_ids: set[str] = set()
    included_window_verdict_counts: Counter[str] = Counter()
    matched_policy_counts: Counter[str] = Counter()

    for row in evaluation_rows:
        match = _match_policy(
            row,
            window_quality_by_window=window_quality_by_window,
            policies=stack.policies,
            slice_policy=slice_policy,
        )
        if match is None:
            continue
        policy, slice_values = match
        adjusted_row = _apply_policy_to_row(
            row,
            replay_config=replay_config,
            policy=policy,
        )
        matched_rows.append(adjusted_row)
        included_window_ids.add(row.snapshot.window_id)
        included_window_verdict_counts[policy.window_quality_regime] += 1
        matched_policy_counts[policy.policy_name] += 1

    simulation_summary = summarize_simulated_trades(row.simulated_trade for row in matched_rows)
    selected_raw_edges = [
        value for row in matched_rows if (value := _selected_edge(row.edge, net=False)) is not None
    ]
    selected_net_edges = [
        value for row in matched_rows if (value := _selected_edge(row.edge, net=True)) is not None
    ]

    return PolicyStackResult(
        stack_name=stack.stack_name,
        stack_role=stack.stack_role,
        snapshot_count=len(matched_rows),
        window_count=len(included_window_ids),
        included_window_verdict_counts=dict(sorted(included_window_verdict_counts.items())),
        matched_policy_counts=dict(sorted(matched_policy_counts.items())),
        trade_count=simulation_summary.trade_count,
        hit_rate=simulation_summary.hit_rate,
        average_selected_raw_edge=_average_decimal(selected_raw_edges),
        average_selected_net_edge=_average_decimal(selected_net_edges),
        total_pnl=simulation_summary.total_pnl,
        pnl_per_window=_normalized_pnl(
            simulation_summary.total_pnl,
            count=len(included_window_ids),
            scale=Decimal("1"),
        ),
        pnl_per_1000_snapshots=_normalized_pnl(
            simulation_summary.total_pnl,
            count=len(matched_rows),
            scale=Decimal("1000"),
        ),
        pnl_per_100_trades=_normalized_pnl(
            simulation_summary.total_pnl,
            count=simulation_summary.trade_count,
            scale=Decimal("100"),
        ),
        average_roi=_average_trade_roi(row.simulated_trade for row in matched_rows),
    )


def policy_stack_result_to_dict(result: PolicyStackResult) -> dict[str, object]:
    return {
        "stack_name": result.stack_name,
        "stack_role": result.stack_role,
        "snapshot_count": result.snapshot_count,
        "window_count": result.window_count,
        "included_window_verdict_counts": result.included_window_verdict_counts,
        "matched_policy_counts": result.matched_policy_counts,
        "trade_count": result.trade_count,
        "hit_rate": serialize_value(result.hit_rate),
        "average_selected_raw_edge": serialize_value(result.average_selected_raw_edge),
        "average_selected_net_edge": serialize_value(result.average_selected_net_edge),
        "total_pnl": serialize_value(result.total_pnl),
        "pnl_per_window": serialize_value(result.pnl_per_window),
        "pnl_per_1000_snapshots": serialize_value(result.pnl_per_1000_snapshots),
        "pnl_per_100_trades": serialize_value(result.pnl_per_100_trades),
        "average_roi": serialize_value(result.average_roi),
    }


def render_policy_stack_report(
    results: Sequence[PolicyStackResult],
    *,
    trade_date: str,
    session_id: str,
    admission_summary_path: str | Path,
) -> str:
    lines = [
        f"# Policy Stack Comparison — {trade_date}",
        "",
        "## Run",
        f"- session_id: `{session_id}`",
        f"- admission_summary: `{admission_summary_path}`",
        "",
        "## Stacks",
    ]
    for result in results:
        lines.extend(
            [
                f"### {result.stack_name}",
                f"- stack_role: `{result.stack_role}`",
                f"- snapshots: {result.snapshot_count}",
                f"- windows: {result.window_count}",
                f"- included_window_verdict_counts: {result.included_window_verdict_counts}",
                f"- matched_policy_counts: {result.matched_policy_counts}",
                f"- trade_count: {result.trade_count}",
                f"- hit_rate: {result.hit_rate}",
                f"- average_selected_net_edge: {result.average_selected_net_edge}",
                f"- total_pnl: {result.total_pnl}",
                f"- average_roi: {result.average_roi}",
                f"- pnl_per_window: {result.pnl_per_window}",
                f"- pnl_per_1000_snapshots: {result.pnl_per_1000_snapshots}",
                f"- pnl_per_100_trades: {result.pnl_per_100_trades}",
            ]
        )
    return "\n".join(lines) + "\n"


def _match_policy(
    row: Any,
    *,
    window_quality_by_window: Mapping[str, WindowQualityRow],
    policies: Sequence[PolicyRule],
    slice_policy: ReplaySlicePolicy,
) -> tuple[PolicyRule, dict[str, str]] | None:
    window_quality = window_quality_by_window.get(row.snapshot.window_id)
    if window_quality is None:
        return None
    slice_values = classify_replay_slice_input(
        ReplaySliceInput(
            labeled_snapshot=row.labeled_snapshot,
            executable_edge=row.edge,
            simulated_trade=row.simulated_trade,
            seconds_remaining=row.seconds_remaining,
            sigma_eff=row.volatility.sigma_eff,
        ),
        policy=slice_policy,
    )
    for policy in policies:
        if window_quality.window_verdict != policy.window_quality_regime:
            continue
        if (
            policy.minimum_window_quote_coverage_ratio is not None
            and (
                window_quality.quote_coverage_ratio is None
                or window_quality.quote_coverage_ratio < policy.minimum_window_quote_coverage_ratio
            )
        ):
            continue
        if policy.required_seconds_remaining_buckets and (
            slice_values["seconds_remaining_bucket"]
            not in policy.required_seconds_remaining_buckets
        ):
            continue
        if policy.required_volatility_regimes and (
            slice_values["volatility_regime"] not in policy.required_volatility_regimes
        ):
            continue
        if policy.required_spread_buckets and (
            slice_values["spread_bucket"] not in policy.required_spread_buckets
        ):
            continue
        if policy.required_raw_edge_buckets and (
            slice_values["raw_edge_bucket"] not in policy.required_raw_edge_buckets
        ):
            continue
        if policy.required_net_edge_buckets and (
            slice_values["net_edge_bucket"] not in policy.required_net_edge_buckets
        ):
            continue
        if policy.required_chainlink_confidence_states and (
            slice_values["chainlink_confidence_state"]
            not in policy.required_chainlink_confidence_states
        ):
            continue
        return policy, slice_values
    return None


def _apply_policy_to_row(
    row: Any,
    *,
    replay_config: Any,
    policy: PolicyRule,
) -> Any:
    cost_policy = EdgeCostPolicy(
        fee_rate_estimate=replay_config.edge_cost_policy.fee_rate_estimate,
        slippage_estimate_up=(
            replay_config.edge_cost_policy.slippage_estimate_up
            * policy.slippage_multiplier_assumption
        ),
        slippage_estimate_down=(
            replay_config.edge_cost_policy.slippage_estimate_down
            * policy.slippage_multiplier_assumption
        ),
        model_error_buffer=replay_config.edge_cost_policy.model_error_buffer,
    )
    quote = _snapshot_to_polymarket_quote(row.snapshot)
    edge = compute_executable_edge(
        fair_value_base=row.fair_value.fair_value_base,
        polymarket_quote=quote,
        cost_policy=cost_policy,
    )
    simulated_trade = simulate_snapshot(
        ReplaySimulationInput(
            labeled_snapshot=row.labeled_snapshot,
            executable_edge=edge,
        ),
        fee_curve=replay_config.fee_curve,
        entry_rules=EntryRulePolicy(
            min_net_edge=policy.min_net_edge,
            target_trade_size_contracts=policy.target_trade_size_contracts,
            allow_buy_up=replay_config.entry_rules.allow_buy_up,
            allow_buy_down=replay_config.entry_rules.allow_buy_down,
        ),
    )
    if (
        policy.max_selected_spread_abs is not None
        and simulated_trade.sim_trade_direction != SIM_NO_TRADE
    ):
        selected_spread = _selected_spread(row.snapshot, preferred_side=edge.preferred_side)
        if selected_spread is None or selected_spread > policy.max_selected_spread_abs:
            simulated_trade = SimulatedTrade(
                snapshot_id=row.snapshot.snapshot_id or "",
                window_id=row.snapshot.window_id,
                polymarket_market_id=row.snapshot.polymarket_market_id,
                sim_trade_direction=SIM_NO_TRADE,
                sim_entry_price=None,
                sim_exit_price=None,
                sim_fee_paid=None,
                sim_slippage_paid=None,
                sim_pnl=Decimal("0"),
                sim_roi=Decimal("0"),
                sim_outcome=SIM_OUTCOME_NO_TRADE,
                predicted_edge_net=simulated_trade.predicted_edge_net,
                realized_edge=None,
                no_trade_reason="spread_cap_exceeded",
                simulation_version="0.1.0",
            )
    return SimpleNamespace(
        snapshot=row.snapshot,
        labeled_snapshot=row.labeled_snapshot,
        edge=edge,
        simulated_trade=simulated_trade,
        seconds_remaining=row.seconds_remaining,
        volatility=row.volatility,
        fair_value=row.fair_value,
    )


def _selected_spread(snapshot: Any, *, preferred_side: str | None) -> Decimal | None:
    if preferred_side == "up":
        return snapshot.market_spread_up_abs
    if preferred_side == "down":
        return snapshot.market_spread_down_abs
    candidates = [
        value
        for value in (snapshot.market_spread_up_abs, snapshot.market_spread_down_abs)
        if value is not None
    ]
    return None if not candidates else min(candidates)


def _snapshot_to_polymarket_quote(snapshot: Any) -> PolymarketQuote | None:
    if snapshot.polymarket_quote_event_ts is None:
        return None
    if (
        snapshot.up_bid is None
        or snapshot.up_ask is None
        or snapshot.down_bid is None
        or snapshot.down_ask is None
    ):
        return None
    return PolymarketQuote(
        venue_id="polymarket",
        market_id=snapshot.polymarket_market_id,
        asset_id=snapshot.asset_id,
        event_ts=snapshot.polymarket_quote_event_ts,
        recv_ts=snapshot.polymarket_quote_recv_ts or snapshot.polymarket_quote_event_ts,
        proc_ts=snapshot.created_ts,
        up_bid=snapshot.up_bid,
        up_ask=snapshot.up_ask,
        down_bid=snapshot.down_bid,
        down_ask=snapshot.down_ask,
        up_bid_size_contracts=snapshot.up_bid_size_contracts or Decimal("0"),
        up_ask_size_contracts=snapshot.up_ask_size_contracts or Decimal("0"),
        down_bid_size_contracts=snapshot.down_bid_size_contracts or Decimal("0"),
        down_ask_size_contracts=snapshot.down_ask_size_contracts or Decimal("0"),
        raw_event_id=f"snapshot:{snapshot.snapshot_id}",
        normalizer_version="0.1.0",
        schema_version="0.1.0",
        created_ts=snapshot.created_ts,
        market_mid_up=snapshot.market_mid_up,
        market_mid_down=snapshot.market_mid_down,
        market_spread_up_abs=snapshot.market_spread_up_abs,
        market_spread_down_abs=snapshot.market_spread_down_abs,
        last_trade_price=snapshot.last_trade_price,
        last_trade_size_contracts=snapshot.last_trade_size_contracts,
    )


def _average_trade_roi(trades: Iterable[SimulatedTrade]) -> Decimal | None:
    traded = [trade.sim_roi for trade in trades if trade.sim_trade_direction != SIM_NO_TRADE]
    return _average_decimal(traded)


def _average_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, start=Decimal("0")) / Decimal(len(values))


def _normalized_pnl(total_pnl: Decimal, *, count: int, scale: Decimal) -> Decimal | None:
    if count <= 0:
        return None
    return (total_pnl / Decimal(count)) * scale


def _selected_edge(edge: Any, *, net: bool) -> Decimal | None:
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


def _load_flat_yaml(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        payload[key.strip()] = _parse_scalar(value.strip())
    return payload


def _parse_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    if value.startswith("[") and value.endswith("]"):
        return json.loads(value)
    try:
        if "." in value:
            return Decimal(value)
        return int(value)
    except Exception:
        return value


def _tuple_of_str(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, list):
        return tuple(str(item) for item in value)
    if isinstance(value, tuple):
        return tuple(str(item) for item in value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()
        return (stripped,)
    return (str(value),)


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return Decimal(str(value))


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return float(value)


__all__ = [
    "PolicyRule",
    "PolicyStack",
    "PolicyStackResult",
    "build_policy_stack_result",
    "load_policy_rule",
    "load_policy_stack",
    "policy_stack_result_to_dict",
    "render_policy_stack_report",
]
