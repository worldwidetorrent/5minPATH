"""Apply the frozen stage-1 good-only calibrator to baseline-only replay rows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

from rtds.features.executable_edge import (
    EdgeCostPolicy,
    ExecutableEdgeEstimate,
    compute_executable_edge,
)
from rtds.replay.good_only_calibration import (
    GoodOnlyCalibrationConfig,
    classify_calibration_bucket,
    load_good_only_calibration_config,
)
from rtds.replay.simulate import (
    EntryRulePolicy,
    FeeCurvePolicy,
    ReplaySimulationInput,
    SimulatedTrade,
    simulate_snapshot,
    summarize_simulated_trades,
)
from rtds.schemas.normalized import PolymarketQuote
from rtds.storage.writer import serialize_value


@dataclass(slots=True, frozen=True)
class FrozenCalibrationBucket:
    """One frozen calibration bucket with support and applied correction."""

    bucket_name: str
    support_flag: str
    provisional_calibrated_f: Decimal | None


@dataclass(slots=True, frozen=True)
class FrozenCalibrationRuntime:
    """Loaded stage-1 good-only calibration contract plus frozen bucket results."""

    calibration_id: str
    config_path: str
    summary_path: str
    config: GoodOnlyCalibrationConfig
    buckets_by_name: dict[str, FrozenCalibrationBucket]


@dataclass(slots=True, frozen=True)
class AppliedCalibration:
    """One fair-value row after frozen stage-1 calibration policy is applied."""

    raw_f: Decimal
    calibrated_f: Decimal
    bucket_name: str
    support_flag: str
    calibration_applied: bool


@dataclass(slots=True, frozen=True)
class CalibratedBaselineRow:
    """One baseline-only replay row with raw and calibrated projections."""

    session_label: str
    session_id: str
    capture_date: str
    snapshot_id: str
    snapshot_ts: Any
    window_id: str
    polymarket_market_id: str
    raw_f: Decimal
    calibrated_f: Decimal
    calibration_bucket: str
    calibration_support_flag: str
    calibration_applied: bool
    raw_preferred_side: str | None
    calibrated_preferred_side: str | None
    raw_selected_raw_edge: Decimal | None
    raw_selected_net_edge: Decimal | None
    calibrated_selected_raw_edge: Decimal | None
    calibrated_selected_net_edge: Decimal | None
    raw_trade_direction: str
    calibrated_trade_direction: str
    raw_pnl: Decimal
    calibrated_pnl: Decimal
    raw_roi: Decimal
    calibrated_roi: Decimal
    raw_no_trade_reason: str | None
    calibrated_no_trade_reason: str | None


@dataclass(slots=True, frozen=True)
class BaselineScenarioSummary:
    """Summary metrics for one baseline-only replay scenario."""

    scenario_name: str
    snapshot_count: int
    window_count: int
    trade_count: int
    hit_rate: Decimal
    average_selected_raw_edge: Decimal | None
    average_selected_net_edge: Decimal | None
    total_pnl: Decimal
    average_roi: Decimal | None
    pnl_per_window: Decimal | None
    pnl_per_1000_snapshots: Decimal | None
    pnl_per_100_trades: Decimal | None


@dataclass(slots=True, frozen=True)
class CalibratedBaselineSessionComparison:
    """Raw-vs-calibrated baseline-only comparison for one pinned session."""

    session_label: str
    session_id: str
    capture_date: str
    raw_summary: BaselineScenarioSummary
    calibrated_summary: BaselineScenarioSummary
    row_count: int
    calibration_bucket_counts: dict[str, int]
    calibration_support_flag_counts: dict[str, int]
    calibration_applied_row_count: int
    delta_trade_count: int
    delta_total_pnl: Decimal
    delta_average_roi: Decimal | None
    delta_average_selected_net_edge: Decimal | None


@dataclass(slots=True, frozen=True)
class CalibratedBaselineComparison:
    """Cross-session raw-vs-calibrated baseline-only replay comparison."""

    analysis_id: str
    description: str
    cross_horizon_manifest_path: str
    replay_comparison_config_path: str
    calibration_config_path: str
    calibration_summary_path: str
    baseline_stack_path: str
    admission_semantics_version: str
    policy_universe: str
    oracle_source: str
    sessions: tuple[CalibratedBaselineSessionComparison, ...]


def load_frozen_calibration_runtime(
    *,
    config_path: str | Path,
    summary_path: str | Path,
) -> FrozenCalibrationRuntime:
    """Load and validate the frozen stage-1 calibration contract."""

    config = load_good_only_calibration_config(config_path)
    payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    if str(payload["calibration_id"]) != config.calibration_id:
        raise ValueError("calibration summary does not match calibration config id")

    summary_buckets = {str(item["bucket_name"]): item for item in payload["buckets"]}
    buckets_by_name: dict[str, FrozenCalibrationBucket] = {}
    for bucket_definition in config.bucket_definitions:
        summary_bucket = summary_buckets.get(bucket_definition.bucket_name)
        if summary_bucket is None:
            raise ValueError(
                f"calibration summary missing bucket {bucket_definition.bucket_name}"
            )
        lower_bound = Decimal(str(summary_bucket["lower_bound_inclusive"]))
        upper_bound = Decimal(str(summary_bucket["upper_bound"]))
        upper_inclusive = bool(summary_bucket["upper_bound_inclusive"])
        if (
            lower_bound != bucket_definition.lower_bound_inclusive
            or upper_bound != bucket_definition.upper_bound
            or upper_inclusive != bucket_definition.upper_bound_inclusive
        ):
            raise ValueError(
                f"calibration summary bucket bounds drifted for {bucket_definition.bucket_name}"
            )
        calibrated = summary_bucket.get("provisional_calibrated_f")
        buckets_by_name[bucket_definition.bucket_name] = FrozenCalibrationBucket(
            bucket_name=bucket_definition.bucket_name,
            support_flag=str(summary_bucket["support_flag"]),
            provisional_calibrated_f=(
                None if calibrated is None else Decimal(str(calibrated))
            ),
        )
    return FrozenCalibrationRuntime(
        calibration_id=config.calibration_id,
        config_path=str(config_path),
        summary_path=str(summary_path),
        config=config,
        buckets_by_name=buckets_by_name,
    )


def apply_frozen_stage1_calibration(
    fair_value_base: Decimal,
    *,
    runtime: FrozenCalibrationRuntime,
) -> AppliedCalibration:
    """Apply stage-1 calibration only where frozen support is sufficient."""

    bucket = classify_calibration_bucket(fair_value_base, config=runtime.config)
    if bucket is None:
        raise ValueError(f"no calibration bucket found for fair value {fair_value_base}")
    bucket_result = runtime.buckets_by_name[bucket.bucket_name]
    if (
        bucket_result.support_flag == "sufficient"
        and bucket_result.provisional_calibrated_f is not None
    ):
        return AppliedCalibration(
            raw_f=fair_value_base,
            calibrated_f=bucket_result.provisional_calibrated_f,
            bucket_name=bucket.bucket_name,
            support_flag=bucket_result.support_flag,
            calibration_applied=True,
        )
    return AppliedCalibration(
        raw_f=fair_value_base,
        calibrated_f=fair_value_base,
        bucket_name=bucket.bucket_name,
        support_flag=bucket_result.support_flag,
        calibration_applied=False,
    )


def build_calibrated_baseline_session_comparison(
    evaluation_rows: Sequence[Any],
    *,
    session_label: str,
    session_id: str,
    capture_date: str,
    replay_config: Any,
    runtime: FrozenCalibrationRuntime,
    entry_rules: EntryRulePolicy | None = None,
    fee_curve: FeeCurvePolicy | None = None,
) -> tuple[CalibratedBaselineSessionComparison, tuple[CalibratedBaselineRow, ...]]:
    """Re-evaluate baseline-only rows with raw and calibrated fair values."""

    baseline_entry_rules = entry_rules or EntryRulePolicy(
        min_net_edge=Decimal("0"),
        target_trade_size_contracts=Decimal("1"),
        allow_buy_up=replay_config.entry_rules.allow_buy_up,
        allow_buy_down=replay_config.entry_rules.allow_buy_down,
    )
    baseline_fee_curve = fee_curve or replay_config.fee_curve
    baseline_cost_policy = EdgeCostPolicy(
        fee_rate_estimate=replay_config.edge_cost_policy.fee_rate_estimate,
        slippage_estimate_up=replay_config.edge_cost_policy.slippage_estimate_up,
        slippage_estimate_down=replay_config.edge_cost_policy.slippage_estimate_down,
        model_error_buffer=replay_config.edge_cost_policy.model_error_buffer,
    )

    rows: list[CalibratedBaselineRow] = []
    raw_edges: list[ExecutableEdgeEstimate] = []
    calibrated_edges: list[ExecutableEdgeEstimate] = []
    raw_trades: list[SimulatedTrade] = []
    calibrated_trades: list[SimulatedTrade] = []

    for row in evaluation_rows:
        raw_f = row.fair_value.fair_value_base
        if raw_f is None:
            continue
        quote = _snapshot_to_polymarket_quote(row.snapshot)
        calibration = apply_frozen_stage1_calibration(raw_f, runtime=runtime)
        raw_edge = compute_executable_edge(
            fair_value_base=raw_f,
            polymarket_quote=quote,
            cost_policy=baseline_cost_policy,
        )
        calibrated_edge = compute_executable_edge(
            fair_value_base=calibration.calibrated_f,
            polymarket_quote=quote,
            cost_policy=baseline_cost_policy,
        )
        raw_trade = simulate_snapshot(
            ReplaySimulationInput(
                labeled_snapshot=row.labeled_snapshot,
                executable_edge=raw_edge,
            ),
            fee_curve=baseline_fee_curve,
            entry_rules=baseline_entry_rules,
        )
        calibrated_trade = simulate_snapshot(
            ReplaySimulationInput(
                labeled_snapshot=row.labeled_snapshot,
                executable_edge=calibrated_edge,
            ),
            fee_curve=baseline_fee_curve,
            entry_rules=baseline_entry_rules,
        )
        rows.append(
            CalibratedBaselineRow(
                session_label=session_label,
                session_id=session_id,
                capture_date=capture_date,
                snapshot_id=row.snapshot.snapshot_id or "",
                snapshot_ts=row.snapshot.snapshot_ts,
                window_id=row.snapshot.window_id,
                polymarket_market_id=row.snapshot.polymarket_market_id,
                raw_f=raw_f,
                calibrated_f=calibration.calibrated_f,
                calibration_bucket=calibration.bucket_name,
                calibration_support_flag=calibration.support_flag,
                calibration_applied=calibration.calibration_applied,
                raw_preferred_side=raw_edge.preferred_side,
                calibrated_preferred_side=calibrated_edge.preferred_side,
                raw_selected_raw_edge=_selected_edge(raw_edge, net=False),
                raw_selected_net_edge=_selected_edge(raw_edge, net=True),
                calibrated_selected_raw_edge=_selected_edge(calibrated_edge, net=False),
                calibrated_selected_net_edge=_selected_edge(calibrated_edge, net=True),
                raw_trade_direction=raw_trade.sim_trade_direction,
                calibrated_trade_direction=calibrated_trade.sim_trade_direction,
                raw_pnl=raw_trade.sim_pnl,
                calibrated_pnl=calibrated_trade.sim_pnl,
                raw_roi=raw_trade.sim_roi,
                calibrated_roi=calibrated_trade.sim_roi,
                raw_no_trade_reason=raw_trade.no_trade_reason,
                calibrated_no_trade_reason=calibrated_trade.no_trade_reason,
            )
        )
        raw_edges.append(raw_edge)
        calibrated_edges.append(calibrated_edge)
        raw_trades.append(raw_trade)
        calibrated_trades.append(calibrated_trade)

    raw_summary = _build_scenario_summary(
        "raw_f",
        scenario_edges=raw_edges,
        trades=raw_trades,
        rows=rows,
        selected_raw_edge_attr="raw_selected_raw_edge",
        selected_net_edge_attr="raw_selected_net_edge",
    )
    calibrated_summary = _build_scenario_summary(
        "calibrated_f",
        scenario_edges=calibrated_edges,
        trades=calibrated_trades,
        rows=rows,
        selected_raw_edge_attr="calibrated_selected_raw_edge",
        selected_net_edge_attr="calibrated_selected_net_edge",
    )

    bucket_counts = _count_by(rows, "calibration_bucket")
    support_flag_counts = _count_by(rows, "calibration_support_flag")
    comparison = CalibratedBaselineSessionComparison(
        session_label=session_label,
        session_id=session_id,
        capture_date=capture_date,
        raw_summary=raw_summary,
        calibrated_summary=calibrated_summary,
        row_count=len(rows),
        calibration_bucket_counts=bucket_counts,
        calibration_support_flag_counts=support_flag_counts,
        calibration_applied_row_count=sum(1 for row in rows if row.calibration_applied),
        delta_trade_count=calibrated_summary.trade_count - raw_summary.trade_count,
        delta_total_pnl=calibrated_summary.total_pnl - raw_summary.total_pnl,
        delta_average_roi=_decimal_delta(
            calibrated_summary.average_roi,
            raw_summary.average_roi,
        ),
        delta_average_selected_net_edge=_decimal_delta(
            calibrated_summary.average_selected_net_edge,
            raw_summary.average_selected_net_edge,
        ),
    )
    return comparison, tuple(rows)


def calibrated_baseline_comparison_to_dict(
    comparison: CalibratedBaselineComparison,
) -> dict[str, object]:
    """Serialize the calibrated baseline comparison to stable JSON."""

    return serialize_value(comparison)


def render_calibrated_baseline_report(comparison: CalibratedBaselineComparison) -> str:
    """Render the raw-vs-calibrated good-only replay comparison."""

    lines = [
        f"# Calibrated Baseline Comparison — {comparison.analysis_id}",
        "",
        comparison.description,
        "",
        "## Frozen Stack",
        f"- cross_horizon_manifest_path: `{comparison.cross_horizon_manifest_path}`",
        f"- replay_comparison_config_path: `{comparison.replay_comparison_config_path}`",
        f"- calibration_config_path: `{comparison.calibration_config_path}`",
        f"- calibration_summary_path: `{comparison.calibration_summary_path}`",
        f"- baseline_stack_path: `{comparison.baseline_stack_path}`",
        f"- admission_semantics_version: `{comparison.admission_semantics_version}`",
        f"- policy_universe: `{comparison.policy_universe}`",
        f"- oracle_source: `{comparison.oracle_source}`",
        "",
        "## Sessions",
    ]
    for session in comparison.sessions:
        raw_summary = session.raw_summary
        calibrated_summary = session.calibrated_summary
        raw_line = (
            f"  snapshots={raw_summary.snapshot_count}, windows={raw_summary.window_count}, "
            f"trades={raw_summary.trade_count}, hit_rate={raw_summary.hit_rate}, "
            f"avg_raw_edge={raw_summary.average_selected_raw_edge}, "
            f"avg_net_edge={raw_summary.average_selected_net_edge}, "
            f"total_pnl={raw_summary.total_pnl}, avg_roi={raw_summary.average_roi}, "
            f"pnl_per_window={raw_summary.pnl_per_window}, "
            f"pnl_per_100_trades={raw_summary.pnl_per_100_trades}, "
            f"pnl_per_1000_snapshots={raw_summary.pnl_per_1000_snapshots}"
        )
        calibrated_line = (
            f"  snapshots={calibrated_summary.snapshot_count}, "
            f"windows={calibrated_summary.window_count}, "
            f"trades={calibrated_summary.trade_count}, "
            f"hit_rate={calibrated_summary.hit_rate}, "
            f"avg_raw_edge={calibrated_summary.average_selected_raw_edge}, "
            f"avg_net_edge={calibrated_summary.average_selected_net_edge}, "
            f"total_pnl={calibrated_summary.total_pnl}, "
            f"avg_roi={calibrated_summary.average_roi}, "
            f"pnl_per_window={calibrated_summary.pnl_per_window}, "
            f"pnl_per_100_trades={calibrated_summary.pnl_per_100_trades}, "
            f"pnl_per_1000_snapshots={calibrated_summary.pnl_per_1000_snapshots}"
        )
        lines.extend(
            [
                f"### {session.session_label}",
                f"- session_id: `{session.session_id}`",
                f"- capture_date: `{session.capture_date}`",
                f"- baseline rows: {session.row_count}",
                f"- calibration_bucket_counts: {session.calibration_bucket_counts}",
                f"- calibration_support_flag_counts: {session.calibration_support_flag_counts}",
                f"- calibration_applied_row_count: {session.calibration_applied_row_count}",
                "- raw:",
                raw_line,
                "- calibrated:",
                calibrated_line,
                (
                    f"- delta: trade_count={session.delta_trade_count}, "
                    f"avg_net_edge={session.delta_average_selected_net_edge}, "
                    f"avg_roi={session.delta_average_roi}, total_pnl={session.delta_total_pnl}"
                ),
            ]
        )
    return "\n".join(lines) + "\n"


def _build_scenario_summary(
    scenario_name: str,
    *,
    scenario_edges: Sequence[ExecutableEdgeEstimate],
    trades: Sequence[SimulatedTrade],
    rows: Sequence[CalibratedBaselineRow],
    selected_raw_edge_attr: str,
    selected_net_edge_attr: str,
) -> BaselineScenarioSummary:
    _ = scenario_edges
    simulation_summary = summarize_simulated_trades(trades)
    window_count = len({row.window_id for row in rows})
    selected_raw_edges = [
        value
        for row in rows
        if (value := getattr(row, selected_raw_edge_attr)) is not None
    ]
    selected_net_edges = [
        value
        for row in rows
        if (value := getattr(row, selected_net_edge_attr)) is not None
    ]
    return BaselineScenarioSummary(
        scenario_name=scenario_name,
        snapshot_count=len(rows),
        window_count=window_count,
        trade_count=simulation_summary.trade_count,
        hit_rate=simulation_summary.hit_rate,
        average_selected_raw_edge=_average_decimal(selected_raw_edges),
        average_selected_net_edge=_average_decimal(selected_net_edges),
        total_pnl=simulation_summary.total_pnl,
        average_roi=_average_trade_roi(trades),
        pnl_per_window=_normalized_pnl(
            simulation_summary.total_pnl,
            count=window_count,
            scale=Decimal("1"),
        ),
        pnl_per_1000_snapshots=_normalized_pnl(
            simulation_summary.total_pnl,
            count=len(rows),
            scale=Decimal("1000"),
        ),
        pnl_per_100_trades=_normalized_pnl(
            simulation_summary.total_pnl,
            count=simulation_summary.trade_count,
            scale=Decimal("100"),
        ),
    )


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


def _selected_edge(edge: ExecutableEdgeEstimate, *, net: bool) -> Decimal | None:
    if edge.preferred_side == "up":
        return edge.edge_up_net if net else edge.edge_up_raw
    if edge.preferred_side == "down":
        return edge.edge_down_net if net else edge.edge_down_raw
    return None


def _average_decimal(values: Iterable[Decimal]) -> Decimal | None:
    materialized = list(values)
    if not materialized:
        return None
    return sum(materialized) / Decimal(len(materialized))


def _average_trade_roi(trades: Iterable[SimulatedTrade]) -> Decimal | None:
    traded = [trade.sim_roi for trade in trades if trade.sim_trade_direction != "no_trade"]
    return _average_decimal(traded)


def _normalized_pnl(
    total_pnl: Decimal,
    *,
    count: int,
    scale: Decimal,
) -> Decimal | None:
    if count <= 0:
        return None
    return (total_pnl / Decimal(count)) * scale


def _count_by(rows: Sequence[CalibratedBaselineRow], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(getattr(row, field_name))
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _decimal_delta(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None or right is None:
        return None
    return left - right


__all__ = [
    "AppliedCalibration",
    "BaselineScenarioSummary",
    "CalibratedBaselineComparison",
    "CalibratedBaselineRow",
    "CalibratedBaselineSessionComparison",
    "FrozenCalibrationBucket",
    "FrozenCalibrationRuntime",
    "apply_frozen_stage1_calibration",
    "build_calibrated_baseline_session_comparison",
    "calibrated_baseline_comparison_to_dict",
    "load_frozen_calibration_runtime",
    "render_calibrated_baseline_report",
]
