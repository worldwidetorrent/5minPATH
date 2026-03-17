"""Execution-sensitivity replay comparison for degraded window regimes."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Iterable, Mapping, Sequence

from rtds.features.executable_edge import EdgeCostPolicy, compute_executable_edge
from rtds.replay.regime_compare import (
    REGIME_ALL_WINDOWS,
    REGIME_DEGRADED_LIGHT_ONLY,
    REGIME_DEGRADED_LIGHT_PLUS_MEDIUM,
    REGIME_DEGRADED_ONLY,
    REGIME_GOOD_ONLY,
    REGIME_GOOD_PLUS_DEGRADED_LIGHT,
    ReplayRegimeResult,
    WindowQualityRow,
    build_regime_result,
    regime_result_to_dict,
)
from rtds.replay.simulate import (
    SIM_NO_TRADE,
    SIM_OUTCOME_NO_TRADE,
    EntryRulePolicy,
    ReplaySimulationInput,
    SimulatedTrade,
    simulate_snapshot,
)
from rtds.schemas.normalized import PolymarketQuote
from rtds.storage.writer import serialize_value

NO_TRADE_SPREAD_CAP_EXCEEDED = "spread_cap_exceeded"


@dataclass(slots=True, frozen=True)
class ExecutionSensitivityVariant:
    """One stressed execution assumption set."""

    variant_name: str
    display_name: str
    slippage_multiplier: Decimal = Decimal("1")
    target_trade_size_multiplier: Decimal = Decimal("1")
    min_net_edge: Decimal | None = None
    max_selected_spread_abs: Decimal | None = None
    minimum_window_quote_coverage_ratio: float | None = None


@dataclass(slots=True, frozen=True)
class ExecutionSensitivityVariantResult:
    """Execution-sensitivity matrix for one stressed assumption set."""

    variant_name: str
    display_name: str
    policy: dict[str, object]
    regime_results: tuple[ReplayRegimeResult, ...]


DEFAULT_SENSITIVITY_REGIME_ORDER: tuple[str, ...] = (
    REGIME_GOOD_ONLY,
    REGIME_DEGRADED_LIGHT_ONLY,
    REGIME_DEGRADED_LIGHT_PLUS_MEDIUM,
    REGIME_DEGRADED_ONLY,
    REGIME_GOOD_PLUS_DEGRADED_LIGHT,
    REGIME_ALL_WINDOWS,
)

DEFAULT_EXECUTION_VARIANTS: tuple[ExecutionSensitivityVariant, ...] = (
    ExecutionSensitivityVariant(
        variant_name="baseline_execution",
        display_name="Baseline execution",
    ),
    ExecutionSensitivityVariant(
        variant_name="slippage_1_5x",
        display_name="1.5x slippage",
        slippage_multiplier=Decimal("1.5"),
    ),
    ExecutionSensitivityVariant(
        variant_name="slippage_2x",
        display_name="2x slippage",
        slippage_multiplier=Decimal("2"),
    ),
    ExecutionSensitivityVariant(
        variant_name="half_size",
        display_name="Half size",
        target_trade_size_multiplier=Decimal("0.5"),
    ),
    ExecutionSensitivityVariant(
        variant_name="tight_spread_cap_0_02",
        display_name="Tight spread cap 0.02",
        max_selected_spread_abs=Decimal("0.02"),
    ),
    ExecutionSensitivityVariant(
        variant_name="strict_quote_coverage_0_95",
        display_name="Strict min quote coverage 0.95",
        minimum_window_quote_coverage_ratio=0.95,
    ),
    ExecutionSensitivityVariant(
        variant_name="degraded_light_candidate_policy",
        display_name="Candidate degraded-light policy",
        slippage_multiplier=Decimal("1.5"),
        target_trade_size_multiplier=Decimal("0.5"),
        min_net_edge=Decimal("0.03"),
        max_selected_spread_abs=Decimal("0.02"),
        minimum_window_quote_coverage_ratio=0.95,
    ),
)


def build_execution_sensitivity_variant_result(
    evaluation_rows: Iterable[Any],
    *,
    window_quality_by_window: Mapping[str, WindowQualityRow],
    replay_config: Any,
    variant: ExecutionSensitivityVariant,
    regime_order: Sequence[str] = DEFAULT_SENSITIVITY_REGIME_ORDER,
) -> ExecutionSensitivityVariantResult:
    """Re-evaluate one replay batch under one stressed execution variant."""

    variant_rows = tuple(
        _apply_variant_to_row(
            row,
            replay_config=replay_config,
            variant=variant,
        )
        for row in evaluation_rows
    )
    regime_results = tuple(
        build_regime_result(
            variant_rows,
            window_verdict_by_window={
                window_id: quality.window_verdict
                for window_id, quality in window_quality_by_window.items()
            },
            regime_name=regime_name,
            window_quality_by_window=window_quality_by_window,
            minimum_window_quote_coverage_ratio=variant.minimum_window_quote_coverage_ratio,
            include_slices=False,
        )
        for regime_name in regime_order
    )
    return ExecutionSensitivityVariantResult(
        variant_name=variant.variant_name,
        display_name=variant.display_name,
        policy=_variant_policy_dict(variant),
        regime_results=regime_results,
    )


def execution_variant_result_to_dict(
    result: ExecutionSensitivityVariantResult,
) -> dict[str, object]:
    """Serialize one variant matrix to stable JSON."""

    return {
        "variant_name": result.variant_name,
        "display_name": result.display_name,
        "policy": result.policy,
        "regimes": [regime_result_to_dict(item) for item in result.regime_results],
    }


def render_execution_sensitivity_report(
    variant_results: Sequence[ExecutionSensitivityVariantResult],
    *,
    trade_date: str,
    session_id: str,
    admission_summary_path: str,
) -> str:
    """Render a markdown sensitivity report for one capture session."""

    lines = [
        f"# Execution Sensitivity Comparison — {trade_date}",
        "",
        "## Run",
        f"- session_id: `{session_id}`",
        f"- admission_summary: `{admission_summary_path}`",
        "",
        "## Verdict",
    ]
    lines.extend(_sensitivity_verdict_lines(variant_results))
    lines.extend(["", "## Variants"])
    for variant_result in variant_results:
        lines.extend(
            [
                f"### {variant_result.display_name}",
                f"- policy: `{variant_result.policy}`",
            ]
        )
        for regime_result in variant_result.regime_results:
            lines.extend(
                [
                    f"- {regime_result.regime_name}: trades={regime_result.trade_count}, "
                    f"hit_rate={regime_result.hit_rate}, "
                    f"avg_net_edge={regime_result.average_selected_net_edge}, "
                    f"avg_roi={regime_result.average_roi}, "
                    f"total_pnl={regime_result.total_pnl}",
                ]
            )
    return "\n".join(lines) + "\n"


def _sensitivity_verdict_lines(
    variant_results: Sequence[ExecutionSensitivityVariantResult],
) -> list[str]:
    by_variant = {result.variant_name: result for result in variant_results}
    baseline = by_variant.get("baseline_execution")
    candidate = by_variant.get("degraded_light_candidate_policy")
    slippage_2x = by_variant.get("slippage_2x")
    if baseline is None:
        return ["- baseline execution variant is missing."]

    baseline_by_regime = {item.regime_name: item for item in baseline.regime_results}
    lines: list[str] = []
    good_only = baseline_by_regime.get(REGIME_GOOD_ONLY)
    degraded_light = baseline_by_regime.get(REGIME_DEGRADED_LIGHT_ONLY)
    if good_only is not None and degraded_light is not None:
        if _materially_weaker(degraded_light, good_only):
            lines.append(
                "- degraded_light remains materially weaker than good_only under baseline "
                "execution; keep the first policy baseline on good windows only."
            )
        else:
            lines.append(
                "- degraded_light stays close to good_only under baseline execution; a second-tier "
                "overlay looks viable."
            )
    if slippage_2x is not None:
        slippage_2x_by_regime = {item.regime_name: item for item in slippage_2x.regime_results}
        stressed_light = slippage_2x_by_regime.get(REGIME_DEGRADED_LIGHT_ONLY)
        if degraded_light is not None and stressed_light is not None:
            if _materially_weaker(stressed_light, degraded_light):
                lines.append(
                    "- degraded_light weakens materially under 2x slippage; the regime is not "
                    "robust enough for baseline policy extraction."
                )
            else:
                lines.append(
                    "- degraded_light holds up under 2x slippage better than expected; the main "
                    "risk is still regime contamination, not pure execution stress."
                )
    if candidate is not None:
        candidate_by_regime = {item.regime_name: item for item in candidate.regime_results}
        candidate_light = candidate_by_regime.get(REGIME_DEGRADED_LIGHT_ONLY)
        if candidate_light is not None and candidate_light.trade_count > 0:
            lines.append(
                "- the candidate degraded-light policy still trades and stays measurable under "
                "its stricter controls; treat it as an exploratory second-tier regime only."
            )
        else:
            lines.append(
                "- the candidate degraded-light policy eliminates nearly all trades; current "
                "degraded windows are not yet ready for a second-tier policy."
            )
    return lines or ["- no sensitivity conclusions available."]


def _materially_weaker(left: ReplayRegimeResult, right: ReplayRegimeResult) -> bool:
    left_roi = left.average_roi or Decimal("0")
    right_roi = right.average_roi or Decimal("0")
    left_net = left.average_selected_net_edge or Decimal("0")
    right_net = right.average_selected_net_edge or Decimal("0")
    return (right_roi - left_roi) > Decimal("0.10") or (right_net - left_net) > Decimal("0.02")


def _apply_variant_to_row(
    row: Any,
    *,
    replay_config: Any,
    variant: ExecutionSensitivityVariant,
) -> Any:
    cost_policy = _variant_cost_policy(replay_config.edge_cost_policy, variant)
    entry_rules = _variant_entry_rules(replay_config.entry_rules, variant)
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
        entry_rules=entry_rules,
    )
    if (
        variant.max_selected_spread_abs is not None
        and simulated_trade.sim_trade_direction != SIM_NO_TRADE
    ):
        selected_spread = _selected_spread(row.snapshot, preferred_side=edge.preferred_side)
        if selected_spread is None or selected_spread > variant.max_selected_spread_abs:
            simulated_trade = _spread_capped_no_trade(
                row=row,
                predicted_edge_net=simulated_trade.predicted_edge_net,
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


def _variant_cost_policy(
    base_policy: EdgeCostPolicy,
    variant: ExecutionSensitivityVariant,
) -> EdgeCostPolicy:
    return EdgeCostPolicy(
        fee_rate_estimate=base_policy.fee_rate_estimate,
        slippage_estimate_up=base_policy.slippage_estimate_up * variant.slippage_multiplier,
        slippage_estimate_down=base_policy.slippage_estimate_down * variant.slippage_multiplier,
        model_error_buffer=base_policy.model_error_buffer,
    )


def _variant_entry_rules(
    base_rules: EntryRulePolicy,
    variant: ExecutionSensitivityVariant,
) -> EntryRulePolicy:
    return EntryRulePolicy(
        min_net_edge=(
            base_rules.min_net_edge if variant.min_net_edge is None else variant.min_net_edge
        ),
        target_trade_size_contracts=(
            base_rules.target_trade_size_contracts * variant.target_trade_size_multiplier
        ),
        allow_buy_up=base_rules.allow_buy_up,
        allow_buy_down=base_rules.allow_buy_down,
    )


def _variant_policy_dict(variant: ExecutionSensitivityVariant) -> dict[str, object]:
    return {
        "slippage_multiplier": serialize_value(variant.slippage_multiplier),
        "target_trade_size_multiplier": serialize_value(variant.target_trade_size_multiplier),
        "min_net_edge": serialize_value(variant.min_net_edge),
        "max_selected_spread_abs": serialize_value(variant.max_selected_spread_abs),
        "minimum_window_quote_coverage_ratio": variant.minimum_window_quote_coverage_ratio,
    }


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


def _spread_capped_no_trade(*, row: Any, predicted_edge_net: Decimal | None) -> SimulatedTrade:
    snapshot = row.snapshot
    return SimulatedTrade(
        snapshot_id=snapshot.snapshot_id or "",
        window_id=snapshot.window_id,
        polymarket_market_id=snapshot.polymarket_market_id,
        sim_trade_direction=SIM_NO_TRADE,
        sim_entry_price=None,
        sim_exit_price=None,
        sim_fee_paid=None,
        sim_slippage_paid=None,
        sim_pnl=Decimal("0"),
        sim_roi=Decimal("0"),
        sim_outcome=SIM_OUTCOME_NO_TRADE,
        predicted_edge_net=predicted_edge_net,
        realized_edge=None,
        no_trade_reason=NO_TRADE_SPREAD_CAP_EXCEEDED,
        simulation_version="0.1.0",
    )


__all__ = [
    "DEFAULT_EXECUTION_VARIANTS",
    "DEFAULT_SENSITIVITY_REGIME_ORDER",
    "ExecutionSensitivityVariant",
    "ExecutionSensitivityVariantResult",
    "NO_TRADE_SPREAD_CAP_EXCEEDED",
    "build_execution_sensitivity_variant_result",
    "execution_variant_result_to_dict",
    "render_execution_sensitivity_report",
]
