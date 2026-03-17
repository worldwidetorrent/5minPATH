"""Focused degraded-regime stress and context analysis."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from rtds.replay.execution_sensitivity import (
    ExecutionSensitivityVariant,
    ExecutionSensitivityVariantResult,
    build_execution_sensitivity_variant_result,
    execution_variant_result_to_dict,
)
from rtds.replay.regime_compare import (
    REGIME_DEGRADED_LIGHT_ONLY,
    REGIME_DEGRADED_MEDIUM_ONLY,
    WindowQualityRow,
    _slice_result_row,
    filter_evaluation_rows_for_regime,
)
from rtds.replay.slices import (
    CHAINLINK_CONFIDENCE_DIMENSION,
    NET_EDGE_DIMENSION,
    RAW_EDGE_DIMENSION,
    SECONDS_REMAINING_DIMENSION,
    SPREAD_DIMENSION,
    VOLATILITY_DIMENSION,
    ReplaySliceInput,
    ReplaySlicePolicy,
    generate_replay_slices,
)

FOCUSED_DEGRADED_REGIME_ORDER: tuple[str, ...] = (
    REGIME_DEGRADED_LIGHT_ONLY,
    REGIME_DEGRADED_MEDIUM_ONLY,
)

FOCUSED_DEGRADED_VARIANTS: tuple[ExecutionSensitivityVariant, ...] = (
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
)

FOCUSED_CONTEXT_DIMENSIONS: tuple[str, ...] = (
    SECONDS_REMAINING_DIMENSION,
    VOLATILITY_DIMENSION,
    SPREAD_DIMENSION,
    RAW_EDGE_DIMENSION,
    NET_EDGE_DIMENSION,
    CHAINLINK_CONFIDENCE_DIMENSION,
)


@dataclass(slots=True, frozen=True)
class DegradedRegimeContextResult:
    """One degraded regime decomposed across context dimensions."""

    regime_name: str
    snapshot_count: int
    window_count: int
    slices: dict[str, tuple[dict[str, object], ...]]


def build_focused_degraded_stress_results(
    evaluation_rows: Sequence[Any],
    *,
    window_quality_by_window: Mapping[str, WindowQualityRow],
    replay_config: Any,
    variants: Sequence[ExecutionSensitivityVariant] = FOCUSED_DEGRADED_VARIANTS,
) -> tuple[ExecutionSensitivityVariantResult, ...]:
    """Build the focused light/medium execution-stress matrix."""

    return tuple(
        build_execution_sensitivity_variant_result(
            evaluation_rows,
            window_quality_by_window=window_quality_by_window,
            replay_config=replay_config,
            variant=variant,
            regime_order=FOCUSED_DEGRADED_REGIME_ORDER,
        )
        for variant in variants
    )


def build_degraded_context_result(
    evaluation_rows: Sequence[Any],
    *,
    window_quality_by_window: Mapping[str, WindowQualityRow],
    regime_name: str,
    slice_policy: ReplaySlicePolicy,
) -> DegradedRegimeContextResult:
    """Decompose one degraded regime by timing, volatility, spread, edge, and oracle context."""

    filtered_rows = filter_evaluation_rows_for_regime(
        evaluation_rows,
        window_verdict_by_window={
            window_id: quality.window_verdict
            for window_id, quality in window_quality_by_window.items()
        },
        regime_name=regime_name,
        window_quality_by_window=window_quality_by_window,
    )
    slice_report = generate_replay_slices(
        [
            ReplaySliceInput(
                labeled_snapshot=row.labeled_snapshot,
                executable_edge=row.edge,
                simulated_trade=row.simulated_trade,
                seconds_remaining=row.seconds_remaining,
                sigma_eff=row.volatility.sigma_eff,
            )
            for row in filtered_rows
        ],
        policy=slice_policy,
    )
    return DegradedRegimeContextResult(
        regime_name=regime_name,
        snapshot_count=len(filtered_rows),
        window_count=len({row.snapshot.window_id for row in filtered_rows}),
        slices={
            dimension: tuple(
                _slice_result_row(row) for row in slice_report.by_dimension[dimension]
            )
            for dimension in FOCUSED_CONTEXT_DIMENSIONS
        },
    )


def context_result_to_dict(result: DegradedRegimeContextResult) -> dict[str, object]:
    """Serialize one degraded context result to stable JSON."""

    return {
        "regime_name": result.regime_name,
        "snapshot_count": result.snapshot_count,
        "window_count": result.window_count,
        "slices": result.slices,
    }


def render_degraded_regime_report(
    *,
    trade_date: str,
    session_id: str,
    admission_summary_path: str | Path,
    stress_results: Sequence[ExecutionSensitivityVariantResult],
    context_results: Sequence[DegradedRegimeContextResult],
) -> str:
    """Render the focused degraded follow-up analysis report."""

    lines = [
        f"# Degraded Regime Follow-up — {trade_date}",
        "",
        "## Run",
        f"- session_id: `{session_id}`",
        f"- admission_summary: `{admission_summary_path}`",
        "",
        "## Stress Verdict",
    ]
    lines.extend(_stress_verdict_lines(stress_results))
    lines.extend(["", "## Stress Matrix"])
    for result in stress_results:
        lines.append(f"### {result.display_name}")
        for regime_result in result.regime_results:
            lines.append(
                f"- {regime_result.regime_name}: trades={regime_result.trade_count}, "
                f"avg_net_edge={regime_result.average_selected_net_edge}, "
                f"avg_roi={regime_result.average_roi}, "
                f"total_pnl={regime_result.total_pnl}"
            )
    lines.extend(["", "## Context Decomposition"])
    for result in context_results:
        lines.extend(
            [
                f"### {result.regime_name}",
                f"- snapshots: {result.snapshot_count}",
                f"- windows: {result.window_count}",
            ]
        )
        for dimension in FOCUSED_CONTEXT_DIMENSIONS:
            rows = result.slices[dimension]
            lines.append(f"- {dimension}:")
            for row in rows:
                lines.append(
                    "  "
                    + f"{row['slice_key']}: rows={row['row_count']}, trades={row['trade_count']}, "
                    + f"hit_rate={row['hit_rate']}, avg_roi={row['average_roi']}, "
                    + f"total_pnl={row['total_pnl']}"
                )
    return "\n".join(lines) + "\n"


def _stress_verdict_lines(
    stress_results: Sequence[ExecutionSensitivityVariantResult],
) -> list[str]:
    by_variant = {result.variant_name: result for result in stress_results}
    baseline = by_variant.get("baseline_execution")
    if baseline is None:
        return ["- baseline execution result is missing."]
    baseline_by_regime = {item.regime_name: item for item in baseline.regime_results}
    light_baseline = baseline_by_regime.get(REGIME_DEGRADED_LIGHT_ONLY)
    medium_baseline = baseline_by_regime.get(REGIME_DEGRADED_MEDIUM_ONLY)
    lines: list[str] = []
    if light_baseline is not None and medium_baseline is not None:
        if _roi_gap(medium_baseline, light_baseline) <= Decimal("0.05"):
            lines.append(
                "- degraded_medium stays close enough to degraded_light under baseline replay to "
                "justify deeper context slicing instead of immediate exclusion."
            )
        else:
            lines.append(
                "- degraded_medium is economically distinct from degraded_light under baseline "
                "replay, so its inclusion needs a separate policy rule."
            )
    for variant_name in ("slippage_1_5x", "slippage_2x", "half_size"):
        result = by_variant.get(variant_name)
        if result is None:
            continue
        stressed_by_regime = {item.regime_name: item for item in result.regime_results}
        stressed_medium = stressed_by_regime.get(REGIME_DEGRADED_MEDIUM_ONLY)
        if medium_baseline is None or stressed_medium is None:
            continue
        if _roi_gap(medium_baseline, stressed_medium) > Decimal("0.10"):
            lines.append(
                f"- degraded_medium weakens materially under `{variant_name}`; its current edge "
                "looks execution-sensitive."
            )
        else:
            lines.append(
                f"- degraded_medium remains measurable under `{variant_name}`; the regime does "
                "not collapse under that stress assumption."
            )
    return lines or ["- no stress conclusions available."]


def _roi_gap(left: Any, right: Any) -> Decimal:
    left_roi = left.average_roi or Decimal("0")
    right_roi = right.average_roi or Decimal("0")
    return abs(left_roi - right_roi)


__all__ = [
    "DegradedRegimeContextResult",
    "FOCUSED_CONTEXT_DIMENSIONS",
    "FOCUSED_DEGRADED_REGIME_ORDER",
    "FOCUSED_DEGRADED_VARIANTS",
    "build_degraded_context_result",
    "build_focused_degraded_stress_results",
    "context_result_to_dict",
    "execution_variant_result_to_dict",
    "render_degraded_regime_report",
]
