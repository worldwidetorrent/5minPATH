"""Replay comparison across window-quality regimes."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from rtds.replay.simulate import SimulatedTrade, summarize_simulated_trades
from rtds.replay.slices import (
    COMPOSITE_QUALITY_DIMENSION,
    DEFAULT_REPLAY_SLICE_POLICY,
    SECONDS_REMAINING_DIMENSION,
    VOLATILITY_DIMENSION,
    ReplaySliceInput,
    ReplaySlicePolicy,
    generate_replay_slices,
)
from rtds.storage.writer import serialize_value

REGIME_GOOD_ONLY = "good_only"
REGIME_DEGRADED_ONLY = "degraded_only"
REGIME_DEGRADED_LIGHT_ONLY = "degraded_light_only"
REGIME_DEGRADED_LIGHT_PLUS_MEDIUM = "degraded_light_plus_degraded_medium"
REGIME_ALL_DEGRADED = "all_degraded"
REGIME_GOOD_PLUS_DEGRADED_LIGHT = "good_plus_degraded_light"
REGIME_GOOD_PLUS_DEGRADED_LIGHT_PLUS_MEDIUM = "good_plus_degraded_light_plus_degraded_medium"
REGIME_GOOD_PLUS_DEGRADED = "good_plus_degraded"
REGIME_ALL_WINDOWS = "all_windows"

DEGRADED_WINDOW_VERDICTS = frozenset(
    {"degraded_light", "degraded_medium", "degraded_heavy"}
)

REGIME_WINDOW_VERDICTS: dict[str, frozenset[str] | None] = {
    REGIME_GOOD_ONLY: frozenset({"good"}),
    REGIME_DEGRADED_ONLY: DEGRADED_WINDOW_VERDICTS,
    REGIME_DEGRADED_LIGHT_ONLY: frozenset({"degraded_light"}),
    REGIME_DEGRADED_LIGHT_PLUS_MEDIUM: frozenset({"degraded_light", "degraded_medium"}),
    REGIME_ALL_DEGRADED: frozenset(
        {"degraded_light", "degraded_medium", "degraded_heavy", "unusable"}
    ),
    REGIME_GOOD_PLUS_DEGRADED_LIGHT: frozenset({"good", "degraded_light"}),
    REGIME_GOOD_PLUS_DEGRADED_LIGHT_PLUS_MEDIUM: frozenset(
        {"good", "degraded_light", "degraded_medium"}
    ),
    REGIME_GOOD_PLUS_DEGRADED: frozenset({"good", *DEGRADED_WINDOW_VERDICTS}),
    REGIME_ALL_WINDOWS: None,
}

REGIME_LABELS: dict[str, str] = {
    REGIME_GOOD_ONLY: "Regime A — good-only windows",
    REGIME_DEGRADED_ONLY: "Regime B — degraded-only windows",
    REGIME_DEGRADED_LIGHT_ONLY: "Regime C — degraded_light-only windows",
    REGIME_DEGRADED_LIGHT_PLUS_MEDIUM: (
        "Regime D — degraded_light + degraded_medium windows"
    ),
    REGIME_ALL_DEGRADED: "Regime E — all degraded / unusable windows",
    REGIME_GOOD_PLUS_DEGRADED_LIGHT: "Regime F — good + degraded_light windows",
    REGIME_GOOD_PLUS_DEGRADED_LIGHT_PLUS_MEDIUM: (
        "Regime G — good + degraded_light + degraded_medium windows"
    ),
    REGIME_GOOD_PLUS_DEGRADED: "Legacy — good + all degraded windows",
    REGIME_ALL_WINDOWS: "Regime H — all windows",
}

DEFAULT_REGIME_ORDER: tuple[str, ...] = (
    REGIME_GOOD_ONLY,
    REGIME_DEGRADED_ONLY,
    REGIME_DEGRADED_LIGHT_ONLY,
    REGIME_DEGRADED_LIGHT_PLUS_MEDIUM,
    REGIME_ALL_DEGRADED,
    REGIME_GOOD_PLUS_DEGRADED_LIGHT,
    REGIME_GOOD_PLUS_DEGRADED_LIGHT_PLUS_MEDIUM,
    REGIME_ALL_WINDOWS,
)

COMPARISON_SLICE_DIMENSIONS: tuple[str, ...] = (
    SECONDS_REMAINING_DIMENSION,
    VOLATILITY_DIMENSION,
    COMPOSITE_QUALITY_DIMENSION,
)


@dataclass(slots=True, frozen=True)
class ReplayRegimeResult:
    """One replay comparison regime summary plus selected slice tables."""

    regime_name: str
    display_name: str
    snapshot_count: int
    window_count: int
    window_verdict_counts: dict[str, int]
    trade_count: int
    hit_rate: Decimal
    average_selected_raw_edge: Decimal | None
    average_selected_net_edge: Decimal | None
    total_pnl: Decimal
    average_roi: Decimal | None
    average_predicted_edge: Decimal | None
    average_realized_edge: Decimal | None
    realized_minus_predicted_edge: Decimal | None
    slices: dict[str, tuple[dict[str, object], ...]]


def load_window_verdicts(admission_summary_path: str | Path) -> dict[str, str]:
    """Load `window_id -> window_verdict` from one capture admission summary."""

    payload = json.loads(Path(admission_summary_path).read_text(encoding="utf-8"))
    continuity = payload.get("polymarket_continuity", {})
    if not isinstance(continuity, Mapping):
        return {}
    window_rows = continuity.get("window_quote_coverage", ())
    if not isinstance(window_rows, Sequence):
        return {}
    verdicts: dict[str, str] = {}
    for row in window_rows:
        if not isinstance(row, Mapping):
            continue
        window_id = row.get("window_id")
        verdict = row.get("window_verdict")
        if isinstance(window_id, str) and isinstance(verdict, str):
            verdicts[window_id] = verdict
    return verdicts


def filter_evaluation_rows_for_regime(
    evaluation_rows: Iterable[Any],
    *,
    window_verdict_by_window: Mapping[str, str],
    regime_name: str,
) -> list[Any]:
    """Keep only evaluation rows admitted by the selected regime."""

    allowed_verdicts = REGIME_WINDOW_VERDICTS[regime_name]
    if allowed_verdicts is None:
        return list(evaluation_rows)
    return [
        row
        for row in evaluation_rows
        if window_verdict_by_window.get(row.snapshot.window_id, "unknown") in allowed_verdicts
    ]


def build_regime_result(
    evaluation_rows: Iterable[Any],
    *,
    window_verdict_by_window: Mapping[str, str],
    regime_name: str,
    slice_policy: ReplaySlicePolicy = DEFAULT_REPLAY_SLICE_POLICY,
) -> ReplayRegimeResult:
    """Aggregate one replay regime from evaluated snapshot rows."""

    rows = filter_evaluation_rows_for_regime(
        evaluation_rows,
        window_verdict_by_window=window_verdict_by_window,
        regime_name=regime_name,
    )
    window_ids = sorted({row.snapshot.window_id for row in rows})
    window_verdict_counts = Counter(
        window_verdict_by_window.get(window_id, "unknown") for window_id in window_ids
    )
    simulation_summary = summarize_simulated_trades(row.simulated_trade for row in rows)

    selected_raw_edges = [
        value for row in rows if (value := _selected_edge(row.edge, net=False)) is not None
    ]
    selected_net_edges = [
        value for row in rows if (value := _selected_edge(row.edge, net=True)) is not None
    ]
    average_selected_raw_edge = _average_decimal(selected_raw_edges)
    average_selected_net_edge = _average_decimal(selected_net_edges)

    slice_inputs = [
        ReplaySliceInput(
            labeled_snapshot=row.labeled_snapshot,
            executable_edge=row.edge,
            simulated_trade=row.simulated_trade,
            seconds_remaining=row.seconds_remaining,
            sigma_eff=row.volatility.sigma_eff,
        )
        for row in rows
    ]
    slice_report = generate_replay_slices(slice_inputs, policy=slice_policy)
    selected_slices = {
        dimension: tuple(_slice_result_row(row) for row in slice_report.by_dimension[dimension])
        for dimension in COMPARISON_SLICE_DIMENSIONS
    }

    return ReplayRegimeResult(
        regime_name=regime_name,
        display_name=REGIME_LABELS[regime_name],
        snapshot_count=len(rows),
        window_count=len(window_ids),
        window_verdict_counts=dict(sorted(window_verdict_counts.items())),
        trade_count=simulation_summary.trade_count,
        hit_rate=simulation_summary.hit_rate,
        average_selected_raw_edge=average_selected_raw_edge,
        average_selected_net_edge=average_selected_net_edge,
        total_pnl=simulation_summary.total_pnl,
        average_roi=_average_trade_roi(row.simulated_trade for row in rows),
        average_predicted_edge=simulation_summary.average_predicted_edge,
        average_realized_edge=simulation_summary.average_realized_edge,
        realized_minus_predicted_edge=simulation_summary.realized_minus_predicted_edge,
        slices=selected_slices,
    )


def render_regime_comparison_report(
    regime_results: Sequence[ReplayRegimeResult],
    *,
    trade_date: str,
    session_id: str,
    admission_summary_path: str | Path,
) -> str:
    """Render a concise markdown comparison report."""

    by_name = {result.regime_name: result for result in regime_results}
    good_only = by_name.get(REGIME_GOOD_ONLY)
    degraded_only = by_name.get(REGIME_DEGRADED_ONLY)
    degraded_light_only = by_name.get(REGIME_DEGRADED_LIGHT_ONLY)
    good_plus_light = by_name.get(REGIME_GOOD_PLUS_DEGRADED_LIGHT)
    good_plus_light_medium = by_name.get(REGIME_GOOD_PLUS_DEGRADED_LIGHT_PLUS_MEDIUM)
    all_windows = by_name.get(REGIME_ALL_WINDOWS)

    lines = [
        f"# Replay Regime Comparison — {trade_date}",
        "",
        "## Run",
        f"- session_id: `{session_id}`",
        f"- admission_summary: `{admission_summary_path}`",
        "",
        "## Verdict",
    ]
    lines.extend(
        _verdict_lines(
            good_only=good_only,
            degraded_only=degraded_only,
            degraded_light_only=degraded_light_only,
            good_plus_light=good_plus_light,
            good_plus_light_medium=good_plus_light_medium,
            all_windows=all_windows,
        )
    )
    lines.extend(
        [
            "",
            "## Regimes",
        ]
    )
    for result in regime_results:
        lines.extend(
            [
                f"### {result.display_name}",
                f"- snapshots: {result.snapshot_count}",
                f"- windows: {result.window_count}",
                f"- window_verdict_counts: {result.window_verdict_counts}",
                f"- trade_count: {result.trade_count}",
                f"- hit_rate: {result.hit_rate}",
                f"- average_selected_raw_edge: {result.average_selected_raw_edge}",
                f"- average_selected_net_edge: {result.average_selected_net_edge}",
                f"- total_pnl: {result.total_pnl}",
                f"- average_roi: {result.average_roi}",
                f"- average_predicted_edge: {result.average_predicted_edge}",
                f"- average_realized_edge: {result.average_realized_edge}",
            ]
        )
    return "\n".join(lines) + "\n"


def regime_result_to_dict(result: ReplayRegimeResult) -> dict[str, object]:
    """Convert one typed regime result to stable JSON-friendly output."""

    return {
        "regime_name": result.regime_name,
        "display_name": result.display_name,
        "snapshot_count": result.snapshot_count,
        "window_count": result.window_count,
        "window_verdict_counts": result.window_verdict_counts,
        "trade_count": result.trade_count,
        "hit_rate": serialize_value(result.hit_rate),
        "average_selected_raw_edge": serialize_value(result.average_selected_raw_edge),
        "average_selected_net_edge": serialize_value(result.average_selected_net_edge),
        "total_pnl": serialize_value(result.total_pnl),
        "average_roi": serialize_value(result.average_roi),
        "average_predicted_edge": serialize_value(result.average_predicted_edge),
        "average_realized_edge": serialize_value(result.average_realized_edge),
        "realized_minus_predicted_edge": serialize_value(result.realized_minus_predicted_edge),
        "slices": result.slices,
    }


def _average_trade_roi(trades: Iterable[SimulatedTrade]) -> Decimal | None:
    traded = [trade.sim_roi for trade in trades if trade.sim_trade_direction != "no_trade"]
    return _average_decimal(traded)


def _average_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, start=Decimal("0")) / Decimal(len(values))


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


def _slice_result_row(row: Any) -> dict[str, object]:
    return {
        "slice_dimension": row.slice_dimension,
        "slice_key": row.slice_key,
        "row_count": row.row_count,
        "trade_count": row.trade_count,
        "no_trade_count": row.no_trade_count,
        "hit_rate": serialize_value(row.hit_rate),
        "total_pnl": serialize_value(row.total_pnl),
        "average_pnl": serialize_value(row.average_pnl),
        "average_roi": serialize_value(row.average_roi),
        "average_predicted_edge": serialize_value(row.average_predicted_edge),
        "average_realized_edge": serialize_value(row.average_realized_edge),
        "realized_minus_predicted_edge": serialize_value(row.realized_minus_predicted_edge),
    }


def _verdict_lines(
    *,
    good_only: ReplayRegimeResult | None,
    degraded_only: ReplayRegimeResult | None,
    degraded_light_only: ReplayRegimeResult | None,
    good_plus_light: ReplayRegimeResult | None,
    good_plus_light_medium: ReplayRegimeResult | None,
    all_windows: ReplayRegimeResult | None,
) -> list[str]:
    lines: list[str] = []
    if good_only is not None and good_plus_light is not None:
        if _close_enough(good_only, good_plus_light):
            lines.append(
                "- good + degraded_light stays close to good-only; light degraded windows look "
                "economically usable under current replay assumptions."
            )
        else:
            lines.append(
                "- even degraded_light windows diverge materially from good-only; keep them "
                "outside the first policy universe for now."
            )
    if good_plus_light is not None and good_plus_light_medium is not None:
        if _close_enough(good_plus_light, good_plus_light_medium):
            lines.append(
                "- adding degraded_medium windows does not materially change the light-degraded "
                "profile; degradation looks broad rather than medium-specific."
            )
        else:
            lines.append(
                "- adding degraded_medium windows changes the economics materially; medium "
                "degradation should remain a separate boundary from light degradation."
            )
    if degraded_light_only is not None and degraded_only is not None:
        if _close_enough(degraded_light_only, degraded_only):
            lines.append(
                "- degraded-only behavior stays close to degraded_light-only; most degraded "
                "impact comes from the light regime rather than a heavy tail."
            )
        else:
            lines.append(
                "- degraded-only behavior is materially worse than degraded_light-only; heavier "
                "degraded windows are driving the contamination."
            )
    if good_only is not None and good_plus_light_medium is not None and all_windows is not None:
        if _close_enough(good_plus_light_medium, all_windows):
            lines.append(
                "- the remaining spread from good-only comes from degraded windows, not a hidden "
                "unusable-window tail."
            )
    if not lines:
        lines.append("- comparison requires at least the good and degraded regime summaries.")
    return lines


def _close_enough(left: ReplayRegimeResult, right: ReplayRegimeResult) -> bool:
    hit_rate_delta = abs(left.hit_rate - right.hit_rate)
    left_net = left.average_selected_net_edge or Decimal("0")
    right_net = right.average_selected_net_edge or Decimal("0")
    net_edge_delta = abs(left_net - right_net)
    return hit_rate_delta <= Decimal("0.05") and net_edge_delta <= Decimal("0.01")


__all__ = [
    "COMPARISON_SLICE_DIMENSIONS",
    "DEFAULT_REGIME_ORDER",
    "REGIME_ALL_DEGRADED",
    "REGIME_ALL_WINDOWS",
    "REGIME_DEGRADED_LIGHT_ONLY",
    "REGIME_DEGRADED_LIGHT_PLUS_MEDIUM",
    "REGIME_DEGRADED_ONLY",
    "REGIME_GOOD_ONLY",
    "REGIME_GOOD_PLUS_DEGRADED_LIGHT",
    "REGIME_GOOD_PLUS_DEGRADED_LIGHT_PLUS_MEDIUM",
    "REGIME_GOOD_PLUS_DEGRADED",
    "ReplayRegimeResult",
    "build_regime_result",
    "filter_evaluation_rows_for_regime",
    "load_window_verdicts",
    "regime_result_to_dict",
    "render_regime_comparison_report",
]
