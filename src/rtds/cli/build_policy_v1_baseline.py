"""Build the first serious policy-v1 report and good-only stage-1 calibration."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

from rtds.cli.replay_day import (
    _build_effective_config,
    _load_or_build_references,
    _load_or_build_snapshots,
    evaluate_snapshots,
)
from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.good_only_calibration import (
    CalibrationObservation,
    GoodOnlyCalibrationSummary,
    build_good_only_calibration_summary,
    good_only_calibration_summary_to_dict,
    load_good_only_calibration_config,
)
from rtds.replay.loader import load_chainlink_ticks
from rtds.replay.policy_cross_horizon import (
    CrossHorizonComparison,
    build_cross_horizon_comparison,
    cross_horizon_comparison_to_dict,
    load_cross_horizon_manifest,
)
from rtds.replay.regime_compare import (
    REGIME_GOOD_ONLY,
    filter_evaluation_rows_for_regime,
    load_window_quality_rows,
)
from rtds.storage.writer import write_json_file, write_text_file

DEFAULT_OUTPUT_ROOT = "artifacts"
DEFAULT_DATA_ROOT = "data"
DEFAULT_CROSS_HORIZON_MANIFEST = "configs/baselines/analysis/policy_v1_cross_horizon.json"
DEFAULT_CALIBRATION_CONFIG = "configs/replay/calibration_good_only_v1.json"


@dataclass(slots=True, frozen=True)
class _CalibrationSessionInput:
    label: str
    session_id: str
    capture_date: str
    admission_summary_path: str


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_policy_v1_baseline(args)
    print(run_dir)
    return 0


def run_policy_v1_baseline(args: argparse.Namespace) -> Path:
    """Build the policy-v1 evidence report and coarse good-only calibration."""

    run_dir = (
        Path(args.output_root)
        / "policy_v1"
        / f"run_{format_utc_compact(utc_now())}"
    )
    manifest = load_cross_horizon_manifest(args.cross_horizon_manifest)
    comparison = build_cross_horizon_comparison(manifest)
    calibration_config = load_good_only_calibration_config(args.calibration_config)
    write_json_file(
        run_dir / "config_effective.json",
        {
            "cross_horizon_manifest": str(args.cross_horizon_manifest),
            "calibration_config": str(args.calibration_config),
            "comparison_config_path": str(manifest["comparison_config_path"]),
            "session_ids": [str(item["session_id"]) for item in manifest["sessions"]],
            "policy_universe": calibration_config.policy_universe,
            "calibration_id": calibration_config.calibration_id,
        },
    )

    session_inputs = tuple(
        _CalibrationSessionInput(
            label=str(item["label"]),
            session_id=str(item["session_id"]),
            capture_date=str(item["capture_date"]),
            admission_summary_path=str(item["admission_summary_path"]),
        )
        for item in manifest["sessions"]
    )
    observations = _build_calibration_observations(
        session_inputs=session_inputs,
        comparison_config_path=str(manifest["comparison_config_path"]),
        data_root=Path(args.data_root),
        output_root=Path(args.output_root),
    )
    calibration_summary = build_good_only_calibration_summary(
        observations,
        config=calibration_config,
        source_manifest_path=str(args.cross_horizon_manifest),
        comparison_config_path=str(manifest["comparison_config_path"]),
    )

    write_json_file(
        run_dir / "cross_horizon_summary.json",
        cross_horizon_comparison_to_dict(comparison),
    )
    write_json_file(
        run_dir / "good_only_calibration_summary.json",
        good_only_calibration_summary_to_dict(calibration_summary),
    )
    write_text_file(
        run_dir / "report" / "report.md",
        _render_policy_v1_report(comparison, calibration_summary),
    )
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cross-horizon-manifest",
        default=DEFAULT_CROSS_HORIZON_MANIFEST,
    )
    parser.add_argument(
        "--calibration-config",
        default=DEFAULT_CALIBRATION_CONFIG,
    )
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    return parser


def _build_calibration_observations(
    *,
    session_inputs: Sequence[_CalibrationSessionInput],
    comparison_config_path: str,
    data_root: Path,
    output_root: Path,
) -> list[CalibrationObservation]:
    observations: list[CalibrationObservation] = []
    for session in session_inputs:
        trade_date = date.fromisoformat(session.capture_date)
        config = _build_effective_config(
            argparse.Namespace(
                date=session.capture_date,
                session_id=session.session_id,
                data_root=str(data_root),
                output_root=str(output_root),
                config=comparison_config_path,
                rebuild_snapshots=True,
                rebuild_reference=True,
                min_seconds_remaining=0,
                max_seconds_remaining=300,
                edge_threshold="0",
                fee_config=None,
                slippage_config=None,
            ),
            trade_date=trade_date,
            run_dir=output_root
            / "policy_v1"
            / "_tmp"
            / f"session_{session.session_id}",
        )
        window_quality_by_window = load_window_quality_rows(session.admission_summary_path)
        chainlink_ticks = load_chainlink_ticks(
            data_root,
            date_utc=config.trade_date,
            session_id=config.session_id,
        )
        references = _load_or_build_references(config, chainlink_ticks=chainlink_ticks)
        snapshots = _load_or_build_snapshots(
            config,
            references=references,
            chainlink_ticks=chainlink_ticks,
        )
        evaluation_rows = evaluate_snapshots(
            snapshots=snapshots,
            references=references,
            config=config,
        )
        filtered_rows = filter_evaluation_rows_for_regime(
            evaluation_rows,
            window_verdict_by_window={
                window_id: row.window_verdict
                for window_id, row in window_quality_by_window.items()
            },
            regime_name=REGIME_GOOD_ONLY,
            window_quality_by_window=window_quality_by_window,
        )
        for row in filtered_rows:
            fair_value_base = row.fair_value.fair_value_base
            resolved_up = row.labeled_snapshot.label.resolved_up
            if fair_value_base is None or resolved_up is None:
                continue
            observations.append(
                CalibrationObservation(
                    session_label=session.label,
                    session_id=session.session_id,
                    capture_date=session.capture_date,
                    window_id=row.snapshot.window_id,
                    fair_value_base=fair_value_base,
                    resolved_up=bool(resolved_up),
                )
            )
    return observations


def _render_policy_v1_report(
    comparison: CrossHorizonComparison,
    calibration_summary: GoodOnlyCalibrationSummary,
) -> str:
    baseline_stack = next(
        result for result in comparison.stack_results if result.stack_name == "baseline_only"
    )
    light_stack = next(
        result
        for result in comparison.stack_results
        if result.stack_name == "baseline_plus_degraded_light"
    )
    medium_stack = next(
        result
        for result in comparison.stack_results
        if result.stack_name == "baseline_plus_degraded_light_gated_medium"
    )
    soak_baseline = next(
        item for item in baseline_stack.horizons if item.session_label == "soak_20h"
    )
    sufficient_buckets = [
        bucket.bucket_name
        for bucket in calibration_summary.buckets
        if bucket.support_flag == "sufficient"
    ]
    thin_buckets = [
        bucket.bucket_name
        for bucket in calibration_summary.buckets
        if bucket.support_flag == "thin"
    ]
    merge_required_buckets = [
        bucket.bucket_name
        for bucket in calibration_summary.buckets
        if bucket.support_flag == "merge_required"
    ]

    lines = [
        "# Policy V1 Report",
        "",
        "## Headline",
        (
            "- `baseline_only` is now the confirmed clean policy universe across the pinned "
            "6-hour, 12-hour, and 20-hour baselines."
        ),
        (
            f"- The strongest validation number is the 20-hour `baseline_only` result: "
            f"{soak_baseline.trade_count} trades, avg net edge "
            f"{soak_baseline.average_selected_net_edge}, "
            f"total PnL {soak_baseline.total_pnl}, avg ROI {soak_baseline.average_roi}."
        ),
        (
            "- `degraded_light` remains economically real but weaker and less stable as "
            "horizon extends, so it stays exploratory-only."
        ),
        (
            "- `degraded_medium` remains exploratory-only behind its explicit context gate, "
            "and `degraded_heavy` plus `unusable` remain excluded."
        ),
        "",
        "## Regime Map",
        "- `good`: baseline policy universe",
        "- `degraded_light`: exploratory second-tier overlay only",
        "- `degraded_medium`: exploratory only behind the context gate",
        "- `degraded_heavy`: excluded",
        "- `unusable`: excluded",
        "",
        "## Cross-Horizon Evidence",
        "### baseline_only",
    ]
    for item in baseline_stack.horizons:
        lines.append(
            f"- {item.session_label}: trades={item.trade_count}, hit_rate={item.hit_rate}, "
            f"avg_net_edge={item.average_selected_net_edge}, total_pnl={item.total_pnl}, "
            f"avg_roi={item.average_roi}, pnl_per_window={item.pnl_per_window}, "
            f"pnl_per_100_trades={item.pnl_per_100_trades}, "
            f"pnl_per_1000_snapshots={item.pnl_per_1000_snapshots}"
        )
    lines.extend(
        [
            "### baseline_plus_degraded_light",
        ]
    )
    for item in light_stack.horizons:
        lines.append(
            f"- {item.session_label}: trades={item.trade_count}, hit_rate={item.hit_rate}, "
            f"avg_net_edge={item.average_selected_net_edge}, total_pnl={item.total_pnl}, "
            f"avg_roi={item.average_roi}, pnl_per_window={item.pnl_per_window}, "
            f"pnl_per_100_trades={item.pnl_per_100_trades}, "
            f"pnl_per_1000_snapshots={item.pnl_per_1000_snapshots}"
        )
    lines.extend(
        [
            "### baseline_plus_degraded_light_gated_medium",
        ]
    )
    for item in medium_stack.horizons:
        lines.append(
            f"- {item.session_label}: trades={item.trade_count}, hit_rate={item.hit_rate}, "
            f"avg_net_edge={item.average_selected_net_edge}, total_pnl={item.total_pnl}, "
            f"avg_roi={item.average_roi}, pnl_per_window={item.pnl_per_window}, "
            f"pnl_per_100_trades={item.pnl_per_100_trades}, "
            f"pnl_per_1000_snapshots={item.pnl_per_1000_snapshots}"
        )
    lines.extend(
        [
            "",
            "## Stage 1 Good-Only Calibration",
            (
                f"- source sessions: {calibration_summary.total_session_count}, "
                f"calibration-eligible good windows: {calibration_summary.total_window_count}, "
                f"good snapshots: {calibration_summary.total_snapshot_count}"
            ),
            (
                "- calibration is intentionally coarse and uncertainty-aware; it should be read "
                "as a diagnostic correction layer, not a final curve fit."
            ),
            (
                f"- support buckets: sufficient="
                f"{calibration_summary.support_flag_counts.get('sufficient', 0)}, "
                f"thin={calibration_summary.support_flag_counts.get('thin', 0)}, "
                f"merge_required={calibration_summary.support_flag_counts.get('merge_required', 0)}"
            ),
            "",
            "### Bucket Table",
        ]
    )
    for bucket in calibration_summary.buckets:
        upper_operator = "]" if bucket.upper_bound_inclusive else ")"
        lines.append(
            f"- {bucket.bucket_name} "
            f"[{bucket.lower_bound_inclusive}, {bucket.upper_bound}{upper_operator}: "
            f"windows={bucket.window_count}, snapshots={bucket.snapshot_count}, "
            f"observed={bucket.observed_resolution_rate}, predicted={bucket.average_predicted_f}, "
            f"gap={bucket.calibration_gap}, "
            f"observed_ci=[{bucket.observed_resolution_rate_ci_low}, "
            f"{bucket.observed_resolution_rate_ci_high}], "
            f"gap_ci=[{bucket.calibration_gap_ci_low}, {bucket.calibration_gap_ci_high}], "
            f"support={bucket.support_flag}, action={bucket.recommended_action}, "
            f"merge_target={bucket.recommended_merge_bucket}"
        )
    lines.extend(
        [
            "",
            "## Calibration Read",
            (
                "- Bins marked `sufficient` have enough good-window support to use a coarse "
                "bucket-mean correction without pretending to know the curve finely."
            ),
            (
                "- Bins marked `thin` are informative but should be treated as provisional and "
                "left wide in any downstream correction layer."
            ),
            (
                "- Bins marked `merge_required` should be merged with their nearest supported "
                "neighbor or left uncorrected until more good-window data arrives."
            ),
            (
                "- Current sufficient bins: "
                f"{', '.join(sufficient_buckets) if sufficient_buckets else 'none'}."
            ),
            (
                f"- Current thin bins: {', '.join(thin_buckets) if thin_buckets else 'none'}."
            ),
            (
                "- Current merge-required bins: "
                f"{', '.join(merge_required_buckets) if merge_required_buckets else 'none'}."
            ),
            "",
            "## Decision",
            "- policy v1 stays frozen as baseline `good`, exploratory `degraded_light`, "
            "gated exploratory `degraded_medium`, excluded `degraded_heavy` and `unusable`.",
            "- Stage 1 calibration should start from `good_only` only.",
            "- The next 24-hour capture should validate this structure, not redefine it.",
        ]
    )
    return "\n".join(lines) + "\n"
if __name__ == "__main__":
    raise SystemExit(main())
