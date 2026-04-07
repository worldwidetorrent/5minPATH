"""Build one cheap per-session fast-lane report without cumulative refresh."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from rtds.cli.day_analysis_shared import (
    build_shadow_quick_stage_a,
    classify_shadow_baseline,
    load_json,
)
from rtds.storage.writer import write_json_file, write_text_file


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_build_day_fast_report(args)
    print(run_dir)
    return 0


def run_build_day_fast_report(args: argparse.Namespace) -> Path:
    summary = load_json(args.summary_path)
    admission = load_json(args.admission_summary_path)
    policy_stack_summary = load_json(args.policy_stack_summary_path)
    calibrated_session_summary = load_json(args.calibrated_session_summary_path)
    shadow_summary = (
        load_json(args.shadow_summary_path)
        if args.shadow_summary_path and Path(args.shadow_summary_path).exists()
        else None
    )
    run_dir = (
        Path(args.output_root)
        / "day_fast_lane"
        / f"date={args.capture_date}"
        / f"session={args.session_id}"
    )
    baseline_only = next(
        stack for stack in policy_stack_summary["stacks"] if stack["stack_name"] == "baseline_only"
    )
    shadow_clean_baseline, shadow_reason = classify_shadow_baseline(shadow_summary)
    shadow_quick_read = (
        build_shadow_quick_stage_a(
            shadow_summary,
            decisions_path=args.shadow_decisions_path,
        )
        if shadow_summary is not None
        else None
    )
    payload = {
        "capture_date": args.capture_date,
        "session_id": args.session_id,
        "capture_passed": summary["session_diagnostics"]["termination_reason"] == "completed",
        "admission_verdict": admission["verdict"],
        "snapshot_eligible_sample_ratio": admission["snapshot_eligibility"][
            "snapshot_eligible_sample_ratio"
        ],
        "window_verdict_counts": admission["polymarket_continuity"]["window_verdict_counts"],
        "raw_baseline_only": baseline_only,
        "calibrated_baseline_only": calibrated_session_summary,
        "shadow_clean_baseline": shadow_clean_baseline,
        "shadow_reason": shadow_reason,
        "shadow_quick_stage_a": shadow_quick_read,
        "artifact_paths": {
            "summary_path": args.summary_path,
            "admission_summary_path": args.admission_summary_path,
            "policy_stack_summary_path": args.policy_stack_summary_path,
            "calibrated_session_summary_path": args.calibrated_session_summary_path,
            "shadow_summary_path": args.shadow_summary_path,
            "shadow_decisions_path": args.shadow_decisions_path,
        },
    }
    write_json_file(run_dir / "summary.json", payload)
    write_text_file(run_dir / "report" / "report.md", _render_report(payload))
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-date", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--summary-path", required=True)
    parser.add_argument("--admission-summary-path", required=True)
    parser.add_argument("--policy-stack-summary-path", required=True)
    parser.add_argument("--calibrated-session-summary-path", required=True)
    parser.add_argument("--shadow-summary-path")
    parser.add_argument("--shadow-decisions-path")
    parser.add_argument("--output-root", default="artifacts")
    return parser


def _render_report(payload: dict[str, object]) -> str:
    raw = payload["raw_baseline_only"]
    calibrated = payload["calibrated_baseline_only"]
    shadow = payload["shadow_quick_stage_a"]
    lines = [
        f"# Day Fast Lane — {payload['session_id']}",
        "",
        f"- capture_passed: `{payload['capture_passed']}`",
        f"- admission_verdict: `{payload['admission_verdict']}`",
        f"- snapshot_eligible_sample_ratio: {payload['snapshot_eligible_sample_ratio']}",
        f"- window_verdict_counts: {payload['window_verdict_counts']}",
        "",
        "## Raw Baseline Only",
        f"- trades: {raw['trade_count']}",
        f"- hit_rate: {raw['hit_rate']}",
        f"- avg_net_edge: {raw['average_selected_net_edge']}",
        f"- total_pnl: {raw['total_pnl']}",
        f"- avg_roi: {raw['average_roi']}",
        "",
        "## Calibrated Baseline Only",
        f"- raw_total_pnl: {calibrated['raw_summary']['total_pnl']}",
        f"- calibrated_total_pnl: {calibrated['calibrated_summary']['total_pnl']}",
        f"- delta_total_pnl: {calibrated['delta_total_pnl']}",
        f"- raw_average_roi: {calibrated['raw_summary']['average_roi']}",
        f"- calibrated_average_roi: {calibrated['calibrated_summary']['average_roi']}",
        f"- delta_average_roi: {calibrated['delta_average_roi']}",
        "",
        "## Shadow",
        f"- shadow_clean_baseline: {payload['shadow_clean_baseline']}",
        f"- shadow_reason: {payload['shadow_reason']}",
    ]
    if shadow is not None:
        lines.extend(
            [
                f"- decision_count: {shadow['decision_count']}",
                f"- actionable_decision_count: {shadow['actionable_decision_count']}",
                f"- fair_value_non_null_count: {shadow.get('fair_value_non_null_count')}",
                (
                    "- calibrated_fair_value_non_null_count: "
                    f"{shadow.get('calibrated_fair_value_non_null_count')}"
                ),
                f"- three_trusted_venue_row_count: {shadow.get('three_trusted_venue_row_count')}",
                f"- three_trusted_venue_rate: {shadow.get('three_trusted_venue_rate')}",
                (
                    "- actionable_given_three_trusted_rate: "
                    f"{shadow.get('actionable_given_three_trusted_rate')}"
                ),
                f"- no_trade_reason_counts: {shadow['no_trade_reason_counts']}",
            ]
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
