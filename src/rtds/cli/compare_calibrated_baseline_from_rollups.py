"""Build cumulative calibrated-baseline comparison from per-session rollups."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.calibrated_baseline import (
    BaselineScenarioSummary,
    CalibratedBaselineComparison,
    CalibratedBaselineSessionComparison,
    calibrated_baseline_comparison_to_dict,
    render_calibrated_baseline_report,
)
from rtds.replay.policy_cross_horizon import load_cross_horizon_manifest
from rtds.storage.writer import write_json_file, write_text_file

DEFAULT_MANIFEST_PATH = "configs/baselines/analysis/policy_v1_calibrated_baseline.json"
DEFAULT_OUTPUT_ROOT = "artifacts"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_calibrated_baseline_from_rollups(args)
    print(run_dir)
    return 0


def run_calibrated_baseline_from_rollups(args: argparse.Namespace) -> Path:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    cross_horizon_manifest = load_cross_horizon_manifest(manifest["cross_horizon_manifest_path"])
    run_dir = (
        Path(args.output_root)
        / "replay_calibrated_baseline"
        / str(manifest["analysis_id"])
        / f"run_{format_utc_compact(utc_now())}"
    )
    write_json_file(run_dir / "config_effective.json", manifest)
    sessions = []
    for session in cross_horizon_manifest["sessions"]:
        rollup_path = (
            Path(args.output_root)
            / "session_rollups"
            / f"date={session['capture_date']}"
            / f"session={session['session_id']}"
            / "session_policy_rollup.json"
        )
        payload = json.loads(rollup_path.read_text(encoding="utf-8"))
        calibrated_session = payload["calibrated_baseline_only_summary"]
        sessions.append(
            CalibratedBaselineSessionComparison(
                session_label=str(payload["session_label"]),
                session_id=str(payload["session_id"]),
                capture_date=str(payload["capture_date"]),
                raw_summary=_baseline_summary_from_dict(calibrated_session["raw_summary"]),
                calibrated_summary=_baseline_summary_from_dict(
                    calibrated_session["calibrated_summary"]
                ),
                row_count=int(calibrated_session["row_count"]),
                calibration_bucket_counts={
                    str(key): int(value)
                    for key, value in calibrated_session["calibration_bucket_counts"].items()
                },
                calibration_support_flag_counts={
                    str(key): int(value)
                    for key, value in calibrated_session["calibration_support_flag_counts"].items()
                },
                calibration_applied_row_count=int(
                    calibrated_session["calibration_applied_row_count"]
                ),
                delta_trade_count=int(calibrated_session["delta_trade_count"]),
                delta_total_pnl=Decimal(str(calibrated_session["delta_total_pnl"])),
                delta_average_roi=_optional_decimal(calibrated_session["delta_average_roi"]),
                delta_average_selected_net_edge=_optional_decimal(
                    calibrated_session["delta_average_selected_net_edge"]
                ),
            )
        )
        write_json_file(
            run_dir / "sessions" / f"session_{session['session_id']}" / "summary.json",
            calibrated_session,
        )
    comparison = CalibratedBaselineComparison(
        analysis_id=str(manifest["analysis_id"]),
        description=str(manifest["description"]),
        cross_horizon_manifest_path=str(manifest["cross_horizon_manifest_path"]),
        replay_comparison_config_path=str(manifest["replay_comparison_config_path"]),
        calibration_config_path=str(manifest["calibration_config_path"]),
        calibration_summary_path=str(manifest["calibration_summary_path"]),
        baseline_stack_path=str(manifest["baseline_stack_path"]),
        admission_semantics_version=str(manifest["admission_semantics_version"]),
        policy_universe=str(manifest["policy_universe"]),
        oracle_source=str(manifest["oracle_source"]),
        sessions=tuple(sessions),
    )
    write_json_file(
        run_dir / "comparison_summary.json",
        calibrated_baseline_comparison_to_dict(comparison),
    )
    write_text_file(
        run_dir / "report" / "report.md",
        render_calibrated_baseline_report(comparison),
    )
    return run_dir


def _baseline_summary_from_dict(payload: dict[str, object]) -> BaselineScenarioSummary:
    return BaselineScenarioSummary(
        scenario_name=str(payload["scenario_name"]),
        snapshot_count=int(payload["snapshot_count"]),
        window_count=int(payload["window_count"]),
        trade_count=int(payload["trade_count"]),
        hit_rate=_optional_decimal(payload["hit_rate"]),
        average_selected_raw_edge=_optional_decimal(payload["average_selected_raw_edge"]),
        average_selected_net_edge=_optional_decimal(payload["average_selected_net_edge"]),
        total_pnl=Decimal(str(payload["total_pnl"])),
        average_roi=_optional_decimal(payload["average_roi"]),
        pnl_per_window=_optional_decimal(payload["pnl_per_window"]),
        pnl_per_1000_snapshots=_optional_decimal(payload["pnl_per_1000_snapshots"]),
        pnl_per_100_trades=_optional_decimal(payload["pnl_per_100_trades"]),
    )


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
