"""Compare raw-vs-calibrated baseline-only replay across the pinned policy-v1 horizons."""

from __future__ import annotations

import argparse
import json
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
from rtds.replay.calibrated_baseline import (
    CalibratedBaselineComparison,
    build_calibrated_baseline_session_comparison,
    calibrated_baseline_comparison_to_dict,
    load_frozen_calibration_runtime,
    render_calibrated_baseline_report,
)
from rtds.replay.loader import load_chainlink_ticks
from rtds.replay.policy_cross_horizon import load_cross_horizon_manifest
from rtds.replay.regime_compare import (
    REGIME_GOOD_ONLY,
    filter_evaluation_rows_for_regime,
    load_window_quality_rows,
)
from rtds.storage.writer import write_json_file, write_jsonl_rows, write_text_file

DEFAULT_MANIFEST_PATH = "configs/baselines/analysis/policy_v1_calibrated_baseline.json"
DEFAULT_OUTPUT_ROOT = "artifacts"
DEFAULT_DATA_ROOT = "data"


@dataclass(slots=True, frozen=True)
class _SessionInput:
    label: str
    session_id: str
    capture_date: str
    admission_summary_path: str


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_calibrated_baseline_comparison(args)
    print(run_dir)
    return 0


def run_calibrated_baseline_comparison(args: argparse.Namespace) -> Path:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    cross_horizon_manifest = load_cross_horizon_manifest(manifest["cross_horizon_manifest_path"])
    runtime = load_frozen_calibration_runtime(
        config_path=manifest["calibration_config_path"],
        summary_path=manifest["calibration_summary_path"],
    )
    run_dir = (
        Path(args.output_root)
        / "replay_calibrated_baseline"
        / str(manifest["analysis_id"])
        / f"run_{format_utc_compact(utc_now())}"
    )
    write_json_file(run_dir / "config_effective.json", manifest)

    session_inputs = tuple(
        _SessionInput(
            label=str(item["label"]),
            session_id=str(item["session_id"]),
            capture_date=str(item["capture_date"]),
            admission_summary_path=str(item["admission_summary_path"]),
        )
        for item in cross_horizon_manifest["sessions"]
    )

    session_results = []
    for session in session_inputs:
        trade_date = date.fromisoformat(session.capture_date)
        config = _build_effective_config(
            argparse.Namespace(
                date=session.capture_date,
                session_id=session.session_id,
                data_root=str(args.data_root),
                output_root=str(args.output_root),
                config=manifest["replay_comparison_config_path"],
                rebuild_snapshots=True,
                rebuild_reference=True,
                min_seconds_remaining=0,
                max_seconds_remaining=300,
                edge_threshold="0",
                fee_config=None,
                slippage_config=None,
            ),
            trade_date=trade_date,
            run_dir=run_dir / "_tmp" / f"session_{session.session_id}",
        )
        window_quality_by_window = load_window_quality_rows(session.admission_summary_path)
        chainlink_ticks = load_chainlink_ticks(
            Path(args.data_root),
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
        session_result, row_records = build_calibrated_baseline_session_comparison(
            filtered_rows,
            session_label=session.label,
            session_id=session.session_id,
            capture_date=session.capture_date,
            replay_config=config,
            runtime=runtime,
        )
        session_results.append(session_result)
        write_jsonl_rows(
            run_dir / "sessions" / f"session_{session.session_id}" / "rows.jsonl",
            row_records,
        )
        write_json_file(
            run_dir / "sessions" / f"session_{session.session_id}" / "summary.json",
            session_result,
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
        sessions=tuple(session_results),
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
