"""Compare raw-vs-calibrated baseline-only replay for one capture session."""

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
    build_calibrated_baseline_session_comparison,
    load_frozen_calibration_runtime,
)
from rtds.replay.good_only_calibration import CalibrationObservation
from rtds.replay.loader import load_chainlink_ticks
from rtds.replay.regime_compare import (
    REGIME_GOOD_ONLY,
    filter_evaluation_rows_for_regime,
    load_window_quality_rows,
)
from rtds.replay.session_rollups import (
    build_session_calibration_rollup,
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
    run_dir = run_calibrated_session_comparison(args)
    print(run_dir)
    return 0


def run_calibrated_session_comparison(args: argparse.Namespace) -> Path:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    runtime = load_frozen_calibration_runtime(
        config_path=manifest["calibration_config_path"],
        summary_path=manifest["calibration_summary_path"],
    )
    session = _SessionInput(
        label=args.session_label or args.session_id,
        session_id=args.session_id,
        capture_date=args.capture_date,
        admission_summary_path=args.admission_summary_path
        or (
            Path(args.output_root)
            / "collect"
            / f"date={args.capture_date}"
            / f"session={args.session_id}"
            / "admission_summary.json"
        ).as_posix(),
    )
    run_dir = (
        Path(args.output_root)
        / "replay_calibrated_session"
        / str(manifest["analysis_id"])
        / f"date={args.capture_date}"
        / f"session_{args.session_id}"
        / f"run_{format_utc_compact(utc_now())}"
    )
    write_json_file(
        run_dir / "config_effective.json",
        {
            "analysis_id": manifest["analysis_id"],
            "session_id": session.session_id,
            "capture_date": session.capture_date,
            "session_label": session.label,
            "replay_comparison_config_path": manifest["replay_comparison_config_path"],
            "calibration_config_path": manifest["calibration_config_path"],
            "calibration_summary_path": manifest["calibration_summary_path"],
            "admission_summary_path": session.admission_summary_path,
        },
    )

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
            window_id: row.window_verdict for window_id, row in window_quality_by_window.items()
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

    write_jsonl_rows(run_dir / "rows.jsonl", row_records)
    write_json_file(run_dir / "summary.json", session_result)
    good_window_count = sum(
        1 for row in window_quality_by_window.values() if row.window_verdict == REGIME_GOOD_ONLY
    )
    calibration_observations = [
        CalibrationObservation(
            session_label=session.label,
            session_id=session.session_id,
            capture_date=session.capture_date,
            window_id=row.snapshot.window_id,
            fair_value_base=row.fair_value.fair_value_base,
            resolved_up=bool(row.labeled_snapshot.label.resolved_up),
        )
        for row in filtered_rows
        if row.fair_value.fair_value_base is not None
        and row.labeled_snapshot.label.resolved_up is not None
    ]
    rollup_root = (
        Path(args.output_root)
        / "session_rollups"
        / f"date={session.capture_date}"
        / f"session={session.session_id}"
    )
    write_json_file(
        rollup_root / "session_calibration_rollup.json",
        build_session_calibration_rollup(
            calibration_observations,
            session_label=session.label,
            session_id=session.session_id,
            capture_date=session.capture_date,
            good_window_count=good_window_count,
            config=runtime.config,
        ),
    )
    write_text_file(run_dir / "report" / "report.md", _render_session_report(session_result))
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-date", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--session-label")
    parser.add_argument("--admission-summary-path")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    return parser


def _render_session_report(session_result: object) -> str:
    summary = session_result
    raw = summary.raw_summary
    calibrated = summary.calibrated_summary
    lines = [
        f"# Calibrated Session Comparison — {summary.session_label}",
        "",
        f"- session_id: `{summary.session_id}`",
        f"- capture_date: `{summary.capture_date}`",
        f"- raw total_pnl: {raw.total_pnl}",
        f"- calibrated total_pnl: {calibrated.total_pnl}",
        f"- delta_total_pnl: {summary.delta_total_pnl}",
        f"- raw average_roi: {raw.average_roi}",
        f"- calibrated average_roi: {calibrated.average_roi}",
        f"- delta_average_roi: {summary.delta_average_roi}",
        f"- raw trade_count: {raw.trade_count}",
        f"- calibrated trade_count: {calibrated.trade_count}",
        f"- delta_trade_count: {summary.delta_trade_count}",
        f"- calibration_applied_row_count: {summary.calibration_applied_row_count}",
        f"- calibration_support_flag_counts: {summary.calibration_support_flag_counts}",
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
