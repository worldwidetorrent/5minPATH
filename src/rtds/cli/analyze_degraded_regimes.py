"""Focused stress and context analysis for degraded replay regimes."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Sequence

from rtds.cli.replay_day import (
    _build_effective_config,
    _load_or_build_references,
    _load_or_build_snapshots,
    _write_effective_config,
    evaluate_snapshots,
)
from rtds.cli.replay_day import _build_parser as _build_replay_parser
from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.degraded_regime_analysis import (
    FOCUSED_DEGRADED_REGIME_ORDER,
    build_degraded_context_result,
    build_focused_degraded_stress_results,
    context_result_to_dict,
    execution_variant_result_to_dict,
    render_degraded_regime_report,
)
from rtds.replay.loader import load_chainlink_ticks
from rtds.replay.regime_compare import load_window_quality_rows
from rtds.replay.slices import DEFAULT_REPLAY_SLICE_POLICY
from rtds.storage.writer import write_json_file, write_text_file

DEFAULT_OUTPUT_ROOT = "artifacts"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_degraded_regime_analysis(args)
    print(run_dir)
    return 0


def run_degraded_regime_analysis(args: argparse.Namespace) -> Path:
    """Run the focused degraded-regime follow-up analysis for one capture session."""

    trade_date = date.fromisoformat(args.date)
    if not isinstance(args.session_id, str) or not args.session_id.strip():
        raise ValueError("--session-id is required for degraded regime analysis")
    session_id = args.session_id.strip()
    run_dir = (
        Path(args.output_root)
        / "replay_degraded_analysis"
        / trade_date.isoformat()
        / f"session_{session_id}"
        / f"run_{format_utc_compact(utc_now())}"
    )
    config = _build_effective_config(args, trade_date=trade_date, run_dir=run_dir)
    _write_effective_config(config)

    admission_summary_path = _resolve_admission_summary_path(
        path_token=args.admission_summary_path,
        output_root=Path(args.output_root),
        trade_date=trade_date,
        session_id=session_id,
    )
    window_quality_by_window = load_window_quality_rows(admission_summary_path)

    chainlink_ticks = load_chainlink_ticks(
        config.data_root,
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

    stress_results = build_focused_degraded_stress_results(
        evaluation_rows,
        window_quality_by_window=window_quality_by_window,
        replay_config=config,
    )
    context_results = tuple(
        build_degraded_context_result(
            evaluation_rows,
            window_quality_by_window=window_quality_by_window,
            regime_name=regime_name,
            slice_policy=DEFAULT_REPLAY_SLICE_POLICY,
        )
        for regime_name in FOCUSED_DEGRADED_REGIME_ORDER
    )
    write_json_file(
        run_dir / "degraded_analysis_summary.json",
        {
            "trade_date": trade_date.isoformat(),
            "session_id": session_id,
            "admission_summary_path": str(admission_summary_path),
            "stress_variants": [
                execution_variant_result_to_dict(item) for item in stress_results
            ],
            "context_decomposition": [
                context_result_to_dict(item) for item in context_results
            ],
        },
    )
    write_text_file(
        run_dir / "report" / "report.md",
        render_degraded_regime_report(
            trade_date=trade_date.isoformat(),
            session_id=session_id,
            admission_summary_path=str(admission_summary_path),
            stress_results=stress_results,
            context_results=context_results,
        ),
    )
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = _build_replay_parser()
    parser.description = __doc__
    parser.add_argument(
        "--admission-summary-path",
        help=(
            "Optional path to the capture admission summary. Defaults to "
            "artifacts/collect/date=.../session=.../admission_summary.json."
        ),
    )
    parser.set_defaults(output_root=DEFAULT_OUTPUT_ROOT)
    return parser


def _resolve_admission_summary_path(
    *,
    path_token: str | None,
    output_root: Path,
    trade_date: date,
    session_id: str,
) -> Path:
    if path_token:
        return Path(path_token)
    return (
        output_root
        / "collect"
        / f"date={trade_date.isoformat()}"
        / f"session={session_id}"
        / "admission_summary.json"
    )


if __name__ == "__main__":
    raise SystemExit(main())
