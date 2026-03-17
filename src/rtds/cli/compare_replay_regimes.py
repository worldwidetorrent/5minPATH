"""Compare replay outcomes across window-quality regimes for one capture session."""

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
from rtds.cli.replay_day import (
    _build_parser as _build_replay_parser,
)
from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.loader import load_chainlink_ticks
from rtds.replay.regime_compare import (
    COMPARISON_SLICE_DIMENSIONS,
    DEFAULT_REGIME_ORDER,
    build_regime_result,
    load_window_quality_rows,
    load_window_verdicts,
    regime_result_to_dict,
    render_regime_comparison_report,
)
from rtds.storage.writer import write_csv_rows, write_json_file, write_text_file

DEFAULT_OUTPUT_ROOT = "artifacts"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_regime_comparison(args)
    print(run_dir)
    return 0


def run_regime_comparison(args: argparse.Namespace) -> Path:
    """Run one multi-regime replay comparison for a finished capture session."""

    trade_date = date.fromisoformat(args.date)
    if not isinstance(args.session_id, str) or not args.session_id.strip():
        raise ValueError("--session-id is required for replay regime comparison")
    session_id = args.session_id.strip()
    run_dir = (
        Path(args.output_root)
        / "replay_compare"
        / trade_date.isoformat()
        / f"session_{session_id}"
        / f"run_{format_utc_compact(utc_now())}"
    )
    config = _build_effective_config(args, trade_date=trade_date, run_dir=run_dir)
    _write_effective_config(config)

    admission_summary_path = _resolve_admission_summary_path(
        path_token=args.admission_summary_path,
        data_root=Path(args.output_root),
        trade_date=trade_date,
        session_id=session_id,
    )
    window_quality_rows = load_window_quality_rows(admission_summary_path)
    window_verdict_by_window = load_window_verdicts(admission_summary_path)

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

    regime_results = [
        build_regime_result(
            evaluation_rows,
            window_verdict_by_window=window_verdict_by_window,
            regime_name=regime_name,
        )
        for regime_name in DEFAULT_REGIME_ORDER
    ]

    write_json_file(
        run_dir / "comparison_summary.json",
        {
            "trade_date": trade_date.isoformat(),
            "session_id": session_id,
            "admission_summary_path": str(admission_summary_path),
            "excluded_unusable_window_count": sum(
                1 for row in window_quality_rows.values() if row.window_verdict == "unusable"
            ),
            "regimes": [regime_result_to_dict(result) for result in regime_results],
        },
    )
    for result in regime_results:
        regime_dir = run_dir / "regimes" / result.regime_name
        write_json_file(regime_dir / "summary.json", regime_result_to_dict(result))
        for dimension in COMPARISON_SLICE_DIMENSIONS:
            write_csv_rows(
                regime_dir / "slices" / f"by_{dimension}.csv",
                list(result.slices[dimension]),
            )

    write_text_file(
        run_dir / "report" / "report.md",
        render_regime_comparison_report(
            regime_results,
            trade_date=trade_date.isoformat(),
            session_id=session_id,
            admission_summary_path=admission_summary_path,
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
    data_root: Path,
    trade_date: date,
    session_id: str,
) -> Path:
    if path_token:
        return Path(path_token)
    return (
        data_root
        / "collect"
        / f"date={trade_date.isoformat()}"
        / f"session={session_id}"
        / "admission_summary.json"
    )


if __name__ == "__main__":
    raise SystemExit(main())
