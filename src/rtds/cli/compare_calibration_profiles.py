"""Build a side-by-side comparison of two calibration profile artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.calibration_profile_compare import (
    DEFAULT_DIMENSIONS,
    build_calibration_profile_comparison,
    calibration_profile_comparison_to_dict,
    load_calibration_profile,
    render_calibration_profile_comparison,
)
from rtds.storage.writer import write_json_file, write_text_file

DEFAULT_LEFT_PROFILE = (
    "artifacts/diagnostics/12h_calibration_failure_profile/profile.json"
)
DEFAULT_RIGHT_PROFILE = (
    "artifacts/diagnostics/24h_calibration_success_profile/profile.json"
)
DEFAULT_OUTPUT_ROOT = "artifacts"
DEFAULT_BUCKETS = ("far_up", "lean_up", "far_down")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_calibration_profile_comparison(args)
    print(run_dir)
    return 0


def run_calibration_profile_comparison(args: argparse.Namespace) -> Path:
    left = load_calibration_profile(args.left_profile)
    right = load_calibration_profile(args.right_profile)
    comparison = build_calibration_profile_comparison(
        left=left,
        right=right,
        buckets=tuple(args.buckets),
        dimensions=tuple(args.dimensions),
        analysis_id=args.analysis_id,
        description=args.description,
    )
    run_dir = (
        Path(args.output_root)
        / "diagnostics"
        / str(args.analysis_id)
        / f"run_{format_utc_compact(utc_now())}"
    )
    write_json_file(
        run_dir / "comparison_summary.json",
        calibration_profile_comparison_to_dict(comparison),
    )
    write_text_file(
        run_dir / "report.md",
        render_calibration_profile_comparison(comparison),
    )
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left-profile", default=DEFAULT_LEFT_PROFILE)
    parser.add_argument("--right-profile", default=DEFAULT_RIGHT_PROFILE)
    parser.add_argument(
        "--analysis-id",
        default="12h_vs_24h_far_up_lean_up_diagnosis",
    )
    parser.add_argument(
        "--description",
        default=(
            "Direct side-by-side comparison of the 12-hour calibration anomaly "
            "and the 24-hour validation session."
        ),
    )
    parser.add_argument(
        "--buckets",
        nargs="+",
        default=list(DEFAULT_BUCKETS),
    )
    parser.add_argument(
        "--dimensions",
        nargs="+",
        default=list(DEFAULT_DIMENSIONS),
    )
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
