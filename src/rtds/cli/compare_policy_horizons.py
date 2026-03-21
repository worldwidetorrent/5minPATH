"""Compare policy-stack results across pinned 6h/12h/20h capture horizons."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.policy_cross_horizon import (
    build_cross_horizon_comparison,
    cross_horizon_comparison_to_dict,
    load_cross_horizon_manifest,
    render_cross_horizon_report,
)
from rtds.storage.writer import write_json_file, write_text_file

DEFAULT_MANIFEST_PATH = "configs/baselines/analysis/policy_v1_cross_horizon.json"
DEFAULT_OUTPUT_ROOT = "artifacts"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_cross_horizon_policy_comparison(args)
    print(run_dir)
    return 0


def run_cross_horizon_policy_comparison(args: argparse.Namespace) -> Path:
    manifest = load_cross_horizon_manifest(args.manifest)
    comparison = build_cross_horizon_comparison(manifest)
    run_dir = (
        Path(args.output_root)
        / "replay_policy_horizon"
        / str(manifest["analysis_id"])
        / f"run_{format_utc_compact(utc_now())}"
    )
    write_json_file(
        run_dir / "comparison_summary.json",
        cross_horizon_comparison_to_dict(comparison),
    )
    write_text_file(
        run_dir / "report" / "report.md",
        render_cross_horizon_report(comparison),
    )
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        default=DEFAULT_MANIFEST_PATH,
        help="Path to the pinned cross-horizon analysis manifest.",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        help="Base artifact root for comparison outputs.",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
