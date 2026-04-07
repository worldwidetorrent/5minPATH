"""Write per-session policy and shadow rollups from existing artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from rtds.cli.day_analysis_shared import (
    build_shadow_quick_stage_a,
    classify_shadow_baseline,
    load_json,
)
from rtds.replay.session_rollups import (
    build_session_policy_rollup,
    build_session_shadow_rollup,
)
from rtds.storage.writer import write_json_file


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    rollup_root = run_write_session_rollups(args)
    print(rollup_root)
    return 0


def run_write_session_rollups(args: argparse.Namespace) -> Path:
    policy_stack_summary = load_json(args.policy_stack_summary_path)
    calibrated_session_summary = load_json(args.calibrated_session_summary_path)
    shadow_summary = (
        load_json(args.shadow_summary_path)
        if args.shadow_summary_path and Path(args.shadow_summary_path).exists()
        else None
    )
    rollup_root = (
        Path(args.output_root)
        / "session_rollups"
        / f"date={args.capture_date}"
        / f"session={args.session_id}"
    )
    write_json_file(
        rollup_root / "session_policy_rollup.json",
        build_session_policy_rollup(
            capture_date=args.capture_date,
            session_id=args.session_id,
            session_label=args.session_label or args.session_id,
            policy_stack_summary=policy_stack_summary,
            calibrated_session_summary=calibrated_session_summary,
        ),
    )
    if shadow_summary is not None:
        shadow_clean_baseline, shadow_reason = classify_shadow_baseline(shadow_summary)
        quick_stage_a = build_shadow_quick_stage_a(
            shadow_summary,
            decisions_path=args.shadow_decisions_path,
        )
        write_json_file(
            rollup_root / "session_shadow_rollup.json",
            build_session_shadow_rollup(
                capture_date=args.capture_date,
                session_id=args.session_id,
                shadow_clean_baseline=shadow_clean_baseline,
                shadow_reason=shadow_reason,
                quick_stage_a=quick_stage_a,
            ),
        )
    return rollup_root


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-date", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--session-label")
    parser.add_argument("--policy-stack-summary-path", required=True)
    parser.add_argument("--calibrated-session-summary-path", required=True)
    parser.add_argument("--shadow-summary-path")
    parser.add_argument("--shadow-decisions-path")
    parser.add_argument("--output-root", default="artifacts")
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
