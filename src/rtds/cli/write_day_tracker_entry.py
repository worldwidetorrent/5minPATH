"""Write one day-block tracker entry from persisted analysis artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from rtds.cli.day_analysis_shared import build_day_tracker_entry, load_json
from rtds.storage.writer import write_json_file


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    tracker_path = run_write_day_tracker_entry(args)
    print(tracker_path)
    return 0


def run_write_day_tracker_entry(args: argparse.Namespace) -> Path:
    summary = load_json(args.summary_path)
    admission = load_json(args.admission_summary_path)
    policy_stack_summary = load_json(args.policy_stack_summary_path)
    calibrated_summary = load_json(args.calibrated_summary_path)
    calibration_summary = load_json(args.policy_v1_calibration_summary_path)
    cross_horizon_summary = load_json(args.policy_v1_cross_horizon_summary_path)
    shadow_summary = (
        load_json(args.shadow_summary_path)
        if args.shadow_summary_path and Path(args.shadow_summary_path).exists()
        else None
    )
    artifact_paths = {
        "summary_path": args.summary_path,
        "admission_summary_path": args.admission_summary_path,
        "policy_stack_summary_path": args.policy_stack_summary_path,
        "calibrated_summary_path": args.calibrated_summary_path,
        "policy_v1_calibration_summary_path": args.policy_v1_calibration_summary_path,
        "policy_v1_cross_horizon_summary_path": args.policy_v1_cross_horizon_summary_path,
    }
    if args.shadow_summary_path:
        artifact_paths["shadow_summary_path"] = args.shadow_summary_path
    entry = build_day_tracker_entry(
        capture_date=args.capture_date,
        session_id=args.session_id,
        summary=summary,
        admission=admission,
        policy_stack_summary=policy_stack_summary,
        calibrated_summary=calibrated_summary,
        calibration_summary=calibration_summary,
        cross_horizon_summary=cross_horizon_summary,
        artifact_paths=artifact_paths,
        shadow_summary=shadow_summary,
    )
    tracker_json_path = Path(args.tracker_json_path)
    tracker_jsonl_path = Path(args.tracker_jsonl_path)
    write_json_file(tracker_json_path, entry)
    tracker_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with tracker_jsonl_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(entry, sort_keys=True))
        handle.write("\n")
    return tracker_json_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-date", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--summary-path", required=True)
    parser.add_argument("--admission-summary-path", required=True)
    parser.add_argument("--policy-stack-summary-path", required=True)
    parser.add_argument("--calibrated-summary-path", required=True)
    parser.add_argument("--policy-v1-calibration-summary-path", required=True)
    parser.add_argument("--policy-v1-cross-horizon-summary-path", required=True)
    parser.add_argument("--tracker-json-path", required=True)
    parser.add_argument("--tracker-jsonl-path", required=True)
    parser.add_argument("--shadow-summary-path")
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
