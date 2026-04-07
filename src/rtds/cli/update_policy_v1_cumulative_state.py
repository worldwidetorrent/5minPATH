"""Update the versioned cumulative policy-v1 calibration state incrementally."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from rtds.cli.build_policy_v1_baseline import _render_policy_v1_report
from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.calibration_state import (
    build_cumulative_calibration_state,
    cumulative_calibration_state_to_dict,
    update_cumulative_calibration_state,
    write_cumulative_calibration_state,
)
from rtds.replay.good_only_calibration import (
    load_good_only_calibration_config,
)
from rtds.replay.policy_cross_horizon import (
    build_cross_horizon_comparison,
    cross_horizon_comparison_to_dict,
    load_cross_horizon_manifest,
)
from rtds.replay.session_rollups import (
    load_session_calibration_rollup,
    session_calibration_rollup_from_payload,
)
from rtds.storage.writer import write_json_file, write_text_file

DEFAULT_OUTPUT_ROOT = "artifacts"
DEFAULT_CROSS_HORIZON_MANIFEST = "configs/baselines/analysis/policy_v1_cross_horizon.json"
DEFAULT_CALIBRATION_CONFIG = "configs/replay/calibration_good_only_v1.json"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_update_policy_v1_cumulative_state(args)
    print(run_dir)
    return 0


def run_update_policy_v1_cumulative_state(args: argparse.Namespace) -> Path:
    output_root = Path(args.output_root)
    state_path = output_root / "policy_v1" / "state" / "good_only_calibration_state_v1.json"
    manifest = load_cross_horizon_manifest(args.cross_horizon_manifest)
    comparison = build_cross_horizon_comparison(manifest)
    calibration_config = load_good_only_calibration_config(args.calibration_config)

    if state_path.exists():
        state_payload = json.loads(state_path.read_text(encoding="utf-8"))
        existing_rollups = tuple(
            session_calibration_rollup_from_payload(item)
            for item in state_payload["session_rollups"]
        )
        current_state = build_cumulative_calibration_state(
            existing_rollups,
            config=calibration_config,
            calibration_config_path=str(args.calibration_config),
            source_manifest_path=str(args.cross_horizon_manifest),
            comparison_config_path=str(manifest["comparison_config_path"]),
        )
    else:
        current_state = None

    if args.initialize_from_manifest and current_state is None:
        incoming_rollups = [
            load_session_calibration_rollup(
                output_root
                / "session_rollups"
                / f"date={session['capture_date']}"
                / f"session={session['session_id']}"
                / "session_calibration_rollup.json"
            )
            for session in manifest["sessions"]
        ]
        next_state = build_cumulative_calibration_state(
            incoming_rollups,
            config=calibration_config,
            calibration_config_path=str(args.calibration_config),
            source_manifest_path=str(args.cross_horizon_manifest),
            comparison_config_path=str(manifest["comparison_config_path"]),
        )
    else:
        if args.session_rollup_path is None:
            raise ValueError("--session-rollup-path is required unless initializing from manifest")
        incoming_rollup = load_session_calibration_rollup(args.session_rollup_path)
        next_state = update_cumulative_calibration_state(
            current_state,
            incoming_rollup=incoming_rollup,
            config=calibration_config,
            calibration_config_path=str(args.calibration_config),
            source_manifest_path=str(args.cross_horizon_manifest),
            comparison_config_path=str(manifest["comparison_config_path"]),
        )

    write_cumulative_calibration_state(state_path, next_state)

    run_dir = output_root / "policy_v1" / f"run_{format_utc_compact(utc_now())}"
    write_json_file(
        run_dir / "config_effective.json",
        {
            "cross_horizon_manifest": str(args.cross_horizon_manifest),
            "calibration_config": str(args.calibration_config),
            "comparison_config_path": str(manifest["comparison_config_path"]),
            "session_ids": [str(item["session_id"]) for item in manifest["sessions"]],
            "policy_universe": calibration_config.policy_universe,
            "calibration_id": calibration_config.calibration_id,
            "source_mode": "incremental_state",
            "state_path": str(state_path),
            "state_version": next_state.state_version,
        },
    )
    write_json_file(
        run_dir / "cross_horizon_summary.json",
        cross_horizon_comparison_to_dict(comparison),
    )
    write_json_file(
        run_dir / "good_only_calibration_summary.json",
        cumulative_calibration_state_to_dict(next_state)["summary"],
    )
    write_text_file(
        run_dir / "report" / "report.md",
        _render_policy_v1_report(comparison, next_state.summary),
    )
    return run_dir
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cross-horizon-manifest", default=DEFAULT_CROSS_HORIZON_MANIFEST)
    parser.add_argument("--calibration-config", default=DEFAULT_CALIBRATION_CONFIG)
    parser.add_argument("--session-rollup-path")
    parser.add_argument("--initialize-from-manifest", action="store_true")
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
