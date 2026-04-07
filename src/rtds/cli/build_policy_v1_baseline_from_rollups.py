"""Build policy-v1 cumulative refresh from per-session rollups."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from rtds.cli.build_policy_v1_baseline import _render_policy_v1_report
from rtds.core.time import format_utc_compact, utc_now
from rtds.replay.good_only_calibration import (
    build_good_only_calibration_summary_from_rollups,
    good_only_calibration_summary_to_dict,
    load_good_only_calibration_config,
)
from rtds.replay.policy_cross_horizon import (
    build_cross_horizon_comparison,
    cross_horizon_comparison_to_dict,
    load_cross_horizon_manifest,
)
from rtds.replay.session_rollups import SessionCalibrationRollup
from rtds.storage.writer import write_json_file, write_text_file

DEFAULT_OUTPUT_ROOT = "artifacts"
DEFAULT_CROSS_HORIZON_MANIFEST = "configs/baselines/analysis/policy_v1_cross_horizon.json"
DEFAULT_CALIBRATION_CONFIG = "configs/replay/calibration_good_only_v1.json"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_dir = run_policy_v1_baseline_from_rollups(args)
    print(run_dir)
    return 0


def run_policy_v1_baseline_from_rollups(args: argparse.Namespace) -> Path:
    run_dir = (
        Path(args.output_root)
        / "policy_v1"
        / f"run_{format_utc_compact(utc_now())}"
    )
    manifest = load_cross_horizon_manifest(args.cross_horizon_manifest)
    comparison = build_cross_horizon_comparison(manifest)
    calibration_config = load_good_only_calibration_config(args.calibration_config)
    session_rollups = []
    for session in manifest["sessions"]:
        rollup_path = (
            Path(args.output_root)
            / "session_rollups"
            / f"date={session['capture_date']}"
            / f"session={session['session_id']}"
            / "session_calibration_rollup.json"
        )
        session_rollups.append(_load_session_calibration_rollup(rollup_path))
    calibration_summary = build_good_only_calibration_summary_from_rollups(
        session_rollups,
        config=calibration_config,
        source_manifest_path=str(args.cross_horizon_manifest),
        comparison_config_path=str(manifest["comparison_config_path"]),
    )
    write_json_file(
        run_dir / "config_effective.json",
        {
            "cross_horizon_manifest": str(args.cross_horizon_manifest),
            "calibration_config": str(args.calibration_config),
            "comparison_config_path": str(manifest["comparison_config_path"]),
            "session_ids": [str(item["session_id"]) for item in manifest["sessions"]],
            "policy_universe": calibration_config.policy_universe,
            "calibration_id": calibration_config.calibration_id,
            "source_mode": "session_rollups",
        },
    )
    write_json_file(
        run_dir / "cross_horizon_summary.json",
        cross_horizon_comparison_to_dict(comparison),
    )
    write_json_file(
        run_dir / "good_only_calibration_summary.json",
        good_only_calibration_summary_to_dict(calibration_summary),
    )
    write_text_file(
        run_dir / "report" / "report.md",
        _render_policy_v1_report(comparison, calibration_summary),
    )
    return run_dir


def _load_session_calibration_rollup(path: Path) -> SessionCalibrationRollup:
    payload = json.loads(path.read_text(encoding="utf-8"))
    from rtds.replay.session_rollups import (
        CalibrationBucketRollup,
        CalibrationWindowRollup,
    )

    return SessionCalibrationRollup(
        session_label=str(payload["session_label"]),
        session_id=str(payload["session_id"]),
        capture_date=str(payload["capture_date"]),
        good_window_count=int(payload["good_window_count"]),
        calibration_eligible_snapshot_count=int(payload["calibration_eligible_snapshot_count"]),
        calibration_eligible_window_count=int(payload["calibration_eligible_window_count"]),
        calibration_eligible_window_ids=tuple(
            str(item) for item in payload["calibration_eligible_window_ids"]
        ),
        bucket_rollups=tuple(
            CalibrationBucketRollup(
                bucket_name=str(bucket["bucket_name"]),
                lower_bound_inclusive=Decimal(str(bucket["lower_bound_inclusive"])),
                upper_bound=Decimal(str(bucket["upper_bound"])),
                upper_bound_inclusive=bool(bucket["upper_bound_inclusive"]),
                snapshot_count=int(bucket["snapshot_count"]),
                window_count=int(bucket["window_count"]),
                resolved_up_count=int(bucket["resolved_up_count"]),
                resolved_down_count=int(bucket["resolved_down_count"]),
                predicted_f_sum=Decimal(str(bucket["predicted_f_sum"])),
                average_predicted_f=(
                    None
                    if bucket["average_predicted_f"] is None
                    else Decimal(str(bucket["average_predicted_f"]))
                ),
                window_rollups=tuple(
                    CalibrationWindowRollup(
                        bucket_name=str(window["bucket_name"]),
                        session_label=str(window["session_label"]),
                        session_id=str(window["session_id"]),
                        capture_date=str(window["capture_date"]),
                        window_id=str(window["window_id"]),
                        snapshot_count=int(window["snapshot_count"]),
                        resolved_up_count=int(window["resolved_up_count"]),
                        predicted_f_sum=Decimal(str(window["predicted_f_sum"])),
                    )
                    for window in bucket["window_rollups"]
                ),
            )
            for bucket in payload["bucket_rollups"]
        ),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cross-horizon-manifest", default=DEFAULT_CROSS_HORIZON_MANIFEST)
    parser.add_argument("--calibration-config", default=DEFAULT_CALIBRATION_CONFIG)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
