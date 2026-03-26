#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <capture-date> <session-id>" >&2
  exit 2
fi

CAPTURE_DATE="$1"
SESSION_ID="$2"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}src"

OUTPUT_ROOT="${OUTPUT_ROOT:-artifacts}"
POLL_SECONDS="${POLL_SECONDS:-30}"
CALIBRATED_MANIFEST="${CALIBRATED_MANIFEST:-configs/baselines/analysis/policy_v1_calibrated_baseline.json}"
POLICY_STACK_SESSION_ROOT="${OUTPUT_ROOT}/replay_policy_stack/${CAPTURE_DATE}/session_${SESSION_ID}"
TRACKER_ROOT="${OUTPUT_ROOT}/day_block_tracker"
TRACKER_JSON="${TRACKER_ROOT}/session_${SESSION_ID}.json"
TRACKER_JSONL="${TRACKER_ROOT}/entries.jsonl"

mkdir -p "$TRACKER_ROOT" logs

latest_run_dir() {
  local root="$1"
  python3 - "$root" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
if not root.exists():
    sys.exit(1)
runs = sorted(
    (path for path in root.glob("run_*") if path.is_dir()),
    key=lambda path: path.name,
)
if not runs:
    sys.exit(1)
print(runs[-1])
PY
}

wait_for_latest_run_dir() {
  local root="$1"
  while true; do
    if run_dir="$(latest_run_dir "$root" 2>/dev/null)"; then
      echo "$run_dir"
      return 0
    fi
    sleep "$POLL_SECONDS"
  done
}

wait_for_file() {
  local path="$1"
  while [[ ! -f "$path" ]]; do
    sleep "$POLL_SECONDS"
  done
}

echo "waiting for Day analysis prerequisites for session ${SESSION_ID}"

POLICY_STACK_RUN_DIR="$(wait_for_latest_run_dir "$POLICY_STACK_SESSION_ROOT")"
POLICY_STACK_SUMMARY="${POLICY_STACK_RUN_DIR}/policy_stack_summary.json"
echo "waiting for policy stack summary: ${POLICY_STACK_SUMMARY}"
wait_for_file "$POLICY_STACK_SUMMARY"
ln -sfn "$(basename "$POLICY_STACK_RUN_DIR")" "${POLICY_STACK_SESSION_ROOT}/run_latest"

echo "running calibrated baseline replay"
python3 -m rtds.cli.compare_calibrated_baseline --manifest "$CALIBRATED_MANIFEST"

CALIBRATED_ROOT="${OUTPUT_ROOT}/replay_calibrated_baseline/policy-v1-good-only-calibrated-baseline"
CALIBRATED_RUN_DIR="$(wait_for_latest_run_dir "$CALIBRATED_ROOT")"
CALIBRATED_SUMMARY="${CALIBRATED_RUN_DIR}/comparison_summary.json"
if [[ ! -f "$CALIBRATED_SUMMARY" ]]; then
  echo "missing calibrated baseline summary: ${CALIBRATED_SUMMARY}" >&2
  exit 1
fi

echo "refreshing policy-v1 support and calibration report"
python3 -m rtds.cli.build_policy_v1_baseline

POLICY_V1_ROOT="${OUTPUT_ROOT}/policy_v1"
POLICY_V1_RUN_DIR="$(wait_for_latest_run_dir "$POLICY_V1_ROOT")"
POLICY_V1_CALIBRATION="${POLICY_V1_RUN_DIR}/good_only_calibration_summary.json"
POLICY_V1_HORIZON="${POLICY_V1_RUN_DIR}/cross_horizon_summary.json"
if [[ ! -f "$POLICY_V1_CALIBRATION" || ! -f "$POLICY_V1_HORIZON" ]]; then
  echo "missing policy v1 outputs in ${POLICY_V1_RUN_DIR}" >&2
  exit 1
fi

echo "writing day tracker entry"
python3 - "$CAPTURE_DATE" "$SESSION_ID" "$POLICY_STACK_SUMMARY" "$CALIBRATED_SUMMARY" "$POLICY_V1_CALIBRATION" "$POLICY_V1_HORIZON" "$TRACKER_JSON" "$TRACKER_JSONL" <<'PY'
import json
import sys
from pathlib import Path

capture_date = sys.argv[1]
session_id = sys.argv[2]
policy_stack_summary_path = Path(sys.argv[3])
calibrated_summary_path = Path(sys.argv[4])
calibration_summary_path = Path(sys.argv[5])
cross_horizon_summary_path = Path(sys.argv[6])
tracker_json_path = Path(sys.argv[7])
tracker_jsonl_path = Path(sys.argv[8])

summary_path = Path(f"artifacts/collect/date={capture_date}/session={session_id}/summary.json")
admission_summary_path = Path(f"artifacts/collect/date={capture_date}/session={session_id}/admission_summary.json")

summary = json.loads(summary_path.read_text(encoding="utf-8"))
admission = json.loads(admission_summary_path.read_text(encoding="utf-8"))
policy_stack_summary = json.loads(policy_stack_summary_path.read_text(encoding="utf-8"))
calibrated_summary = json.loads(calibrated_summary_path.read_text(encoding="utf-8"))
calibration_summary = json.loads(calibration_summary_path.read_text(encoding="utf-8"))
cross_horizon_summary = json.loads(cross_horizon_summary_path.read_text(encoding="utf-8"))

session_calibrated = next(
    session for session in calibrated_summary["sessions"] if session["session_id"] == session_id
)
cross_horizon_session = next(
    session for session in cross_horizon_summary["sessions"] if session["session_id"] == session_id
)

bucket_ci_widths = {
    bucket["bucket_name"]: (
        float(bucket["calibration_gap_ci_high"]) - float(bucket["calibration_gap_ci_low"])
    )
    for bucket in calibration_summary["buckets"]
}
middle_bucket = next(
    (bucket for bucket in calibration_summary["buckets"] if bucket["bucket_name"] == "near_mid"),
    None,
)

entry = {
    "capture_date": capture_date,
    "session_id": session_id,
    "termination_reason": summary["session_diagnostics"]["termination_reason"],
    "lifecycle_state": summary["session_diagnostics"]["lifecycle_state"],
    "sample_count": summary["sample_count"],
    "good_window_count": admission["polymarket_continuity"]["window_verdict_counts"]["good"],
    "good_snapshot_count": calibration_summary["total_snapshot_count"],
    "session_good_window_count": cross_horizon_session["window_verdict_counts"]["good"],
    "snapshot_eligible_sample_count": admission["snapshot_eligibility"]["snapshot_eligible_sample_count"],
    "snapshot_eligible_sample_ratio": admission["snapshot_eligibility"]["snapshot_eligible_sample_ratio"],
    "bucket_support_flags": {
        bucket["bucket_name"]: bucket["support_flag"]
        for bucket in calibration_summary["buckets"]
    },
    "bucket_ci_widths": bucket_ci_widths,
    "support_flag_counts": calibration_summary["support_flag_counts"],
    "near_mid_support_flag": middle_bucket["support_flag"] if middle_bucket else None,
    "near_mid_ci_width": bucket_ci_widths.get("near_mid"),
    "policy_stack_metrics": {
        stack["stack_name"]: {
            "trade_count": stack["trade_count"],
            "hit_rate": stack["hit_rate"],
            "average_selected_net_edge": stack["average_selected_net_edge"],
            "total_pnl": stack["total_pnl"],
            "average_roi": stack["average_roi"],
            "pnl_per_window": stack["pnl_per_window"],
            "pnl_per_100_trades": stack["pnl_per_100_trades"],
            "pnl_per_1000_snapshots": stack["pnl_per_1000_snapshots"],
        }
        for stack in policy_stack_summary["stacks"]
    },
    "calibrated_baseline_metrics": {
        "raw_summary": session_calibrated["raw_summary"],
        "calibrated_summary": session_calibrated["calibrated_summary"],
        "delta_total_pnl": session_calibrated["delta_total_pnl"],
        "delta_average_roi": session_calibrated["delta_average_roi"],
        "delta_average_selected_net_edge": session_calibrated["delta_average_selected_net_edge"],
        "delta_trade_count": session_calibrated["delta_trade_count"],
        "calibration_applied_row_count": session_calibrated["calibration_applied_row_count"],
        "calibration_support_flag_counts": session_calibrated["calibration_support_flag_counts"],
    },
    "notable_anomalies": [
        "session remains quote-noisy overall despite clean structural continuity",
        "use good-only baseline metrics as the primary calibration signal",
    ],
    "artifact_paths": {
        "summary_path": str(summary_path),
        "admission_summary_path": str(admission_summary_path),
        "policy_stack_summary_path": str(policy_stack_summary_path),
        "calibrated_summary_path": str(calibrated_summary_path),
        "policy_v1_calibration_summary_path": str(calibration_summary_path),
        "policy_v1_cross_horizon_summary_path": str(cross_horizon_summary_path),
    },
}

tracker_json_path.parent.mkdir(parents=True, exist_ok=True)
tracker_json_path.write_text(json.dumps(entry, indent=2, sort_keys=True) + "\n", encoding="utf-8")
with tracker_jsonl_path.open("a", encoding="utf-8", newline="\n") as handle:
    handle.write(json.dumps(entry, sort_keys=True) + "\n")
PY

echo "day analysis chain complete for ${SESSION_ID}"
echo "policy_stack_summary=${POLICY_STACK_SUMMARY}"
echo "calibrated_summary=${CALIBRATED_SUMMARY}"
echo "policy_v1_run_dir=${POLICY_V1_RUN_DIR}"
echo "tracker_json=${TRACKER_JSON}"
