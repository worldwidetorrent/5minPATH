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
SUMMARY_PATH="${OUTPUT_ROOT}/collect/date=${CAPTURE_DATE}/session=${SESSION_ID}/summary.json"
ADMISSION_PATH="${OUTPUT_ROOT}/collect/date=${CAPTURE_DATE}/session=${SESSION_ID}/admission_summary.json"
SHADOW_SUMMARY_PATH="${OUTPUT_ROOT}/shadow/${SESSION_ID}/shadow_summary.json"
TRACKER_ARGS=(
  --capture-date "$CAPTURE_DATE"
  --session-id "$SESSION_ID"
  --summary-path "$SUMMARY_PATH"
  --admission-summary-path "$ADMISSION_PATH"
  --policy-stack-summary-path "$POLICY_STACK_SUMMARY"
  --calibrated-summary-path "$CALIBRATED_SUMMARY"
  --policy-v1-calibration-summary-path "$POLICY_V1_CALIBRATION"
  --policy-v1-cross-horizon-summary-path "$POLICY_V1_HORIZON"
  --tracker-json-path "$TRACKER_JSON"
  --tracker-jsonl-path "$TRACKER_JSONL"
)
if [[ -f "$SHADOW_SUMMARY_PATH" ]]; then
  TRACKER_ARGS+=(--shadow-summary-path "$SHADOW_SUMMARY_PATH")
fi
python3 -m rtds.cli.write_day_tracker_entry "${TRACKER_ARGS[@]}"

echo "day analysis chain complete for ${SESSION_ID}"
echo "policy_stack_summary=${POLICY_STACK_SUMMARY}"
echo "calibrated_summary=${CALIBRATED_SUMMARY}"
echo "policy_v1_run_dir=${POLICY_V1_RUN_DIR}"
echo "tracker_json=${TRACKER_JSON}"
