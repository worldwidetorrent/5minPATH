#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}src"

OUTPUT_ROOT="${OUTPUT_ROOT:-artifacts}"
CALIBRATED_MANIFEST="${CALIBRATED_MANIFEST:-configs/baselines/analysis/policy_v1_calibrated_baseline.json}"
TRACKER_ROOT="${OUTPUT_ROOT}/day_block_tracker"

ROLLUP_READY="$(python3 - <<'PY'
import json
from pathlib import Path
manifest = json.loads(Path('configs/baselines/analysis/policy_v1_cross_horizon.json').read_text())
missing = []
for session in manifest['sessions']:
    root = Path('artifacts/session_rollups') / f"date={session['capture_date']}" / f"session={session['session_id']}"
    required = [
        root / 'session_policy_rollup.json',
        root / 'session_calibration_rollup.json',
    ]
    if not all(path.exists() for path in required):
        missing.append(session['session_id'])
print('yes' if not missing else 'no')
PY
)"

if [[ "$ROLLUP_READY" == "yes" ]]; then
  python3 -m rtds.cli.compare_calibrated_baseline_from_rollups --manifest "$CALIBRATED_MANIFEST"
  POLICY_SESSION_ROLLUP="${OUTPUT_ROOT}/session_rollups/date=${1:-}/session=${2:-}/session_calibration_rollup.json"
  UPDATE_ARGS=(
    --cross-horizon-manifest configs/baselines/analysis/policy_v1_cross_horizon.json
    --calibration-config configs/replay/calibration_good_only_v1.json
  )
  if [[ -f "${OUTPUT_ROOT}/policy_v1/state/good_only_calibration_state_v1.json" ]]; then
    if [[ $# -eq 2 ]]; then
      UPDATE_ARGS+=(--session-rollup-path "$POLICY_SESSION_ROLLUP")
    else
      UPDATE_ARGS+=(--initialize-from-manifest)
    fi
  else
    UPDATE_ARGS+=(--initialize-from-manifest)
  fi
  python3 -m rtds.cli.update_policy_v1_cumulative_state "${UPDATE_ARGS[@]}"
else
  python3 -m rtds.cli.compare_calibrated_baseline --manifest "$CALIBRATED_MANIFEST"
  python3 -m rtds.cli.build_policy_v1_baseline
fi

if [[ $# -eq 2 ]]; then
  CAPTURE_DATE="$1"
  SESSION_ID="$2"

  CALIBRATED_ROOT="${OUTPUT_ROOT}/replay_calibrated_baseline/policy-v1-good-only-calibrated-baseline"
  CALIBRATED_SUMMARY="$(python3 - "$CALIBRATED_ROOT" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
runs = sorted(path for path in root.glob("run_*") if path.is_dir())
if not runs:
    raise SystemExit(1)
print(runs[-1] / "comparison_summary.json")
PY
)"
  POLICY_V1_ROOT="${OUTPUT_ROOT}/policy_v1"
  POLICY_V1_RUN_DIR="$(python3 - "$POLICY_V1_ROOT" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
runs = sorted(path for path in root.glob("run_*") if path.is_dir())
if not runs:
    raise SystemExit(1)
print(runs[-1])
PY
)"
  POLICY_STACK_ROOT="${OUTPUT_ROOT}/replay_policy_stack/${CAPTURE_DATE}/session_${SESSION_ID}"
  POLICY_STACK_SUMMARY="$(python3 - "$POLICY_STACK_ROOT" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
runs = sorted(path for path in root.glob("run_*") if path.is_dir())
if not runs:
    raise SystemExit(1)
print(runs[-1] / "policy_stack_summary.json")
PY
)"
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
    --policy-v1-calibration-summary-path "${POLICY_V1_RUN_DIR}/good_only_calibration_summary.json"
    --policy-v1-cross-horizon-summary-path "${POLICY_V1_RUN_DIR}/cross_horizon_summary.json"
    --tracker-json-path "${TRACKER_ROOT}/session_${SESSION_ID}.json"
    --tracker-jsonl-path "${TRACKER_ROOT}/entries.jsonl"
  )
  if [[ -f "$SHADOW_SUMMARY_PATH" ]]; then
    TRACKER_ARGS+=(--shadow-summary-path "$SHADOW_SUMMARY_PATH")
  fi
  python3 -m rtds.cli.write_day_tracker_entry "${TRACKER_ARGS[@]}"
fi
