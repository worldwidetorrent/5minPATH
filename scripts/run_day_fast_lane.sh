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
CALIBRATED_MANIFEST="${CALIBRATED_MANIFEST:-configs/baselines/analysis/policy_v1_calibrated_baseline.json}"

python3 -m rtds.cli.compare_policy_stacks \
  --date "$CAPTURE_DATE" \
  --session-id "$SESSION_ID" \
  --rebuild-snapshots true \
  --rebuild-reference true

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

python3 -m rtds.cli.compare_calibrated_session \
  --capture-date "$CAPTURE_DATE" \
  --session-id "$SESSION_ID" \
  --session-label "$SESSION_ID" \
  --manifest "$CALIBRATED_MANIFEST"

CALIBRATED_SESSION_ROOT="${OUTPUT_ROOT}/replay_calibrated_session/policy-v1-good-only-calibrated-baseline/date=${CAPTURE_DATE}/session_${SESSION_ID}"
CALIBRATED_SESSION_SUMMARY="$(python3 - "$CALIBRATED_SESSION_ROOT" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
runs = sorted(path for path in root.glob("run_*") if path.is_dir())
if not runs:
    raise SystemExit(1)
print(runs[-1] / "summary.json")
PY
)"

SUMMARY_PATH="${OUTPUT_ROOT}/collect/date=${CAPTURE_DATE}/session=${SESSION_ID}/summary.json"
ADMISSION_PATH="${OUTPUT_ROOT}/collect/date=${CAPTURE_DATE}/session=${SESSION_ID}/admission_summary.json"
SHADOW_SUMMARY_PATH="${OUTPUT_ROOT}/shadow/${SESSION_ID}/shadow_summary.json"
SHADOW_DECISIONS_PATH="${OUTPUT_ROOT}/shadow/${SESSION_ID}/shadow_decisions.jsonl"

BUILD_ARGS=(
  --capture-date "$CAPTURE_DATE"
  --session-id "$SESSION_ID"
  --summary-path "$SUMMARY_PATH"
  --admission-summary-path "$ADMISSION_PATH"
  --policy-stack-summary-path "$POLICY_STACK_SUMMARY"
  --calibrated-session-summary-path "$CALIBRATED_SESSION_SUMMARY"
)
if [[ -f "$SHADOW_SUMMARY_PATH" ]]; then
  BUILD_ARGS+=(--shadow-summary-path "$SHADOW_SUMMARY_PATH")
fi
if [[ -f "$SHADOW_DECISIONS_PATH" ]]; then
  BUILD_ARGS+=(--shadow-decisions-path "$SHADOW_DECISIONS_PATH")
fi

python3 -m rtds.cli.build_day_fast_report "${BUILD_ARGS[@]}"

ROLLUP_ARGS=(
  --capture-date "$CAPTURE_DATE"
  --session-id "$SESSION_ID"
  --session-label "$SESSION_ID"
  --policy-stack-summary-path "$POLICY_STACK_SUMMARY"
  --calibrated-session-summary-path "$CALIBRATED_SESSION_SUMMARY"
)
if [[ -f "$SHADOW_SUMMARY_PATH" ]]; then
  ROLLUP_ARGS+=(--shadow-summary-path "$SHADOW_SUMMARY_PATH")
fi
if [[ -f "$SHADOW_DECISIONS_PATH" ]]; then
  ROLLUP_ARGS+=(--shadow-decisions-path "$SHADOW_DECISIONS_PATH")
fi

python3 -m rtds.cli.write_session_rollups "${ROLLUP_ARGS[@]}"
