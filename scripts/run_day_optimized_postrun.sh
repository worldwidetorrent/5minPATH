#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <capture-date> <session-id> [--checkpoint]" >&2
  exit 2
fi

CAPTURE_DATE="$1"
SESSION_ID="$2"
RUN_CHECKPOINT="${3:-}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

./scripts/run_day_fast_lane.sh "$CAPTURE_DATE" "$SESSION_ID"

if [[ "$RUN_CHECKPOINT" == "--checkpoint" ]]; then
  ./scripts/run_checkpoint_refresh.sh "$CAPTURE_DATE" "$SESSION_ID"
else
  echo "fast lane complete for ${SESSION_ID}; cumulative checkpoint refresh deferred"
fi
