# 0006 — Stage-1 Good-Only Calibration

Status: Accepted  
Date: 2026-03-21

## Decision

The first calibration pass for policy v1 is now frozen as:

- `good_only` windows only
- coarse fair-value buckets, not deciles
- bootstrap confidence intervals
- explicit support flags:
  - `sufficient`
  - `thin`
  - `merge_required`
- merge-or-leave-uncorrected behavior for unsupported buckets

The active config is [`configs/replay/calibration_good_only_v1.json`](/home/ubuntu/testingproject/configs/replay/calibration_good_only_v1.json).

## Why

Cross-horizon policy validation over the pinned 6-hour, 12-hour, 20-hour, and 24-hour sessions
confirmed that:

- `good` is the strongest clean policy universe
- `degraded_light` is economically real but weaker and less stable
- `degraded_medium` remains exploratory and context-gated
- `degraded_heavy` and `unusable` stay excluded

So the first calibration layer should not pool degraded regimes into the baseline
until there is stronger evidence that doing so improves reliability rather than
mixing structurally different window types.

## Consequences

- Baseline calibration starts from `good_only`.
- The first calibration artifact is diagnostic as much as corrective.
- Buckets with enough support can carry a provisional bucket-mean correction.
- Buckets with weak support should be merged or left uncorrected rather than forced
  into a false-precision fit.
- The first sanctioned replay application of the calibrator is the frozen raw-vs-calibrated
  `baseline_only` comparison in [`configs/baselines/analysis/policy_v1_calibrated_baseline.json`](/home/ubuntu/testingproject/configs/baselines/analysis/policy_v1_calibrated_baseline.json),
  which applies bucket corrections only where support is `sufficient`.
- Future calibration refreshes should add clean `good` windows without redefining the
  regime map by default.
