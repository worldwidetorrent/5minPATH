# 0007 — Stage 2 Far-Up Correction Decision

## Status

Accepted on `main`.

## Context

The frozen reference stack is:
- policy v1 universe split
- admission semantics v2
- stage-1 `good_only` calibrator
- current oracle source
- current replay assumptions
- current regime definitions

Pinned evidence before this decision:
- 12h calibrated anomaly: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/12h_calibration_failure_profile/report.md)
- 24h calibrated success case: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/24h_calibration_success_profile/report.md)
- direct comparison: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/12h_vs_24h_far_up_lean_up_diagnosis/run_20260324T035140Z/report.md)
- stable diagnosis note: [`12h_vs_24h_far_up_lean_up_diagnosis.md`](/home/ubuntu/testingproject/docs/baselines/12h_vs_24h_far_up_lean_up_diagnosis.md)

The open question was whether the 12-hour `far_up` and `lean_up` failure represented a recurring context-specific problem that justified a Stage 2 gate.

## Decision

Do not implement a Stage 2 context gate yet.

Keep the stage-1 calibrator broadly intact and treat the 12-hour failure as a session-specific anomaly under the currently available coarse context variables.

## Why

The comparison does not show a recurring failure under the same coarse contexts.

The same coarse slices that hurt 12h improve materially on 24h:
- `far_up low_vol`: `-204.057` on 12h vs `+1138.228` on 24h
- `far_up tight_spread`: `-197.298` on 12h vs `+962.177` on 24h
- `far_up early_window`: `-113.278` on 12h vs `+191.582` on 24h
- `far_up mid_window`: `-92.151` on 12h vs `+728.115` on 24h
- `lean_up low_vol`: `-51.286` on 12h vs `+122.849` on 24h
- `lean_up tight_spread`: `-61.784` on 12h vs `+130.825` on 24h

Control behavior also stays coherent:
- `far_down` improves in both sessions

So the evidence does not support a simple gate based on:
- `time_remaining`
- `volatility_regime`
- `spread_bucket`

## Consequences

Current stack remains frozen:
- `good` = baseline
- `degraded_light` = exploratory
- `degraded_medium` = context-gated exploratory
- `degraded_heavy` = excluded
- `unusable` = excluded

Calibration stance:
- keep Stage 1 active for diagnosis and baseline evaluation
- do not freeze a Stage 2 gate from the current 12h/24h evidence
- continue broader validation before changing correction rules

If Stage 2 becomes necessary later, the next candidate gate should be based on a persistence / sustained-move proxy rather than these current coarse slices alone.
