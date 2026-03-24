# 12h vs 24h Far-Up / Lean-Up Diagnosis

Primary artifact:
- comparison summary: [`comparison_summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/12h_vs_24h_far_up_lean_up_diagnosis/run_20260324T035140Z/comparison_summary.json)
- comparison report: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/12h_vs_24h_far_up_lean_up_diagnosis/run_20260324T035140Z/report.md)
- 12h profile source: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/12h_calibration_failure_profile/report.md)
- 24h profile source: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/24h_calibration_success_profile/report.md)

## Question

Does the 12-hour `far_up` / `lean_up` calibration failure recur under the same coarse contexts on the 24-hour session, or is it a session-specific anomaly?

## Answer

The current evidence favors a session-specific anomaly, not a recurring failure that can be repaired cleanly with the existing coarse context variables.

The same coarse contexts that hurt the 12-hour session improve materially on the 24-hour session:
- `far_up low_vol`: `-204.057` on 12h vs `+1138.228` on 24h
- `far_up tight_spread`: `-197.298` on 12h vs `+962.177` on 24h
- `far_up early_window`: `-113.278` on 12h vs `+191.582` on 24h
- `far_up mid_window`: `-92.151` on 12h vs `+728.115` on 24h
- `lean_up low_vol`: `-51.286` on 12h vs `+122.849` on 24h
- `lean_up tight_spread`: `-61.784` on 12h vs `+130.825` on 24h

That means a simple Stage 2 gate based only on `time_remaining`, `volatility_regime`, or `spread_bucket` is not justified by the current comparison.

## Bucket Read

`far_up` is still the main unstable bucket, but the instability is not structurally recurring under the same coarse contexts.

12h:
- total delta PnL: `-261.076`
- failure concentrated in `low_vol`, `tight_spread`, and `early/mid` slices
- calibration cut profitable `up` exposure and redirected part of it into weaker `down` flow

24h:
- total delta PnL: `+1150.928`
- strongest repair also comes from `low_vol`, `tight_spread`, and `mid` slices
- the same `up` extreme that failed on 12h is the main positive repair on 24h

`lean_up` behaves like a weaker version of the same phenomenon:
- 12h total delta PnL: `-87.826`
- 24h total delta PnL: `+119.147`

`far_down` remains the control bucket:
- 12h total delta PnL: `+190.387`
- 24h total delta PnL: `+157.970`

This makes `far_down` useful as a stability reference: the problem is not that all sufficient-bucket corrections are broadly broken.

## Interpretation

The current coarse variables explain where the 12-hour anomaly happened, but they do not isolate a recurring bad context. The same coarse slices are positive on the 24-hour session.

So the most likely reads are:
- session-specific averaging cost in the 12-hour run, or
- a missing persistence-style context variable that is not captured by `time_remaining`, `volatility_regime`, or `spread_bucket`

The current evidence is not strong enough to freeze a Stage 2 gate on the existing coarse slices.

## Practical Decision

For now:
- keep Stage 1 calibration intact
- treat the calibrator as economically meaningful but not yet fully settled
- do not introduce a Stage 2 context gate based only on `time_remaining`, `volatility_regime`, or `spread_bucket`

If Stage 2 is pursued later, the next candidate variable should be a persistence / sustained-move proxy rather than another coarse slice split.
