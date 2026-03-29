# Day 4 Shadow Execution Gap Pass 1

Session: `20260327T093850581Z`

Primary artifacts:

- report artifact: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_shadow_execution_gap_pass1/20260329T001500Z/report.md)
- machine summary: [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_shadow_execution_gap_pass1/20260329T001500Z/summary.json)
- source decisions: [`shadow_decisions.jsonl`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_decisions.jsonl)

## Headline

This is the first real execution-gap pass using Day 4 shadow only, without waiting for calibrated replay joins.

- actionable decision count: `1443`
- actionable rate: `2.8339%`
- average quote age on actionable rows: `1027.9494 ms`
- average spread at decision on actionable rows: `0.0115211365`
- average entry slippage vs top-of-book on actionable rows: `0.000`

In the current taker-shadow model, intended entry was effectively at top-of-book whenever the row became actionable.

## Trusted-Venue Conditioning

- trusted venues `0`: `0` actionable out of `319` rows (`0.0000%`)
- trusted venues `1`: `0` actionable out of `1296` rows (`0.0000%`)
- trusted venues `2`: `0` actionable out of `47674` rows (`0.0000%`)
- trusted venues `3`: `1443` actionable out of `1631` rows (`88.4733%`)

This is the key Day 4 execution result:

- downstream tradability was not the main blocker
- upstream composite formation was the blocker
- once `3` trusted venues were available, the sidecar was actionable most of the time

## Actionable Distribution

By volatility regime:

- `high_vol: 1424`
- `mid_vol: 10`
- `low_vol: 9`

By window quality regime:

- `good: 1443`

So the Day 4 actionable set was overwhelmingly:

- `good` windows
- high-volatility states

## Hourly Shape

Best actionable hours:

- `2026-03-27 12:00 UTC`: `293` actionable (`14.36%`)
- `2026-03-27 13:00 UTC`: `256` actionable (`13.20%`)
- `2026-03-27 11:00 UTC`: `174` actionable (`8.27%`)
- `2026-03-27 15:00 UTC`: `171` actionable (`8.48%`)

Weakest stretch:

- `2026-03-27 21:00 UTC`: `0`
- `2026-03-28 03:00 UTC`: `0`
- `2026-03-28 04:00 UTC`: `0`
- `2026-03-28 05:00 UTC`: `0`

This matches the earlier composite-availability diagnosis: Day 4 live tradability was strongly daypart-dependent, not uniformly available across the full session.

## Sample Actionable Rows

Representative actionable rows in the artifact show:

- intended side populated
- intended entry equal to the current ask on the chosen side
- spread typically `0.01` to `0.03`
- quote age usually sub-`2s`
- trusted venue count `3`

That means the first execution-gap signal is not “shadow wants impossible fills.” It is “shadow rarely gets the composite state needed to act, but when it does, the immediate book terms look realistic under the current simplified taker model.”

## Bottom Line

The first Day 4 shadow-only execution-gap pass says:

- live-forward tradability is real, not hypothetical
- it is sparse overall because `3`-venue composite availability is sparse
- when the composite exists, the strategy is usually actionable
- the current shadow taker model shows essentially zero extra slippage versus top-of-book, so the real bottleneck remains state availability, not book-touch mechanics

The next pass should add the calibrated replay join after Day 4 calibration finishes, so replay economics can be compared directly against this live-forward tradability surface.
