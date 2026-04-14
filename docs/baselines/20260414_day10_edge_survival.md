# Day 10 Edge Survival

Artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day10_edge_survival/20260413T133325Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day10_edge_survival/20260413T133325Z/report.md)

Session:
- Day 10 `20260412T123517467Z`

Scope:
- clean shadow runtime session
- fast-lane calibrated replay rows joined to live-forward shadow decisions
- one-contract normalized PnL comparison, matching the existing Day 4/Day 7/Day 8/Day 9 edge-survival method

Headline:
- capture and shadow both closed cleanly
- calibrated replay was strong
- live edge survival was positive and better than Day 4, Day 8, and Day 9, but still far below Day 7
- the main drags were availability first and side mismatch second; fill loss was not the bottleneck

Key numbers:
- calibrated trade rows: `10319`
- joined calibrated trade rows: `6742`
- shadow actionable joined trade rows: `4020`
- shadow side-match count: `2141`
- replay expected PnL per contract: `1683.421`
- shadow realized PnL per contract: `119.39`
- edge survival ratio: `0.07092105896267184501084398971`
- joined trade rate: `0.6533578835158446`
- shadow actionable rate on calibrated trade rows: `0.38957263300707434`
- side-match rate on shadow actionable rows: `0.5325870646766169`

Gap decomposition:
- availability loss per contract: `1143.061`
- fill loss per contract: `-2.141`
- side mismatch loss per contract: `423.111`
- residual outcome loss per contract: `0.000`

Availability reason counts:
- `missing_shadow_row`: `3577`
- `insufficient_trusted_venues`: `1246`
- `future_event_clock_skew`: `1077`
- `edge_below_threshold`: `317`
- `spread_too_wide`: `65`
- `quote_stale`: `17`

Shadow runtime facts:
- shadow clean baseline: `true`
- decision count: `52804`
- live-forward decisions: `52804`
- backlog decisions: `0`
- actionable decisions: `29969`
- 3-trusted-venue rows: `40353`
- 3-trusted-venue rate: `0.7642034694341337777441102947`
- actionable given 3 trusted: `0.7426709290511238321810026516`

Interpretation:
- Day 10 is a clean shadow comparison day.
- It is not another Day 7-style high-survival day.
- It is materially better than Day 4, Day 8, and Day 9 on edge survival, but still mainly loses modeled edge before or at actionability.
- Its side-match rate of `0.5325870646766169` keeps the Day 8-style side-agreement problem alive as a core weak-day diagnostic.
