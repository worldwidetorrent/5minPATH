# Day 9 Edge Survival

Artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day9_edge_survival/20260413T094543Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day9_edge_survival/20260413T094543Z/report.md)

Session:
- Day 9 `20260411T031416598Z`

Scope:
- clean shadow runtime session
- fast-lane calibrated replay rows joined to live-forward shadow decisions
- one-contract normalized PnL comparison, matching the existing Day 4/Day 7/Day 8 edge-survival method

Headline:
- calibrated replay was strong
- live edge survival was positive but modest
- the main drag was availability, not fill loss

Key numbers:
- calibrated trade rows: `11432`
- joined calibrated trade rows: `7530`
- shadow actionable joined trade rows: `758`
- shadow side-match count: `496`
- replay expected PnL per contract: `2094.541`
- shadow realized PnL per contract: `112.42`
- edge survival ratio: `0.05367285720355915687494300661`
- joined trade rate: `0.6586773967809657`
- shadow actionable rate on calibrated trade rows: `0.06630510846745977`
- side-match rate on shadow actionable rows: `0.6543535620052771`

Gap decomposition:
- availability loss per contract: `1951.589`
- fill loss per contract: `-0.496`
- side mismatch loss per contract: `31.028`
- residual outcome loss per contract: `0.000`

Availability reason counts:
- `insufficient_trusted_venues`: `5535`
- `missing_shadow_row`: `3902`
- `future_event_clock_skew`: `1165`
- `edge_below_threshold`: `61`
- `spread_too_wide`: `7`
- `quote_stale`: `4`

Interpretation:
- Day 9 is not another Day 7-style high-survival day.
- It is still materially better than Day 4 and Day 8 on survival ratio.
- The calibrated replay signal remains economically meaningful, but the live-forward execution surface filtered most of that modeled edge away before actionability.
- This supports keeping the research contract frozen and rerunning the minimum-edge filter experiment only after the expanded clean-shadow set is closed.
