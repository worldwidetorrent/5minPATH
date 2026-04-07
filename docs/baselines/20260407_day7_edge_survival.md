## Day 7 Edge Survival

Primary artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_edge_survival/20260407T120000Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_edge_survival/20260407T120000Z/report.md)

Method note:
- replay rows are one-contract simulated trades
- shadow realized pnl is normalized to one contract so the two sides are comparable

Headline:
- calibrated trade rows: `7259`
- joined calibrated trade rows: `4609`
- shadow actionable joined trade rows: `2961`
- replay expected pnl per contract: `1829.231`
- shadow realized pnl per contract: `616.74`
- edge survival ratio: `0.3371580735292590164938162539`

Gap decomposition:
- availability loss per contract: `1116.512`
- fill loss per contract: `-2.848`
- side mismatch loss per contract: `98.827`
- residual outcome loss per contract: `0.000`

Read:
- Day 7 preserved a meaningful fraction of calibrated modeled edge under clean shadow conditions
- the dominant drag was availability, not fill loss
- side mismatch was secondary
- event-time skew remains a quality caveat, but it is not the same class as the old visibility leak
