## Clean Shadow Baseline Edge Comparison

Primary artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_baseline_edge_comparison/20260407T121500Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_baseline_edge_comparison/20260407T121500Z/report.md)

Comparison scope:
- Day 4 clean shadow baseline
- Day 7 clean shadow baseline

Headline:
- Day 4 edge survival ratio: `0.007507565613801537116011238129`
- Day 7 edge survival ratio: `0.3371580735292590164938162539`
- Day 7 calibrated replay was strong while raw replay was weak:
  - raw baseline PnL: `-193.219`
  - calibrated baseline PnL: `1829.231`

Repeatability read:
- Day 7 preserved much more modeled edge than Day 4
- on both clean shadow baseline days, the main drag was composite availability rather than fill loss
- Day 7 still carries an explicit `future_event_clock_skew` caveat, but those rows were non-actionable in the historical session

Important asymmetry:
- Day 4 predates the recv-vs-event skew split, so its historical shadow artifact does not carry a directly comparable explicit `future_event_clock_skew` series
