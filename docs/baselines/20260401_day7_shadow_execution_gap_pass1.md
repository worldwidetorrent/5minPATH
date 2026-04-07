# Day 7 Shadow Execution Gap Pass 1

Session:

- `20260401T112554963Z`

Artifacts:

- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_shadow_execution_gap_pass1/20260405T120000Z/report.md)
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_shadow_execution_gap_pass1/20260405T120000Z/summary.json)
- [`actionable_rows.jsonl`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_shadow_execution_gap_pass1/20260405T120000Z/actionable_rows.jsonl)

Headline:

- decisions: `48914`
- actionable decisions: `31356`
- actionable rate: `64.1043%`
- fair-value rows: `40435`
- calibrated fair-value rows: `40435`

Tradability shape:

- average quote age on actionable rows: `866.8577 ms`
- average spread on actionable rows: `0.01068605689501211889271590764`
- average entry slippage vs top-of-book: `0`
- size coverage pass rate on actionable rows: `100%`

Trusted venue conditioning:

- `3` trusted venues: `40435`
- `2` trusted venues: `7853`
- `1` trusted venue: `255`
- `0` trusted venues: `371`
- actionable rows occurred only with `3` trusted venues: `31356`

Regime breakdown of actionable rows:

- volatility:
  - `high_vol: 8018`
  - `mid_vol: 14643`
  - `low_vol: 8695`
- window quality:
  - `good: 31356`

Read:

- Day 7 is not a marginal shadow day. It produced large real tradable volume.
- Downstream book-touch mechanics still do not look like the limiting problem.
- Actionability remains effectively conditional on full `3`-venue composite formation.
