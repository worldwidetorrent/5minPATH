# Day 7 Execution Gap Pass 2

Session:

- `20260401T112554963Z`

Artifacts:

- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_execution_gap_pass2/20260405T120000Z/report.md)
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_execution_gap_pass2/20260405T120000Z/summary.json)
- [`joined_rows.jsonl`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_execution_gap_pass2/20260405T120000Z/joined_rows.jsonl)

Headline:

- calibrated replay rows: `7643`
- calibrated replay trade rows: `7259`
- joined rows on shadow key: `4852`
- joined calibrated replay trade rows: `4609`
- shadow actionable on joined calibrated rows: `2961`
- shadow actionable rate on joined calibrated rows: `64.2439%`

Conditional on trusted venues:

- joined calibrated rows with `3` trusted venues: `3825`
- shadow actionable given `3` trusted venues: `2961 / 3825 = 77.4118%`
- joined calibrated rows with `2` trusted venues: `766`
- shadow actionable given `2` trusted venues: `0%`

Fill / side comparison:

- side match count on actionable joined rows: `2848`
- side match rate on actionable joined rows: `96.1837%`
- mean shadow minus replay expected fill: `0`
- mean shadow entry slippage vs top-of-book: `0`

Hourly read:

- best joined-hour actionability was around:
  - `2026-04-01 23:00 UTC`: `78.70%`
  - `2026-04-01 21:00 UTC`: `77.44%`
  - `2026-04-01 19:00 UTC`: `76.77%`
- weaker joined-hour blocks included:
  - `2026-04-01 13:00 UTC`: `24.18%`
  - `2026-04-01 14:00 UTC`: `36.62%`
  - `2026-04-02 11:00 UTC`: `48.59%`

Regime read:

- joined calibrated replay rows by volatility regime:
  - `high_vol: 1252`
  - `mid_vol: 2165`
  - `low_vol: 1192`
- shadow actionable rate on joined calibrated rows:
  - `high_vol: 55.91%`
  - `mid_vol: 69.33%`
  - `low_vol: 63.76%`

Read:

- Day 7 Stage B is materially stronger than Day 4.
- Live actionability is no longer rare once the joined calibrated replay trade is actually present in shadow.
- Execution realism still does not look like the primary limiter; composite availability remains the gating condition.
