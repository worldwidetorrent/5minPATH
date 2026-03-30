# Day 4 Execution Gap Pass 2

Artifacts:
- report: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_execution_gap_pass2/20260330T021500Z/report.md)
- machine summary: [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_execution_gap_pass2/20260330T021500Z/summary.json)
- joined rows: [`joined_rows.jsonl`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_execution_gap_pass2/20260330T021500Z/joined_rows.jsonl)

Join:
- calibrated replay rows from [`rows.jsonl`](/home/ubuntu/testingproject/artifacts/replay_calibrated_baseline/policy-v1-good-only-calibrated-baseline/run_20260329T012114Z/sessions/session_20260327T093850581Z/rows.jsonl)
- Day 4 shadow decisions from [`shadow_decisions.jsonl`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_decisions.jsonl)
- join key:
  - `session_id`
  - `window_id`
  - `polymarket_market_id`
  - `snapshot_ts == decision_ts`

Headline counts:
- replay rows: `12694`
- joined rows: `8170`
- calibrated trade rows: `12126`
- joined calibrated trade rows: `7823`

So the Stage B comparison is on the coherent overlap between calibrated replay snapshots and live-forward shadow decisions, not on every replay row.

Actionability:
- shadow actionable on joined calibrated trade rows: `29`
- shadow actionable rate on joined calibrated rows: `0.3707%`
- joined calibrated rows with `3` trusted venues: `31`
- shadow actionable rate given `3` trusted venues: `93.5484%`

This is the core Stage B result:
- the calibrated replay can want a trade, but almost none of those rows are live-tradable unless the composite actually reaches `3` trusted venues
- once `3` trusted venues exist, the calibrated replay and shadow align most of the time

Fill comparison:
- mean `shadow intended fill - replay expected fill`: `0`
- mean `entry slippage vs top-of-book`: `0`
- side match on actionable joined calibrated rows: `28 / 29` (`96.55%`)

So when Day 4 replay trades were actually live-actionable, the shadow taker model did not show a new downstream fill/slippage problem. The main bottleneck remained upstream composite scarcity.

By hour:
- strongest joined-actionability hour: `2026-03-27 16:00 UTC`
  - `17` actionable on `302` joined calibrated trade rows (`5.63%`)
- smaller positive hours:
  - `2026-03-28 08:00 UTC`: `6 / 552`
  - `2026-03-28 06:00 UTC`: `3 / 524`
- several hours were `0` actionable despite large calibrated replay trade counts:
  - `2026-03-27 21:00 UTC`
  - `2026-03-27 22:00 UTC`
  - `2026-03-27 23:00 UTC`
  - `2026-03-28 03:00 UTC`
  - `2026-03-28 04:00 UTC`
  - `2026-03-28 05:00 UTC`

By trusted-venue state:
- `1` trusted venue: `0 / 122` actionable
- `2` trusted venues: `0 / 7670` actionable
- `3` trusted venues: `29 / 31` actionable

By calibrated edge bucket:
- `<0.01`: `0 / 378`
- `0.01-0.05`: `2 / 1334`
- `0.05-0.10`: `2 / 1772`
- `0.10+`: `25 / 4339`

Read:
- The best replay trades were only rarely tradable live on Day 4 because the live composite rarely reached `3` trusted venues at the exact decision timestamp.
- But when that composite gate opened, replay-vs-shadow alignment was strong:
  - actionability was high
  - side agreement was high
  - expected fill vs intended fill divergence was effectively zero

Conclusion:
- Day 4 Stage B says execution realism is not yet the main blocker.
- Composite scarcity is still the dominant bottleneck.
- Once the live composite is valid, the calibrated replay appears directionally and economically consistent with live-forward shadow execution.
