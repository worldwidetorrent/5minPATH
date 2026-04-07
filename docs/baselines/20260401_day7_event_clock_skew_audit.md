# Day 7 Event-Time Clock Skew Audit

Session:

- `20260401T112554963Z`

Artifacts:

- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_event_clock_skew_audit/20260405T120000Z/report.md)
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_event_clock_skew_audit/20260405T120000Z/summary.json)
- [`skew_rows.jsonl`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_event_clock_skew_audit/20260405T120000Z/skew_rows.jsonl)

Headline:

- `future_event_clock_skew` rows: `8202`
- actionable skew rows: `0`
- non-actionable skew rows: `8202`

Source breakdown:

- `quote_event_ts: 8174`
- `chainlink_event_ts: 14`
- `exchange_event_ts: 14`

Magnitude histogram:

- `0-250ms: 390`
- `250-500ms: 6851`
- `500-1000ms: 899`
- `1000ms+: 62`

Hourly clustering:

- skew is broad, not narrowly clustered
- highest hourly counts were:
  - `2026-04-02 00:00 UTC: 403`
  - `2026-04-01 20:00 UTC: 384`
  - `2026-04-01 12:00 UTC: 382`
  - `2026-04-01 23:00 UTC: 376`

Read:

- Day 7 confirms the remaining shadow quality issue is mostly Polymarket `quote_event_ts` ahead of `decision_ts`, not a true recv-time visibility leak.
- The skew rows are overwhelmingly in the `250-500ms` bucket.
- Because skew rows were entirely non-actionable on Day 7, this is a quality/filtering problem rather than hidden actionable flow being thrown away downstream.
