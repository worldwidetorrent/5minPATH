# 2026-04-01 Pre-Day 7 Validation Smoke

Artifacts:

- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/pre_day7_validation_smoke/20260401T103142560Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/pre_day7_validation_smoke/20260401T103142560Z/report.md)
- capture summary: [`summary.json`](/home/ubuntu/testingproject/artifacts/collect/date=2026-04-01/session=20260401T103142560Z/summary.json)
- shadow summary: [`shadow_summary.json`](/home/ubuntu/testingproject/artifacts/shadow/20260401T103142560Z/shadow_summary.json)

Headline:

- capture completed cleanly
- shadow stayed in `live_only_from_attach_ts`
- `backlog_decision_count = 0`
- `future_recv_visibility_leak = 0`
- `future_event_clock_skew = 27`
- non-null fair values: `213`
- non-null calibrated fair values: `213`
- actionable decisions: `186`
- no generic `invalid_state`

Interpretation:

- the Day 5 recv-time visibility leak fix held in a fresh bounded run
- the Day 6 event-time-ahead condition is now separated cleanly into `future_event_clock_skew`
- the bounded live smoke therefore cleared the shadow-side semantic split needed before Day 7
- the Kraken missing-`result` abort path was covered by the collector regression test; the live smoke itself did not reproduce that malformed payload
