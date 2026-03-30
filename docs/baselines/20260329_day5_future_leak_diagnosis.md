# Day 5 Future-Leak Diagnosis

Day 5 shadow for session `20260329T002704901Z` stays quarantined as baseline evidence.

Primary artifacts:
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day5_future_leak_diagnosis/20260330T094500Z/report.md)
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day5_future_leak_diagnosis/20260330T094500Z/summary.json)
- [`future_leak_rows.jsonl`](/home/ubuntu/testingproject/artifacts/diagnostics/day5_future_leak_diagnosis/20260330T094500Z/future_leak_rows.jsonl)

## Result

- Day 4 future-leak rows: `0`
- Day 5 future-leak rows: `46`
- offending source: `quote_recv_ts` on all `46`
- hour buckets:
  - `2026-03-29T18:00`: `28`
  - `2026-03-29T19:00`: `18`
- affected windows:
  - `btc-5m-20260329T180000Z`: `28`
  - `btc-5m-20260329T191500Z`: `18`

This is not a startup, shutdown, or day-rollover failure. It is a narrow mid-session visibility bug around two window-open clusters.

## Root Cause

The live-state adapter was allowing Polymarket rows into the as-of state surface when:

- `event_ts <= decision_ts`

even if:

- `recv_ts > decision_ts`

That created a real future-state violation for the quote surface, and the sidecar correctly emitted `future_state_leak_detected`.

## Patch

The narrow fix is in [`capture_output_live_state_adapter.py`](/home/ubuntu/testingproject/src/rtds/execution/capture_output_live_state_adapter.py):

- Polymarket rows now become visible only when `recv_ts <= decision_ts`
- if `recv_ts` is absent, the adapter falls back to the previous timestamp rule

Regression coverage is in:
- [`test_capture_output_live_state_adapter.py`](/home/ubuntu/testingproject/tests/execution/test_capture_output_live_state_adapter.py)

New test:
- `test_capture_output_adapter_waits_for_polymarket_recv_ts_before_emitting`

## Decision Rule

This issue was reproducible quickly with a controlled smoke fixture, so I patched the narrow edge case instead of leaving it as an unexplained quarantine.

Operationally:
- historical Day 5 shadow remains quarantined as evidence
- the adapter fix is ready for the next live shadow session
- no capture, policy, admission, or source-set changes were made
