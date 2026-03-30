# Day 4 Shadow Summary Sanity

Session:
- `20260327T093850581Z`

Historical Day 4 summary artifact:
- [`shadow_summary.json`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_summary.json)

Line-count truth:
- [`shadow_decisions.jsonl`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_decisions.jsonl): `50920` lines
- [`shadow_order_states.jsonl`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_order_states.jsonl): `101840` lines

Historical summary counts:
- `decision_count = 50900`
- `written_decision_count = 50900`
- `order_state_transition_count = 101800`

So the Day 4 historical summary undercounts:
- decision rows by `20`
- order-state rows by `40`

Status:
- this is a historical Day 4 artifact issue only
- it predates the shutdown reconciliation fix in commit `ca7571b`
- newer shadow sessions should reconcile counts from JSONL at shutdown

Safe downstream rule:
- for Day 4 specifically, treat the JSONL files as the authoritative count source
- use [`shadow_summary.json`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_summary.json) for metadata and rates, but not as the final row-count authority

Conclusion:
- no new patch is needed for Day 4 itself
- the reconciliation fix is already in the repo
- Day 4 remains usable, but downstream analyses should continue to use JSONL truth for its exact counts
