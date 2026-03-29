# Day 4 Shadow Execution Baseline

Session: `20260327T093850581Z`

Primary artifact set:

- report artifact: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_shadow_execution_baseline/20260329T000128Z/report.md)
- machine summary: [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_shadow_execution_baseline/20260329T000128Z/summary.json)
- source summary: [`shadow_summary.json`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_summary.json)
- source decisions: [`shadow_decisions.jsonl`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_decisions.jsonl)

## Headline

Day 4 is the first real live-forward shadow execution baseline:

- actionable decisions: `1443`
- rows with non-null `fair_value_base`: `1631`
- rows with non-null `calibrated_fair_value_base`: `1631`
- rows reaching `3` trusted venues: `1631 / 50920` (`3.2031%`)
- actionable conditional on `3` trusted venues: `1443 / 1631` (`88.4733%`)

That means the sidecar produced a real execution candidate set once full composite state formed.

## No-Trade Reason Mix

From row-level `shadow_decisions.jsonl`:

- `insufficient_trusted_venues`: `48970` (`96.1705%`)
- `missing_composite_nowcast`: `319` (`0.6265%`)
- `edge_below_threshold`: `121` (`0.2376%`)
- `spread_too_wide`: `46` (`0.0903%`)
- `quote_stale`: `21` (`0.0412%`)

Interpretation:

- the dominant Day 4 blocker was still sparse `3`-venue composite availability
- once composite formed, the remaining rejects were small and plausibly economic or market-structure driven

## Trusted-Venue Distribution

- `0` trusted venues: `319`
- `1` trusted venue: `1296`
- `2` trusted venues: `47674`
- `3` trusted venues: `1631`

Most rows stopped at `2` trusted venues. The small but real `3`-venue subset is where essentially all meaningful execution evidence came from.

## Fair-Value Reach

- non-null `fair_value_base`: `1631 / 50920` (`3.2031%`)
- non-null `calibrated_fair_value_base`: `1631 / 50920` (`3.2031%`)

On Day 4, fair-value reach and `3`-trusted-venue reach were effectively the same set.

## Execution-Side Baseline

The first useful Day 4 execution-side baseline is:

- live-forward decision rows on disk: `50920`
- actionable decisions: `1443`
- actionable rate over all rows: `2.8339%`
- rows reaching `3` trusted venues: `3.2031%`
- actionable rate conditional on `3` trusted venues: `88.4733%`

So the bottleneck is not primarily downstream policy rejection. It is upstream live composite availability.

## Note on Summary Drift

There is a small shutdown accounting mismatch:

- [`shadow_summary.json`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_summary.json) reports `50900` decisions
- [`shadow_decisions.jsonl`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_decisions.jsonl) contains `50920` rows

Treat the JSONL row counts as the authoritative Day 4 execution baseline until the shadow summary shutdown reconciliation is tightened.
