# Day 7 vs Day 8 Side-Mismatch Audit

Artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_day8_side_mismatch_audit/20260411T000000Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_day8_side_mismatch_audit/20260411T000000Z/report.md)
- `day_7_audit_rows.jsonl`
- `day_8_audit_rows.jsonl`

Scope:
- diagnostic only
- no policy, calibrator, venue, capture, shadow-contract, or fast-lane workflow changes

Compared sessions:
- Day 7 `20260401T112554963Z`
- Day 8 `20260407T110750965Z`

## Main Finding

Day 8 was weak because directional agreement failed after actionability, not because runtime,
join rate, raw actionability, event-time skew, or fragile 3-venue state failed.

Day-level rates:
- Day 7 joined trade rate: `0.6349359415897506`
- Day 8 joined trade rate: `0.6333640068358091`
- Day 7 shadow actionable rate on calibrated rows: `0.40790742526518803`
- Day 8 shadow actionable rate on calibrated rows: `0.4395951097673196`
- Day 7 side-match rate on actionable rows: `0.961837217156366`
- Day 8 side-match rate on actionable rows: `0.5520334928229665`

That means Day 8 had Day 7-like join/actionability, but not Day 7-like directional agreement.

## Bucket Concentration

Day 7 mismatches were small in count:
- match bucket counts: `{'lean_up': 332, 'near_mid': 211, 'lean_down': 388, 'far_down': 1166, 'far_up': 751}`
- mismatch bucket counts: `{'far_down': 103, 'far_up': 8, 'lean_up': 2}`

Day 8 mismatches were broad and tail-heavy:
- match bucket counts: `{'far_down': 625, 'near_mid': 109, 'lean_up': 304, 'lean_down': 230, 'far_up': 578}`
- mismatch bucket counts: `{'lean_down': 83, 'far_down': 430, 'lean_up': 102, 'near_mid': 23, 'far_up': 860}`

So Day 8 mismatch was not merely a neutral-boundary artifact. It happened in deep directional buckets too.

## Distance / Boundary Read

Replay neutral-distance quantiles were not materially weaker for Day 8 mismatches:
- Day 8 match replay-neutral-distance p50: `0.0550863434130156`
- Day 8 mismatch replay-neutral-distance p50: `0.0729589103986533`

Selected-edge buckets also show Day 8 mismatches were not mostly tiny-edge rows:
- Day 8 mismatch edge buckets: `{'1c_to_2p5c': 133, '2p5c_to_5c': 158, 'gte_5c': 1111, 'lt_1c': 96}`

So a simple neutral dead zone alone is unlikely to fix Day 8.

## Live-vs-Replay Fair-Value Delta

Replay rows do not persist replay composite USD price, so the audit uses live-vs-replay calibrated-fair-value delta as the available proxy.

Day 7:
- match p50 abs fair-value delta: `0E-16`
- mismatch p50 abs fair-value delta: `0.1140261248591909`

Day 8:
- match p50 abs fair-value delta: `0.0793533673035268`
- mismatch p50 abs fair-value delta: `0.2798930824398017`

This is the strongest explanatory cut: Day 8 mismatches had much larger live-vs-replay fair-value disagreement.

## Hour Structure

Day 8 mismatch clustered heavily in a few UTC hours:
- `2026-04-08 04:00`: `285` mismatches, side-match rate `0.42655935613682094`
- `2026-04-08 05:00`: `272` mismatches, side-match rate `0.5277777777777778`
- `2026-04-08 03:00`: `155` mismatches, side-match rate `0.38492063492063494`
- `2026-04-08 02:00`: `149` mismatches, side-match rate `0.3436123348017621`
- `2026-04-08 06:00`: `130` mismatches, side-match rate `0.6569920844327177`

Day 7 had far fewer mismatches, with the largest hours:
- `2026-04-02 06:00`: `30` mismatches, side-match rate `0.6938775510204082`
- `2026-04-01 17:00`: `26` mismatches, side-match rate `0.7868852459016393`

So Day 8’s directional failure had a stronger hour-regime component.

## Venue Quality / Skew Overlap

Day 8 mismatches did not look like fragile 3-venue states:
- Day 8 mismatch venue eligibility pattern: `{'binance:True,coinbase:True,kraken:True': 1498}`
- Day 8 mismatch Binance near-outlier count: `47`
- Day 8 mismatch live venue-dispersion p50: `13.72500000`

Event-time skew also did not overlap the actionable side failures:
- Day 8 mismatch skew buckets: `{'absent': 1498}`

So event-time skew remains a quality caveat, but this audit does not support it as the direct cause of Day 8 side mismatch.

## Offline Filter Experiment

Dead-zone filters:
- Day 8 `dead_zone_medium` reduced kept rows but made survival worse: `-0.002964481376622421590616612126`
- Day 8 `dead_zone_conservative` eliminated all rows

Minimum-edge filters:
- Day 8 `min_edge_modest` survival improved from `0.009356814279938101074763486408` to `0.05123879021600910911826062384`
- Day 8 `min_edge_strict` survival improved to `0.1409109592618582079214441273`
- Day 7 `min_edge_strict` remained strong: `0.3536854348550719498443273452`

Combined filters:
- Day 8 `combined_medium` survival improved only to `0.04155545183056976738916657981`
- strict combined eliminated all rows

## Recommendation

Do not change live policy yet.

A neutral dead zone does not look promising on this two-day comparison. A stricter minimum-edge filter is more promising:
- it improved Day 8 survival materially
- it did not kill Day 7
- it needs more clean-day evidence before becoming a policy candidate

Working conclusion:
- Day 8 weakness was driven by live-vs-replay fair-value disagreement and directional side mismatch
- not by fill loss
- not by event-time skew overlap
- not by fragile missing-venue state
- not primarily by near-neutral boundary rows
