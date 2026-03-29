# Day 4 Live Composite Availability Diagnosis

Session: `20260327T093850581Z`

Primary artifacts:

- report artifact: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_live_composite_availability/20260329T000000Z/report.md)
- machine summary: [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_live_composite_availability/20260329T000000Z/summary.json)
- source decisions: [`shadow_decisions.jsonl`](/home/ubuntu/testingproject/artifacts/shadow/20260327T093850581Z/shadow_decisions.jsonl)

## Headline

Across the full Day 4 live-forward shadow session:

- decision rows: `50920`
- `3`-trusted-venue rows: `1631` (`3.2031%`)
- fair-value-available rows: `1631` (`3.2031%`)
- actionable rows: `1443` (`2.8339%`)

So the Day 4 sidecar did produce a real execution candidate set, but only after a very small fraction of rows reached a valid `3`-venue composite.

## Venue Eligibility Rates

- Coinbase eligibility rate: `93.9317%`
- Binance eligibility rate: `5.9112%`
- Kraken trusted rate: `98.3523%`

This already explains most of the bottleneck:

- Kraken was almost always trusted
- Coinbase was usually eligible
- Binance was almost never eligible

## Root Cause Mix

Coinbase ineligible reasons:

- `outlier_rejected`: `2084`
- `future_source_ts`: `629`
- `stale_source`: `373`
- `event_age_hard_cap_exceeded`: `3`
- `missing_source`: `1`

Binance ineligible reasons:

- `outlier_rejected`: `47498`
- `stale_source`: `408`
- `event_age_hard_cap_exceeded`: `4`

Kraken ineligible reasons:

- `stale_source`: `409`
- `outlier_rejected`: `426`
- `event_age_hard_cap_exceeded`: `4`

Important Day 4 finding:

- Binance upstream normalization exclusion was no longer the blocker
- Binance stayed on `normalized_with_missing_event_ts` for all `50920` rows, but that status was already admitted by the shadow-live patch
- the real Binance blocker on Day 4 was the outlier path, not upstream exclusion

## Hourly Shape

The live composite window was strongly time-varying.

Best hours:

- `2026-03-27 12:00 UTC`: `3`-trusted rate `15.73%`, actionable rate `14.36%`
- `2026-03-27 13:00 UTC`: `3`-trusted rate `15.88%`, actionable rate `13.20%`

Weakest stretch:

- from roughly `2026-03-27 21:00 UTC` through `2026-03-28 05:00 UTC`, `3`-trusted rate was effectively near zero
- during those same hours, Binance eligibility collapsed to roughly `0.5%` to `1.7%`
- Kraken stayed near `98%+`
- Coinbase mostly stayed in the `95%+` range

That means the overnight collapse was overwhelmingly a Binance problem, not a broad all-venue collapse.

## Interpretation

This tranche answers the main question directly.

Why only about `1.6k` of `~50.9k` rows reached `3` trusted venues:

- not mainly Binance upstream exclusion
- not mainly Coinbase freshness basis
- mostly the Binance outlier path

More precisely:

- Binance was eligible on only `5.9112%` of Day 4 rows
- Binance was marked `outlier_rejected` on `47498` rows
- Coinbase freshness failures were only `373` rows total
- even Coinbase total ineligible rows were much smaller than Binance outlier rejections

So the Day 4 composite bottleneck was primarily:

1. Binance outlier rejection
2. Coinbase secondary eligibility loss, mostly outlier/future-source issues rather than freshness alone
3. Kraken was the stable anchor, not the problem

## Bottom Line

The full-day Day 4 diagnosis says the live composite bottleneck is mostly the Binance outlier path.

The data does **not** support the simpler story that the session was mainly blocked by:

- Binance upstream normalization exclusion, or
- Coinbase freshness basis alone

The next execution-side diagnosis should therefore stay narrow:

- inspect why Binance is rejected as an outlier so often in the live-forward path
- inspect Coinbase `future_source_ts` and outlier behavior as the secondary issue
- do not revisit capture, policy, or regime semantics based on this tranche
