# Replay Vs Live Eligibility Parity

Date: `2026-03-26`

## Question

Does live shadow reject exchange venues, especially Coinbase, for reasons replay would not?

## Verdict

There is a **configuration mismatch**, not a timestamp-basis mismatch.

What is the same:
- replay and live both use exchange `event_ts` for freshness
- replay and live both use the shared freshness/composite code:
  - [`assess_source_freshness`](/home/ubuntu/testingproject/src/rtds/quality/freshness.py)
  - [`assess_exchange_composite_quality`](/home/ubuntu/testingproject/src/rtds/quality/dispersion.py)
  - [`compute_composite_nowcast`](/home/ubuntu/testingproject/src/rtds/features/composite_nowcast.py)
- replay and live both exclude exchange quotes from composite input unless
  `normalization_status == "normalized"` and `crossed_market_flag == false`

What is different:
- replay currently uses `max_composite_age_ms: 1500` from
  [`quality_thresholds.yaml`](/home/ubuntu/testingproject/configs/replay/quality_thresholds.yaml)
- replay currently uses `min_active_venues: 2` from
  [`quality_thresholds.yaml`](/home/ubuntu/testingproject/configs/replay/quality_thresholds.yaml)
- live shadow currently uses the feature defaults in
  [`composite_nowcast.py`](/home/ubuntu/testingproject/src/rtds/features/composite_nowcast.py)
  and [`freshness.py`](/home/ubuntu/testingproject/src/rtds/quality/freshness.py):
  - stale after `2000 ms`
  - minimum trusted venues `3`

## Evidence

Replay snapshot rebuilding in [`replay_day.py`](/home/ubuntu/testingproject/src/rtds/cli/replay_day.py):
- builds `FreshnessPolicy(stale_after_ms=max_composite_age_ms, missing_after_ms=max_composite_age_ms * 5)`
- passes that policy into both:
  - `assess_exchange_composite_quality(...)`
  - `compute_composite_nowcast(...)`
- sets `minimum_venue_count=min_active_venues`

Live shadow assembly in [`state_assembler.py`](/home/ubuntu/testingproject/src/rtds/execution/state_assembler.py):
- calls `compute_composite_nowcast(quotes, as_of_ts=sample_ts)` with no override
- calls `assess_source_freshness(... last_event_ts=quote.event_ts)` for venue diagnostics
- therefore uses the default feature freshness policy and default minimum venue count

Composite quote validity in [`composite_nowcast.py`](/home/ubuntu/testingproject/src/rtds/features/composite_nowcast.py):
- `_latest_valid_quotes_by_venue(...)` drops quotes if:
  - `quote.crossed_market_flag`
  - `quote.normalization_status != "normalized"`

## Practical conclusion

Live shadow is **not** rejecting Coinbase because it switched to a different timestamp basis than replay.

Instead:
- both stacks use `event_ts`
- live is actually **more permissive** on exchange freshness (`2000 ms` vs replay `1500 ms`)
- but live is **stricter** on required trusted venue count (`3` vs replay `2`)

So if the question is “would replay reject Coinbase for a reason live would not?”, the answer is:
- on freshness alone: replay is at least as strict, and often stricter
- on whole-composite admissibility: live is stricter because it requires three trusted venues

## Bottom line

Parity result:
- timestamp basis: `same`
- quote-validity rule: `same`
- freshness threshold: `mismatch found`
- minimum trusted venue count: `mismatch found`

This means the current live shadow blockage is not caused by a hidden Coinbase-only semantic fork. The real live-vs-replay mismatch is the active freshness/venue-count configuration.
