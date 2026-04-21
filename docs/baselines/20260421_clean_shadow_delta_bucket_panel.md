# Clean Shadow Delta-Bucket Panel

Artifacts:
- [summary.json](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_delta_bucket_panel/20260421T020000Z/summary.json)
- [report.md](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_delta_bucket_panel/20260421T020000Z/report.md)
- [analyze_clean_shadow_delta_bucket_panel.py](/home/ubuntu/testingproject/scripts/analyze_clean_shadow_delta_bucket_panel.py)

Scope:
- clean shadow baseline days only: Day 4, Day 7, Day 8, Day 9, Day 10, Day 11
- rows bucketed by `calibrated_fair_value_abs_delta_live_vs_replay`
- measured inside each bucket:
  - side-match on shadow-actionable rows
  - edge survival contribution
  - availability loss

## Core Read

The delta-bucket panel supports the narrow conditional-structure hypothesis.

Across the six clean days:
- low delta rows remain directionally stable:
  - `lt_2c` side-match is effectively `1.0` on every day with nontrivial rows
  - `2c_to_5c` side-match stays around `0.90` to `0.99`
- the first real degradation starts in `5c_to_10c`:
  - weak and middle days drop into the `0.71` to `0.78` side-match range
- the failure bucket is `gte_10c`:
  - Day 8: `0.3643`
  - Day 9: `0.4397`
  - Day 10: `0.3925`
  - Day 11: `0.4310`
  - even Day 7 only holds `0.6744` there

That is the cleanest repeatable signal in the panel:
- once comparable live-vs-replay calibrated fair-value divergence gets large, side agreement decays hard
- on middle and weak days, the `gte_10c` bucket is flat-to-negative survival

## Day 7 vs Rest

Day 7 remains the only strong day because it concentrated realized contribution in the low-delta buckets:
- low-delta realized share: `0.9074`
- wide-delta realized share: `0.0926`

The middle days still get most realized contribution from low-delta rows, but much less cleanly:
- Day 9 low / wide: `0.6099 / 0.3901`
- Day 10 low / wide: `0.5347 / 0.4653`
- Day 11 low / wide: `0.7065 / 0.2935`

Day 8 is the clearest weak-day failure:
- low-delta realized share is negative because low-delta replay rows were net negative on that day
- wide-delta realized share expands to `2.4027`
- `gte_10c` side-match collapses to `0.3643`

So the difference between the strong day and the middle/weak days is not just availability. It is whether the actionable realized contribution stays concentrated where live and replay calibrated fair values are still close.

## Availability Read

The missing-delta bucket is also important:
- it dominates replay PnL mass on several days
- it contributes zero realized PnL by construction
- it absorbs a large share of availability loss

This means the original project conclusion still holds:
- availability is the first drag
- side mismatch is the second drag

The delta-bucket panel refines that second drag:
- side mismatch is not uniform
- it worsens sharply when comparable live-vs-replay calibrated fair-value divergence gets large

## Practical Conclusion

This is the first serious conditional offline rule candidate that has survived cross-day scrutiny:
- not a blanket higher minimum-edge threshold
- but a conditional caution or exclusion when live-vs-replay calibrated fair-value divergence widens materially

Current evidence is still diagnostic, not a live policy change. The panel is strong enough to justify a future offline conditional experiment around wide-delta states, especially `gte_10c`.
