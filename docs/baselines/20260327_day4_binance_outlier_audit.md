# Day 4 Binance Outlier Audit

Artifacts:
- report: [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_binance_outlier_audit/20260330T021500Z/report.md)
- machine summary: [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_binance_outlier_audit/20260330T021500Z/summary.json)
- extracted rows: [`binance_outlier_rows.jsonl`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_binance_outlier_audit/20260330T021500Z/binance_outlier_rows.jsonl)

Scope:
- session `20260327T093850581Z`
- all Day 4 shadow decisions where Binance was marked `outlier_rejected`

Headline counts:
- shadow decision rows inspected: `50920`
- Binance `outlier_rejected` rows: `47498`

Comparison-set strength:
- comparison-set size `3`: `47231`
- comparison-set size `2`: `267`
- weak comparison-set rate (`<=2`): `0.5621%`

So Binance was almost always judged against a full 3-venue comparison set, not a weak one.

Coinbase state at the same timestamps:
- Coinbase eligible: `46114`
- Coinbase `outlier_rejected`: `1118`
- Coinbase `future_source_ts`: `266`

This matters because it means Binance was usually being compared against both Coinbase and Kraken, not just one anchor venue.

Deviation shape:
- absolute deviation p50: `47.0350` USD
- absolute deviation p90: `55.3500` USD
- absolute deviation max: `132.3350` USD
- relative deviation p50: `7.0966` bps
- relative deviation p90: `8.3709` bps
- relative deviation max: `20.0583` bps

Reference thresholds from the composite-quality policy:
- outlier absolute threshold: `25` USD
- outlier relative threshold: `5` bps

So the median Binance rejection on Day 4 was not a near-miss. It was materially beyond both thresholds.

Hourly clustering:
- the heaviest Binance-outlier hours were `2026-03-28 01:00` through `2026-03-28 08:00 UTC`
- top hours:
  - `2026-03-28 07:00 UTC`: `2266`
  - `2026-03-28 05:00 UTC`: `2255`
  - `2026-03-28 08:00 UTC`: `2241`
  - `2026-03-28 06:00 UTC`: `2229`

This lines up with the same overnight dead-zone where Day 4 `3`-trusted-venue availability collapsed.

Conclusion:
- Binance is not being judged too harshly because of a weak comparison set.
- On Day 4, the dominant Binance problem was real divergence against a usually full 3-venue comparison set.
- That makes the main live-composite bottleneck narrower:
  - primary: Binance outlier behavior
  - secondary: smaller Coinbase eligibility loss
  - not: weak-reference comparison mechanics
