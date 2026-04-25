# Sample Outputs

This page gives a quick view of the kind of output `5minPATH` produces without requiring a reader to run a full capture first.

The numbers below summarize the six clean shadow baseline days used in the final evidence set. The survival, 3-trusted-venue, and side-match rates are copied from `artifacts/diagnostics/clean_shadow_condition_panel/20260421T010000Z/summary.json`.

## Clean Shadow Edge Survival

| Day | Edge Survival | 3-Trusted-Venue Rate | Side-Match Rate | Class |
| --- | ---: | ---: | ---: | --- |
| Day 4 | 0.75% | 3.20% | 96.6% | weak |
| Day 7 | 33.72% | 82.67% | 96.2% | strong |
| Day 8 | 0.94% | 93.74% | 55.2% | weak |
| Day 9 | 5.37% | 21.13% | 65.4% | middle |
| Day 10 | 7.09% | 76.42% | 53.3% | middle |
| Day 11 | 3.78% | 29.62% | 66.5% | middle |

Distribution summary:

- mean survival: about 8.6%
- median survival: about 4.6%
- weak / middle / strong days: 2 / 3 / 1

## Failure Anatomy

The live-forward comparison decomposes modeled edge loss into three practical buckets.

| Component | Meaning | Final Read |
| --- | --- | --- |
| Availability loss | Replay found a trade, but the live shadow state did not have enough trusted venue/composite support to act. | Main drag |
| Side mismatch loss | Replay and live shadow were both actionable, but disagreed on direction or state. | Second drag |
| Fill loss | Intended live entry was worse than the replay assumption at top of book. | Minor |

The project conclusion is not that the signal was fake. It is that the current technique did not convert the signal consistently enough under live conditions.

## Conditional Finding

The strongest conditional failure signal was wide live-vs-replay calibrated fair-value divergence.

Bucketed side agreement showed:

| Live-vs-Replay Delta Bucket | Observed Pattern |
| --- | --- |
| less than 2c | side agreement stayed stable where volume existed |
| 2c to 5c | side agreement stayed strong |
| 5c to 10c | degradation began |
| 10c or more | failure bucket on weak and middle days |

That finding was useful diagnostically, but not enough to justify a policy change by itself. A wide-delta exclusion improved some strong/middle cases but failed the weak cohort, especially Day 8.

## Bottom Line

The sample output supports the same project conclusion as the closeout docs:

- the measurement engine works
- calibration is useful in replay
- live edge survival exists but is inconsistent
- deployment is not recommended from the current evidence
