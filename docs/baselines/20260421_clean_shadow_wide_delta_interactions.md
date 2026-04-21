# Clean Shadow Wide-Delta Interaction Panel

Artifacts:
- [summary.json](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_wide_delta_interactions/20260421T040000Z/summary.json)
- [report.md](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_wide_delta_interactions/20260421T040000Z/report.md)
- [analyze_clean_shadow_wide_delta_interactions.py](/home/ubuntu/testingproject/scripts/analyze_clean_shadow_wide_delta_interactions.py)

Scope:
- clean shadow baseline days only
- wide comparable delta rows only: `5c_to_10c` and `gte_10c`
- focused comparison:
  - Day 8
  - middle cohort Day 9 / Day 10 / Day 11
- tested interaction axes:
  - delta x side-match regime
  - delta x hour of day
  - delta x calibration bucket
  - delta x volatility regime
  - delta x availability state

## Core Read

The panel does produce a second condition, but it is only partly strong:

- wide delta by itself is a real failure state
- `gte_10c` is generally bad across non-strong days
- the clearest additional separator is **wide delta combined with the replay wanting `lean_up` / `far_up`**
- a weaker secondary separator is **wide delta during `low_vol`**

The panel does **not** show a strong second separator from:
- availability state
- hour of day alone

## What Separates Day 8 From Day 9 / Day 10 / Day 11

### 1. Delta Bucket Alone Is Not Enough

`gte_10c` is bad on both Day 8 and the middle cohort:
- Day 8: side-match `0.3643`, survival `0.1632`
- middle cohort: side-match `0.4037`, survival `-0.0505`

So `>= 10c` identifies bad state, but it does **not** explain why Day 8 is the special failure day.

The more surprising difference is in `5c_to_10c`:
- Day 8: side-match `0.7639`, survival `-0.1736`
- middle cohort: side-match `0.7641`, survival `0.6104`

That means side-match rate by itself is not enough either. Day 8 can still fail even where the nominal wide-delta side-match rate looks middle-like.

### 2. Calibration Bucket Is The Strongest Second Condition

Day 8 wide-delta failure is heavily concentrated in the up-side replay buckets:

- `far_up`
  - Day 8: side-match `0.3307`, survival `-0.5487`
  - middle cohort: side-match `0.4588`, survival `0.0311`
- `lean_up`
  - Day 8: side-match `0.5852`, survival `-0.1010`
  - middle cohort: side-match `0.5704`, survival `0.2063`

By contrast, Day 8 is not broadly broken across every calibration bucket:
- `far_down`: survival `0.5225`
- `lean_down`: survival `0.3436`
- `near_mid`: survival `0.5731`

That is the cleanest conditional pattern in the panel:

> **Wide-delta rows are especially unsafe when replay wants the up-side tail.**

That is much more specific than “wide delta bad” alone.

### 3. Volatility Regime Helps, But Less Cleanly

Day 8:
- `high_vol`: survival `0.9779`, side-match `0.7044`
- `low_vol`: survival `0.0563`, side-match `0.2850`
- `mid_vol`: survival `-0.2054`, side-match `0.6189`

Middle cohort:
- `high_vol`: survival `0.4196`, side-match `0.7284`
- `low_vol`: survival `0.0065`, side-match `0.4218`
- `mid_vol`: survival `0.1358`, side-match `0.5519`

So `high_vol` is supportive on both Day 8 and the middle cohort. The real drag is `low_vol`, especially on Day 8, where side agreement collapses hardest there.

This makes volatility a plausible secondary interaction, but weaker than calibration bucket.

### 4. Availability State Does Not Separate The Cohorts

Wide-delta rows on both Day 8 and the middle cohort fall into only two meaningful states:
- `actionable`
- `non_actionable_3tv`

The split is similar enough that it does not explain the difference:
- Day 8 actionable share: `0.7314`
- middle cohort actionable share: `0.7688`

So this tranche does not support availability state as the missing second condition.

### 5. Hour Of Day Is Too Noisy

There are good and bad hours in both Day 8 and the middle cohort. Hour structure may still matter, but this panel does not show a clean hour-only separator strong enough to carry a rule.

## Decision

This tranche sharpens the project conclusion again:

- the first condition is real:
  - wide live-vs-replay calibrated fair-value divergence
- the strongest second condition found here is:
  - **wide delta x replay up-side bucket (`lean_up` / `far_up`)**
- a weaker secondary interaction is:
  - **wide delta x low-vol regime**

What it does **not** show:
- a robust standalone rule ready for refinement

The honest read is:
- there is a real conditional structure
- but it is still not strong enough to justify a policy change by itself
- if there is one final offline refinement path, it should target:
  - wide delta
  - plus up-side replay buckets
  - with low-vol treated as a possible supporting context

If that still does not survive a future bounded offline test, the clean conclusion is that the research phase is complete and the system remains a valid measurement engine rather than a deployment candidate.
