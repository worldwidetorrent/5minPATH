# Clean Shadow Delta-Gate Experiment

Artifacts:
- [summary.json](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_delta_gate_experiment/20260421T030000Z/summary.json)
- [report.md](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_delta_gate_experiment/20260421T030000Z/report.md)
- [analyze_clean_shadow_delta_gate_experiment.py](/home/ubuntu/testingproject/scripts/analyze_clean_shadow_delta_gate_experiment.py)

Scope:
- clean shadow baseline days only: Day 4, Day 7, Day 8, Day 9, Day 10, Day 11
- offline diagnostic only
- compared:
  - current baseline
  - exclude `>= 10c` comparable live-vs-replay calibrated fair-value delta
  - exclude `>= 5c` comparable live-vs-replay calibrated fair-value delta
- rows with missing comparable delta stay in scope

## Question

Does a delta gate improve weak and middle days without materially damaging Day 7?

## Aggregate Read

Only one variant is even plausibly useful:
- `exclude_gte_10c`

Aggregate result:
- current survival: `0.09035403825268918085113881216`
- exclude `>= 10c` survival: `0.1050040581458865312756700794`
- exclude `>= 5c` survival: `0.08423284231717638061390609492`

So:
- excluding `>= 10c` improves weighted aggregate survival
- excluding `>= 5c` is too aggressive and makes the aggregate worse

The `>= 10c` gate also improves aggregate side-match strongly:
- current side-match: `0.6625654865851722`
- exclude `>= 10c`: `0.920468948035488`

But that is not enough by itself. The day-level and cohort-level behavior still matters.

## Day-Level Read

### Day 7

The `>= 10c` gate preserves and improves the strong day:
- current survival: `0.3371580735292590164938162539`
- exclude `>= 10c`: `0.3731772608034055583043157610`

This passes the first important subcheck.

### Day 9 / Day 10 / Day 11

The `>= 10c` gate improves the middle cohort:
- Day 9: `0.05367285720355915687494300661` -> `0.07094275098596662792093528487`
- Day 10: `0.07092105896267184501084398971` -> `0.1105017883095441183469210691`
- Day 11: `0.03777064777699521671627717347` -> `0.04576402783169588198405351330`

So the middle days do respond in the direction the delta-bucket panel suggested.

### Day 4 / Day 8

The weak cohort does **not** respond cleanly.

Day 4:
- unchanged under `>= 10c` because the day barely had any comparable wide-delta rows to remove

Day 8:
- current survival: `0.009356814279938101074763486408`
- exclude `>= 10c`: `-0.08341676140702153292722243904`

That is the blocking result.

It means wide-delta gating alone does **not** rescue the weak failure mode universally. On Day 8, removing `>= 10c` rows leaves a still-bad lower-delta remainder rather than fixing the day.

## Cohort Read

### Strong

The strong cohort is preserved:
- current: `0.3371580735292590164938162539`
- exclude `>= 10c`: `0.3731772608034055583043157610`

### Middle

The middle cohort improves:
- current: `0.05300877825367880921080530936`
- exclude `>= 10c`: `0.07014694703122190903180311179`

### Weak

The weak cohort gets worse:
- current: `0.007754457306715367115784892488`
- exclude `>= 10c`: `-0.002693006692802447912825255259`

That is enough to reject this as a universal conditional gate.

## Decision

The delta gate is a meaningful diagnostic, but it does **not** yet pass as a general refinement.

What it does support:
- wide comparable live-vs-replay calibrated fair-value divergence is a real failure state
- excluding `>= 10c` helps the strong day and the middle cohort

What it does **not** support:
- a one-rule wide-delta gate across all clean-shadow days

The honest next conclusion is narrower:
- the project found a real conditional failure state
- but that state is still not sufficient by itself to define a robust rule
- Day 8 still needs another condition beyond wide-delta exclusion

So the next serious offline refinement, if any, would have to be:
- `>= 10c` delta gate
- plus another condition that separates Day 8 from Day 9/10/11

Without that second condition, the system remains:
- a valid measurement engine
- economically inconsistent
- not ready for broader policy refinement or deployment
