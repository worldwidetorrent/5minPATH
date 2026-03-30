# 0005 Policy V1 And Admission V2

## Status

Accepted.

## Context

The 6-hour baseline session `20260316T101341416Z`, the 12-hour pilot `20260317T033427850Z`,
the 20-hour soak-validation session `20260320T071726065Z`, and the 24-hour full-day validation
session `20260321T131012752Z` all completed with the same
structural result:

- zero off-family drift
- zero unresolved selected bindings
- stable oracle continuity
- stable exchange continuity
- strong snapshot eligibility

Those runs also showed that session-wide degraded-count rejection was too blunt once the structural continuity checks were already passing. The remaining disagreement was not whether the sessions were usable at all, but which window-quality regimes should be admitted into replay and policy extraction.

The replay and sensitivity evidence across the pinned sessions now supports the following regime map:

- `good` is the clean baseline extraction universe
- `degraded_light` is a measurable second-tier exploratory overlay
- `degraded_medium` is only acceptable behind an explicit context gate
- `degraded_heavy` is excluded
- `unusable` is excluded

## Decision

Freeze the current research stack as:

- `admission semantics v2` for capture-session admission
- `policy v1` for replay and first-pass policy extraction

### Admission semantics v2

Session-level structural pass requires:

- completed run
- zero off-family drift
- zero unresolved selected bindings
- oracle continuity
- exchange continuity
- meaningful snapshot eligibility

Window-level inclusion then controls what may enter replay:

- `good` -> include
- `degraded_light` -> include only in exploratory mode
- `degraded_medium` -> include only if the explicit context gate passes
- `degraded_heavy` -> exclude
- `unusable` -> exclude

The previous session-wide degraded-count outcome remains preserved as `legacy_verdict` for comparison only.

### Policy v1

The sanctioned policy configs are:

- [`configs/replay/policies/good_only_baseline.yaml`](/home/ubuntu/testingproject/configs/replay/policies/good_only_baseline.yaml)
- [`configs/replay/policies/degraded_light_exploratory.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_light_exploratory.yaml)
- [`configs/replay/policies/degraded_medium_context_gated.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_medium_context_gated.yaml)

The sanctioned policy stacks are:

- [`configs/replay/policy_stacks/baseline_only.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_only.yaml)
- [`configs/replay/policy_stacks/baseline_plus_degraded_light.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_plus_degraded_light.yaml)
- [`configs/replay/policy_stacks/baseline_plus_degraded_light_gated_medium.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_plus_degraded_light_gated_medium.yaml)

Current policy stance:

- baseline extraction comes from `good_only`
- `degraded_light` is exploratory overlay only
- `degraded_medium` is exploratory only and must pass the explicit context gate
- `degraded_heavy` and `unusable` stay outside the trading universe

## Why

- The architecture and capture spine are stable enough to stop using branch-local semantics.
- The pinned 6-hour, 12-hour, 20-hour, and 24-hour sessions agree on the structural regime map.
- Window-level inclusion/exclusion matches the project design better than blunt session-wide degraded-count rejection.
- `good_only` remains the clean first policy baseline even after degraded-light and degraded-medium stress analysis and cross-horizon policy-stack comparison.

## Consequences

Positive:

- `main` can now carry one explicit admission contract and one explicit policy baseline.
- The 24-hour run now validates the hardened collector and frozen semantic stack over a full day.
- Future replay and capture comparisons can be judged against pinned sessions and versioned semantics.

Negative:

- `degraded_light` and gated `degraded_medium` remain exploratory rather than baseline-admitted.
- The full-day replay economics are mixed, so policy-v1 remains frozen structurally but should not be treated as economically settled without refreshed calibration and further validation.

## Revisit

The originally planned 24-hour refresh has already happened, followed by the Day 1 through Day 5
block sessions. Those later runs strengthened calibration support and did not overturn the frozen
regime map, so this decision remains in force.

Revisit only if a later replay comparison or execution-side evidence overturns the current regime
ordering rather than merely refining calibration or live tradability diagnostics.
