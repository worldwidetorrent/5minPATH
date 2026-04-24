# 0009 — Phase 1 Closeout

## Status

Accepted on `main`.

## Context

Phase 1 set out to answer a bounded question:

> Can this repo build a durable capture, replay, calibration, and live-forward shadow measurement stack that both detects real signal and converts enough of that signal under live conditions to justify further refinement or deployment thinking?

By the close of Phase 1, the repo had accumulated:
- stable capture under the frozen contract
- admission semantics `v2`
- a stable policy `v1` universe split
- a stage-1 `good_only` calibrator
- repeated clean live-forward shadow runtime baselines
- six clean shadow comparison days:
  - Day 4
  - Day 7
  - Day 8
  - Day 9
  - Day 10
  - Day 11

The key evidence set is indexed in:
- [`20260421_phase1_evidence_index.md`](../baselines/20260421_phase1_evidence_index.md)

## What Was Built

Phase 1 successfully built and exercised:
- durable multi-source capture
- replay analysis over session-scoped normalized datasets
- calibrated replay evaluation
- a frozen execution-v0 shadow sidecar boundary
- fast-lane session closeout and incremental analysis support
- cross-day clean-shadow comparison diagnostics

## What Was Validated

Phase 1 validated all of the following:
- the system is operationally real
- runtime cleanliness repeats on clean shadow days
- calibration is consistently economically useful in replay
- some modeled edge survives into live-forward shadow conditions on certain days

The strongest positive evidence is:
- Day 7 demonstrated strong survival on a clean post-fix shadow day

The stronger project-level conclusion, however, is distributional:
- clean runtime repeated
- replay calibration remained useful
- live edge survival stayed inconsistent across days

## What Was Rejected

Phase 1 also closed several candidate explanations and refinements:
- fill mechanics are not the main drag
- a blanket stricter minimum-edge filter is not a valid universal policy refinement
- a simple wide-delta exclusion rule is not a valid universal policy refinement
- no broad policy redesign is justified from the Phase 1 evidence
- no deployment recommendation is justified from the Phase 1 evidence

## Final Read

The dominant live-survival drags are:
1. availability
2. side mismatch / directional disagreement
3. fill loss is minor

Phase 1 also identified real conditional structure:
- wide live-vs-replay calibrated fair-value divergence is a real failure state
- the strongest second condition found was wide delta combined with replay up-side buckets (`lean_up` / `far_up`)
- a weaker secondary context was low-vol

That conditional structure is real enough to diagnose the failure mode, but not strong enough to justify policy change or deployment.

## Decision

Close Phase 1 here.

Declare:

> Phase 1 complete: validated measurement engine, no deployment recommendation.

More fully:
- the repo successfully validated a market-data capture, replay, calibration, and live-forward shadow measurement engine
- calibration consistently improved replay economics
- live edge survival was real but economically inconsistent across clean days
- the main live bottlenecks were composite availability and directional disagreement, not fill mechanics
- Phase 1 does not support deployment

## Consequences

What this repo may honestly claim from Phase 1:
- real predictive structure exists under the replay/calibration framework
- the current stack can measure that structure live-forward without conflating runtime failure with economics
- the current technique does not convert that structure into consistent live-survival economics across days

What this repo should not claim from Phase 1:
- a production-ready policy
- a robust standalone conditional rule
- a deployment recommendation

If future work is reopened, it should be treated as a separate project, not as a continuation of Phase 1.

Recommended next project name:
- `Conditional Survival Refinement`

That project should start only by explicit choice, with a fresh bounded objective and the same guardrails:
- no production-ready policy claim
- no robust standalone conditional-rule claim
- no deployment recommendation without new evidence
