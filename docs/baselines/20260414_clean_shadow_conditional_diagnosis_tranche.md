# Clean Shadow Conditional-Diagnosis Tranche

## Objective

Do not redesign the system again.

The next tranche answers one narrow question:

> Why does edge survival vary so much across clean shadow days, and is there a repeatable conditional pattern worth trusting?

The expanded five-day minimum-edge experiment is closed as a universal policy test:
- useful as a diagnostic
- rejected as a blanket policy change

## Frozen Contract

Do not change:
- `policy v1`
- stage-1 `good_only` calibrator
- 3-venue composite rule
- venue set
- capture contract
- shadow `live_only_from_attach_ts` attach mode
- fast-lane workflow

Exit condition:
- new evidence remains comparable to Day 4, Day 7, Day 8, Day 9, and Day 10.

## Collection Plan

For Day 11 and onward:
- run capture + shadow normally
- run fast lane only after completion
- classify capture as valid or not valid
- classify shadow as clean baseline or quarantined
- run the quick edge-survival pass
- append session rollups
- do not run a heavy checkpoint automatically

For each clean shadow day, record:
- raw baseline-only PnL
- calibrated baseline-only PnL
- edge survival ratio
- joined trade rate
- actionable rate on calibrated rows
- side-match rate
- availability loss
- side-mismatch loss
- fill loss
- event-time skew rate

## Stop Rule

Stop this collection phase after:
- `3` more clean shadow days, or
- `2` weeks,

whichever comes first.

This keeps the empirical extension bounded and prevents endless drift.

## Decision Gate

After the stop rule fires, compare the expanded clean-shadow set and ask:
- Are strong-survival days rare exceptions?
- Are weak and middle days explained by a repeatable availability or side-agreement condition?
- Is there a conditional filter worth testing offline, rather than a blanket policy threshold?

Do not move to live trading or policy redesign before this gate.

## Out Of Scope

Do not:
- redesign `policy v1`
- add venues
- relax the 3-venue rule
- change the calibrator
- treat the blunt minimum-edge threshold as a default policy
- reintroduce heavy full-history recompute as the daily default
