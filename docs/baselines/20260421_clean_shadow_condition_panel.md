# Clean Shadow Condition Panel

Artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_condition_panel/20260421T010000Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_condition_panel/20260421T010000Z/report.md)

Scope:
- Day 4 `20260327T093850581Z`
- Day 7 `20260401T112554963Z`
- Day 8 `20260407T110750965Z`
- Day 9 `20260411T031416598Z`
- Day 10 `20260412T123517467Z`
- Day 11 `20260414T072820542Z`

Headline:
- Day 7 is still the only day where high availability and high directional agreement happened together at scale.
- Day 9, Day 10, and Day 11 form the current middle band because they preserve some edge, but one of the two gates stays weak on each day.
- Day 4 and Day 8 remain weak for different reasons:
  Day 4 mostly failed on availability, while Day 8 had abundant 3-trusted state but poor directional agreement.

Most useful cuts:
- strong cohort mean 3-trusted rate: `0.8266549454144008`
- middle cohort mean 3-trusted rate: `0.4238968367056857666666666667`
- strong cohort mean side-match rate on actionable rows: `0.961837217156366`
- middle cohort mean side-match rate on actionable rows: `0.6172709222183253666666666667`
- weak cohort is heterogeneous:
  Day 4 is a low-availability collapse (`3.20%` 3-trusted),
  Day 8 is a side-agreement failure despite high availability (`93.74%` 3-trusted, `55.20%` side-match).

Available proxy for live-vs-replay disagreement:
- replay composite USD price is not persisted in replay artifacts, so the panel uses live-vs-replay calibrated fair-value delta as the available proxy
- outside the Day 7 strong regime, side-match weakens materially once the fair-value delta bucket widens

Operational read:
- event-time skew still overlaps the weak and middle cohorts
- fill loss remains negligible
- the practical bottleneck is still trusted-state formation first and directional agreement second

Decision read:
- if a future offline rule exists, it should be conditional on the specific availability + directional-agreement signature, not a blanket threshold
- if that signature does not stabilize with more clean days, the honest conclusion remains that the system is a valid measurement engine with regime-dependent economics
