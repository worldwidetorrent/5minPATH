# Clean Shadow Baseline Edge Comparison Through Day 9

Artifacts:
- Day 4 [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day4_edge_survival/20260407T120000Z/summary.json)
- Day 7 [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day7_edge_survival/20260407T120000Z/summary.json)
- Day 8 [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day8_edge_survival/20260409T080000Z/summary.json)
- Day 9 [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/day9_edge_survival/20260413T094543Z/summary.json)

Compared sessions:
- Day 4 `20260327T093850581Z`
- Day 7 `20260401T112554963Z`
- Day 8 `20260407T110750965Z`
- Day 9 `20260411T031416598Z`

Scope:
- clean shadow runtime sessions only
- calibrated replay rows joined to live-forward shadow decisions
- one-contract normalized edge-survival method

## Headline

Day 7 remains the only high-survival clean shadow day so far.

Day 9 improved over Day 4 and Day 8, but still preserved only a modest fraction of calibrated modeled edge.

Edge survival ratios:
- Day 4: `0.007507565613801537116011238129`
- Day 7: `0.3371580735292590164938162539`
- Day 8: `0.009356814279938101074763486408`
- Day 9: `0.05367285720355915687494300661`

## Comparison Table

| Day | Replay expected PnL | Shadow realized PnL | Survival | Joined rate | Actionable on calibrated rows | Side-match on actionable |
|---|---:|---:|---:|---:|---:|---:|
| Day 4 | `2325.654` | `17.46` | `0.007507565613801537116011238129` | `0.6451426686458849` | `0.002391555335642421` | `0.9655172413793104` |
| Day 7 | `1829.231` | `616.74` | `0.3371580735292590164938162539` | `0.6349359415897506` | `0.40790742526518803` | `0.961837217156366` |
| Day 8 | `472.383` | `4.42` | `0.009356814279938101074763486408` | `0.6333640068358091` | `0.4395951097673196` | `0.5520334928229665` |
| Day 9 | `2094.541` | `112.42` | `0.05367285720355915687494300661` | `0.6586773967809657` | `0.06630510846745977` | `0.6543535620052771` |

## Interpretation

- Day 4 was mainly an availability failure: very few calibrated rows became shadow-actionable.
- Day 7 was the strong case: availability and side agreement were both high.
- Day 8 had Day 7-like availability but weak side agreement.
- Day 9 had high replay strength and normal join rate, but actionability on calibrated rows fell back down to `6.63%`, making availability the dominant drag again.

Working conclusion:
- runtime cleanliness is repeating
- calibrated replay remains economically meaningful
- live edge survival is regime-dependent
- superseded by `docs/baselines/20260414_clean_shadow_baseline_edge_comparison.md` after Day 10 closed; the expanded minimum-edge experiment is recorded in `docs/baselines/20260414_clean_shadow_min_edge_experiment.md`
