# Clean Shadow Baseline Edge Comparison Through Day 11

Artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_baseline_edge_comparison/20260421T000000Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_baseline_edge_comparison/20260421T000000Z/report.md)

Compared sessions:
- Day 4 `20260327T093850581Z`
- Day 7 `20260401T112554963Z`
- Day 8 `20260407T110750965Z`
- Day 9 `20260411T031416598Z`
- Day 10 `20260412T123517467Z`
- Day 11 `20260414T072820542Z`

Distribution summary:
- mean survival: `0.0860645028943708122144425247`
- median survival: `0.04572175249027718679561009004`
- min survival: `0.007507565613801537116011238129`
- max survival: `0.3371580735292590164938162539`
- weak days: `2` (`Day 4`, `Day 8`)
- middle days: `3` (`Day 9`, `Day 10`, `Day 11`)
- strong days: `1` (`Day 7`)

Interpretation:
- Day 7 remains the only strong-survival clean-shadow day.
- Day 9, Day 10, and Day 11 now form the middle band.
- Day 4 and Day 8 remain the weak days, but for different reasons:
  availability collapse on Day 4 and side-agreement failure on Day 8.
- Day 11 joins the middle band with `3.78%` survival and a `29.62%` 3-trusted-venue rate, so the main drag remains availability first and side mismatch second.
- Fill loss remains negligible across the clean-shadow set.

Working conclusion:
- runtime cleanliness is repeating
- calibration remains economically valuable in replay
- Day 7 still looks like a rare strong-survival regime rather than the current norm
- the current norm is weak-to-middle survival under the frozen contract
- do not change policy yet; continue the measurement program or stop and write the project-level distribution conclusion

This note supersedes [`20260414_clean_shadow_baseline_edge_comparison.md`](/home/ubuntu/testingproject/docs/baselines/20260414_clean_shadow_baseline_edge_comparison.md).
