# Clean Shadow Baseline Structure Comparison

Artifacts:
- [`summary.json`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_baseline_structure_comparison/20260409T141500Z/summary.json)
- [`report.md`](/home/ubuntu/testingproject/artifacts/diagnostics/clean_shadow_baseline_structure_comparison/20260409T141500Z/report.md)

Compared sessions:
- Day 4 `20260327T093850581Z`
- Day 7 `20260401T112554963Z`
- Day 8 `20260407T110750965Z`

Focus:
- availability rate
- joined trade rate
- shadow actionable rate on calibrated rows
- side-match rate
- hour-of-day structure

Working conclusion:
- Day 4 was weak because availability was almost absent.
- Day 7 was strong because availability and side agreement were both strong.
- Day 8 had Day 7-like availability, but not Day 7-like side agreement.

So the most useful explanation of Day 7 vs Day 8 is:
- not infrastructure
- not replay/shadow join rate
- mainly directional agreement under live conditions once the row was actually available
