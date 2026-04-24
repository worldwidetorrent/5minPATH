# Phase 1 Evidence Index

This index lists the primary evidence set that supports the Phase 1 closeout decision:
- [`0009_phase1_closeout.md`](../decisions/0009_phase1_closeout.md)

## Frozen Contract Decisions

- [`0005_policy_v1_and_admission_v2.md`](../decisions/0005_policy_v1_and_admission_v2.md)
- [`0006_stage1_good_only_calibration.md`](../decisions/0006_stage1_good_only_calibration.md)
- [`0007_stage2_far_up_correction_decision.md`](../decisions/0007_stage2_far_up_correction_decision.md)
- [`0008_execution_v0_shadow_boundary.md`](../decisions/0008_execution_v0_shadow_boundary.md)

## Clean Shadow Day Closeouts

- Day 4 baseline and execution:
  - [`20260327T093850581Z.md`](20260327T093850581Z.md)
  - [`20260327_day4_shadow_execution_baseline.md`](20260327_day4_shadow_execution_baseline.md)
  - [`20260327_day4_shadow_execution_gap_pass1.md`](20260327_day4_shadow_execution_gap_pass1.md)
  - [`20260327_day4_execution_gap_pass2.md`](20260327_day4_execution_gap_pass2.md)
  - [`20260327_day4_live_composite_availability.md`](20260327_day4_live_composite_availability.md)
  - [`20260327_day4_binance_outlier_audit.md`](20260327_day4_binance_outlier_audit.md)
- Day 7:
  - [`20260401T112554963Z.md`](20260401T112554963Z.md)
  - [`20260401_day7_shadow_execution_gap_pass1.md`](20260401_day7_shadow_execution_gap_pass1.md)
  - [`20260401_day7_execution_gap_pass2.md`](20260401_day7_execution_gap_pass2.md)
  - [`20260401_day7_event_clock_skew_audit.md`](20260401_day7_event_clock_skew_audit.md)
  - [`20260407_day7_edge_survival.md`](20260407_day7_edge_survival.md)
- Day 8:
  - [`20260411_day7_day8_side_mismatch_audit.md`](20260411_day7_day8_side_mismatch_audit.md)
- Day 9:
  - [`20260413_day9_edge_survival.md`](20260413_day9_edge_survival.md)
- Day 10:
  - [`20260414_day10_edge_survival.md`](20260414_day10_edge_survival.md)
- Day 11:
  - generated local fast-lane closeout artifact: `artifacts/day_fast_lane/date=2026-04-14/session=20260414T072820542Z/report/report.md`
  - generated local edge survival artifact: `artifacts/diagnostics/day11_edge_survival/20260419T172614Z/report.md`

## Cross-Day Comparison Set

- [`20260421_clean_shadow_baseline_edge_comparison.md`](20260421_clean_shadow_baseline_edge_comparison.md)
- [`20260421_clean_shadow_condition_panel.md`](20260421_clean_shadow_condition_panel.md)
- [`20260421_clean_shadow_delta_bucket_panel.md`](20260421_clean_shadow_delta_bucket_panel.md)
- [`20260421_clean_shadow_delta_gate_experiment.md`](20260421_clean_shadow_delta_gate_experiment.md)
- [`20260421_clean_shadow_wide_delta_interactions.md`](20260421_clean_shadow_wide_delta_interactions.md)

## Supporting Runtime / Block Context

- [`20260405_block_day4_day7_summary.md`](20260405_block_day4_day7_summary.md)
- [`20260414_clean_shadow_conditional_diagnosis_tranche.md`](20260414_clean_shadow_conditional_diagnosis_tranche.md)
- [`20260414_clean_shadow_min_edge_experiment.md`](20260414_clean_shadow_min_edge_experiment.md)

## Short Project Conclusion

Phase 1 built and validated:
- a full capture / replay / calibration / live-forward shadow measurement stack
- repeated clean runtime behavior
- persistent replay-calibration usefulness

Phase 1 also established:
- live survival is inconsistent across clean days
- availability and directional disagreement are the main bottlenecks
- fill mechanics are minor by comparison
- no deployment recommendation follows from the evidence
