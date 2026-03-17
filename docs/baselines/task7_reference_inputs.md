# Task 7 Reference Inputs

This note freezes the sanctioned inputs for Task 7 degraded-regime analysis.

## Goal

Keep the next comparison tranche reproducible:

- same replay path
- same execution assumptions
- same quality labels
- same topline metrics

## Frozen reference sessions

- 6-hour baseline: [`20260316T101341416Z`](/home/ubuntu/testingproject/docs/baselines/20260316T101341416Z.md)
- 12-hour pilot: [`20260317T033427850Z`](/home/ubuntu/testingproject/docs/baselines/20260317T033427850Z.md)

The machine-readable manifest tying both sessions together is:

- [`configs/baselines/analysis/task7_reference_runs.json`](/home/ubuntu/testingproject/configs/baselines/analysis/task7_reference_runs.json)

## Fixed comparison contract

Use this replay config for both sessions:

- [`configs/replay/task7_reference_comparison.yaml`](/home/ubuntu/testingproject/configs/replay/task7_reference_comparison.yaml)

That contract fixes:

- `snapshot_cadence_ms = 1000`
- `max_composite_age_ms = 1500`
- `max_oracle_age_ms = 5000`
- `min_active_venues = 2`
- `taker_fee_bps = 0`
- `slippage_buffer_bps = 10`
- `model_uncertainty_bps = 15`
- `fast_return_count = 10`
- `baseline_return_count = 30`

## Frozen quality labels

For Task 7, window quality comes from the capture admission artifact only:

- `good`
- `degraded_light`
- `degraded_medium`
- `degraded_heavy`
- `unusable`

Source of truth:

- `artifacts/collect/.../admission_summary.json`
- `polymarket_continuity.window_quote_coverage[*].window_verdict`
- `polymarket_continuity.window_quality_classifier.classifier_version = window_quality_v1`
- [`configs/replay/window_quality_classifier_v1.json`](/home/ubuntu/testingproject/configs/replay/window_quality_classifier_v1.json)

The current classifier thresholds are explicit and versioned. They are keyed off:

- `quote_coverage_ratio`
- `degraded_samples_outside_rollover_grace_window`
- `max_consecutive_valid_empty_book`
- `snapshot_eligible_ratio`

## Frozen regime order

Task 7 regime comparison uses this order:

- `good_only`
- `degraded_only`
- `degraded_light_only`
- `degraded_medium_only`
- `degraded_heavy_only`
- `good_plus_degraded_light`
- `good_plus_degraded_light_medium`
- `good_plus_all_degraded`
- `all_windows`

## Frozen topline metrics

For each regime, compare:

- `trade_count`
- `hit_rate`
- `average_selected_raw_edge`
- `average_selected_net_edge`
- `total_pnl`
- `average_roi`
- `pnl_per_window`
- `pnl_per_1000_snapshots`
- `pnl_per_100_trades`

## Sanctioned commands

6-hour baseline:

```bash
.venv/bin/python -m rtds.cli.compare_replay_regimes \
  --date 2026-03-16 \
  --session-id 20260316T101341416Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

12-hour pilot:

```bash
.venv/bin/python -m rtds.cli.compare_replay_regimes \
  --date 2026-03-17 \
  --session-id 20260317T033427850Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

Do not switch back to ad hoc replay configs for this tranche.

Focused degraded follow-up on the 12-hour pilot:

```bash
.venv/bin/python -m rtds.cli.analyze_degraded_regimes \
  --date 2026-03-17 \
  --session-id 20260317T033427850Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

That follow-up keeps the same replay contract and adds two sanctioned overlays:

- medium-specific execution stress for `degraded_light_only` and `degraded_medium_only`
- context decomposition by `seconds_remaining_bucket`, `volatility_regime`, `spread_bucket`, `raw_edge_bucket`, `net_edge_bucket`, and `chainlink_confidence_state`

Window-aware policy-stack comparison on the pinned sessions:

```bash
.venv/bin/python -m rtds.cli.compare_policy_stacks \
  --date 2026-03-17 \
  --session-id 20260317T033427850Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

That comparison uses three sanctioned stack configs:

- [`baseline_only.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_only.yaml)
- [`baseline_plus_degraded_light.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_plus_degraded_light.yaml)
- [`baseline_plus_degraded_light_gated_medium.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_plus_degraded_light_gated_medium.yaml)

The current policy stance those stacks encode is:

- baseline: [`good_only_baseline.yaml`](/home/ubuntu/testingproject/configs/replay/policies/good_only_baseline.yaml)
- exploratory light overlay: [`degraded_light_exploratory.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_light_exploratory.yaml)
- exploratory medium gate: [`degraded_medium_context_gated.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_medium_context_gated.yaml)

## Combined window-quality summary

Rebuild one machine-readable summary with per-window verdicts for both reference runs:

```bash
.venv/bin/python -m rtds.cli.export_window_quality_summary \
  --manifest-path configs/baselines/analysis/task7_reference_runs.json
```

That writes:

- `artifacts/analysis/task7_reference_runs/window_quality_summary.json`
