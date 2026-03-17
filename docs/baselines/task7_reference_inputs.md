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

## Frozen regime order

Task 7 regime comparison uses this order:

- `good_only`
- `degraded_only`
- `degraded_light_only`
- `degraded_light_plus_degraded_medium`
- `all_degraded`
- `good_plus_degraded_light`
- `good_plus_degraded_light_plus_degraded_medium`
- `all_windows`

## Frozen topline metrics

For each regime, compare:

- `trade_count`
- `hit_rate`
- `average_selected_raw_edge`
- `average_selected_net_edge`
- `total_pnl`
- `average_roi`

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
