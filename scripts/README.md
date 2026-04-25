# Scripts Guide

This folder contains operational wrappers and research-analysis scripts.

`5minPATH` is a research/data pipeline, so the repository preserves both the primary user path and the diagnostic scripts used to produce the final evidence set. Do not treat every file in this directory as a required workflow.

## Primary User-Facing Scripts

These are the main scripts a new user should care about:

- `run_collectors.sh` — sanctioned bounded capture entrypoint
- `run_day_fast_lane.sh` — cheap daily post-run closeout
- `run_day_optimized_postrun.sh` — heavier optimized closeout path

Start here before running any one-off analysis scripts.

## Operational Support Scripts

These support the documented capture and analysis workflows:

- `run_shadow_live.sh` — live-forward shadow sidecar launcher
- `run_checkpoint_refresh.sh` — checkpoint-style heavier refresh path
- `run_day_analysis_chain.sh` — historical full day analysis chain
- `build_day.sh` — day artifact build helper
- `evaluate_day.sh` — day evaluation helper

Use these only when following the runbooks or reproducing a specific documented workflow.

## Showcase And Evidence Scripts

These generate or summarize project evidence:

- `analyze_edge_survival.py`
- `analyze_shadow_execution.py`
- `analyze_side_mismatch_audit.py`
- `analyze_clean_shadow_baseline_edge_comparison.py`
- `analyze_clean_shadow_condition_panel.py`
- `analyze_clean_shadow_delta_bucket_panel.py`
- `analyze_clean_shadow_delta_gate_experiment.py`
- `analyze_clean_shadow_min_edge_experiment.py`
- `analyze_clean_shadow_wide_delta_interactions.py`

They are useful for reproducing evidence notes, but they are not the first commands a new user should run.

## Historical Diagnostic Scripts

These are retained for reproducibility of the research trail. They are not the primary user interface:

- `analyze_day4_execution.py`
- `analyze_day5_future_leak.py`

Similar day-specific or incident-specific scripts should be read as diagnostic artifacts, not as part of the normal user path.

## Rule Of Thumb

If you are a new user, start with:

1. `README.md`
2. `docs/how_to_use.md`
3. `scripts/run_collectors.sh`
4. `scripts/run_day_fast_lane.sh`
5. optional `dashboard/`

Most other scripts are reproducibility artifacts or research diagnostics.
