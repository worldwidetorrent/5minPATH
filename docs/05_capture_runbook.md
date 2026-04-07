# Phase-1 Capture Runbook

This repo now has one sanctioned capture path for the phase-1 data pull:

```bash
./scripts/run_collectors.sh
```

What it runs:

- Polymarket metadata collector
- Chainlink BTC/USD public Data Streams collector with RPC snapshot fallback
- Binance BTCUSDT quote collector
- Coinbase BTC-USD quote collector
- Kraken XBT/USD quote collector
- Polymarket quote collector for the selected BTC market

By default this is a one-shot capture pass, not a daemon. For bounded live work, the same sanctioned entrypoint now supports three operating profiles:

- `smoke`: coarse operational sanity checks
- `pilot`: denser replay-admission validation
- `admission`: same dense cadence profile intended for longer candidate-day work

The `pilot` and `admission` presets also widen sample-based failure thresholds for Chainlink, exchange, and Polymarket so 1-second sampling does not abort on a few seconds of transient loss. Pilot mode now also tolerates isolated unusable Polymarket windows better than admission mode, because the pilot is meant to finish and identify bad windows instead of dying on the first thin-book stretch.

## Deviation note

The original architecture still aims at source-faithful, continuously running collectors. The current phase-1 implementation is intentionally narrower:

- it is bounded-session polling, not long-running
- it uses public REST and RPC endpoints instead of the eventual websocket / RTDS-first stack
- it now prefers the public delayed Chainlink Data Streams BTC/USD endpoint instead of the old `latestRoundData` RPC-only path because the public stream feed is boundary-usable under the current anchor policy
- Polymarket metadata currently comes from the `up-or-down` event feed because that surface exposes the recurring BTC 5-minute family densely enough to select the exact target strip
- it persists the minimum real raw and normalized datasets needed to unblock replay-day admission work
- it now hardens the bounded acquisition path with retry/backoff, degraded-sample tracking, and threshold-based early termination because rollover-safe capture failed without that resilience layer
- it still falls back to `latestRoundData` RPC for Chainlink continuity when the public stream endpoint is unavailable, and it records oracle-source lineage explicitly in normalized ticks and window references

This deviation is deliberate. The immediate requirement is to prove the repo can produce real persisted files under the frozen layout without committing captured data or broadening the module surface prematurely.

## Start

Run from repo root:

```bash
./scripts/run_collectors.sh
```

Optional tuning:

```bash
./scripts/run_collectors.sh --timeout-seconds 30 --metadata-pages 2 --metadata-limit 500
```

10-minute smoke test:

```bash
./scripts/run_collectors.sh --capture-mode smoke --duration-seconds 600
```

20-minute boundary validation:

```bash
./scripts/run_collectors.sh --capture-mode pilot --duration-seconds 1200
```

2-hour pilot in `tmux`:

```bash
tmux new-session -d -s phaseb_pilot './scripts/run_collectors.sh --capture-mode pilot --duration-seconds 7200'
```

Optional per-source cadence override:

```bash
./scripts/run_collectors.sh \
  --capture-mode pilot \
  --metadata-poll-interval-seconds 30 \
  --chainlink-poll-interval-seconds 1 \
  --exchange-poll-interval-seconds 1 \
  --polymarket-quote-poll-interval-seconds 1 \
  --boundary-burst-enabled \
  --boundary-burst-window-seconds 15 \
  --boundary-burst-interval-seconds 1
```

Optional oracle-source override:

```bash
./scripts/run_collectors.sh --chainlink-source-preference snapshot_rpc
```

Optional resilience tuning:

```bash
./scripts/run_collectors.sh \
  --max-fetch-retries 3 \
  --base-backoff-seconds 0.5 \
  --max-backoff-seconds 5 \
  --heartbeat-interval-seconds 60 \
  --forward-progress-watchdog-seconds 300 \
  --chainlink-circuit-breaker-seconds 300 \
  --max-consecutive-polymarket-failures 3 \
  --max-consecutive-polymarket-failures-in-grace 5 \
  --max-consecutive-unusable-polymarket-windows 2 \
  --polymarket-unusable-window-min-quote-coverage-ratio 0.2 \
  --polymarket-rollover-grace-seconds 90
```

## Stop

- Normal operation: the command exits on its own after the configured pass or bounded session.
- If it hangs: press `Ctrl-C`.
- If running in `tmux`: `tmux kill-session -t phaseb_pilot`

## Logs

- Log file: `logs/collect_<session>.log`
- Summary artifact: `artifacts/collect/date=YYYY-MM-DD/session=<session>/summary.json`
- Partial summary artifact: `artifacts/collect/date=YYYY-MM-DD/session=<session>/summary.partial.json`
- Admission artifact: `artifacts/collect/date=YYYY-MM-DD/session=<session>/admission_summary.json`
- Per-sample diagnostics: `artifacts/collect/date=YYYY-MM-DD/session=<session>/sample_diagnostics.jsonl`

Crash-safe session checkpointing:

- the collector appends `sample_diagnostics.jsonl` as each sample completes; it is no longer an end-of-run-only dump
- the collector now appends raw and normalized session rows incrementally during the run, so partial sessions keep their landed market/oracle/quote data instead of waiting for one end-of-run write
- the collector rewrites `summary.partial.json` on a fixed checkpoint cadence; default is every `60` seconds
- `summary.partial.json` includes the lifecycle state/history, last completed sample number, last sample started timestamp, last artifact flush timestamp, last healthy timestamp per source, current selected `market_id` / slug / `window_id`, per-collector output paths/counts, and rolling failure counters
- if a long run dies before finalization, `summary.partial.json` plus `sample_diagnostics.jsonl` are the source of truth for the partial session record
- lifecycle states are explicit and machine-readable: `running`, `degraded`, `completed`, `failed_cleanly`, `aborted_watchdog`, `aborted_source_failure`
- the collector now emits periodic heartbeat log lines and aborts with `aborted_watchdog` if no sample completes within the configured forward-progress watchdog window
- repeated Chainlink network failure is now bounded by both request retry/backoff and a run-level circuit breaker; it either recovers or terminates cleanly instead of running as a zombie session
- malformed per-venue exchange payloads now degrade the exchange source and preserve payload-shape diagnostics in raw failure rows instead of aborting the full session on an uncaught parser error

## Output layout

Raw outputs:

- `data/raw/polymarket_metadata/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/raw/chainlink/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/raw/exchange/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/raw/polymarket_quotes/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`

Normalized outputs:

- `data/normalized/market_metadata_events/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/normalized/chainlink_ticks/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/normalized/exchange_quotes/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/normalized/polymarket_quotes/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`

## Health checks

Healthy collectors produce log lines showing:

- one selected BTC 5-minute target-family market with `market_id`, slug, and `window_id`
- one or more capture samples
- one or more Chainlink stream ticks or fallback rounds captured, with oracle-source lineage in diagnostics
- Binance, Coinbase, and Kraken quote snapshots captured on each sample
- one or more Polymarket quotes captured for the currently selected admitted family market
- capture-schedule details showing effective per-source interval and whether boundary burst mode is active for quote/oracle samples
- retry warnings only when a source is recovering, not on every sample
- heartbeat lines showing sample number, lifecycle, last completed sample, selected window, last flush time, and per-source health summary
- degraded samples are logged explicitly when one source is temporarily impaired
- Polymarket quote semantics are split between `valid_empty_book`, `quote_unavailable`, and `binding_invalid`
- Polymarket 404s near rollover trigger metadata refresh and selector re-evaluation before the session treats the binding as invalid
- one summary artifact path

Non-empty output means:

- each of the raw directories above contains at least one `.jsonl` file
- each of the normalized directories above contains at least one `.jsonl` file

## First sanity checks

Run these from repo root:

```bash
find data/raw -type f | sort
find data/normalized -type f | sort
python3 - <<'PY'
from pathlib import Path
for root in (Path("data/raw"), Path("data/normalized")):
    rows = sum(path.read_text(encoding="utf-8").count("\n") for path in root.rglob("*.jsonl"))
    print(root, rows)
PY
```

Check the latest summary:

```bash
find artifacts/collect -name summary.json | sort | tail -n 1 | xargs cat
find artifacts/collect -name summary.partial.json | sort | tail -n 1 | xargs cat
find artifacts/collect -name admission_summary.json | sort | tail -n 1 | xargs cat
```

For a smoke session, confirm:

- `sample_count` is greater than `1`
- `selector_diagnostics.candidate_count`, `admitted_count`, and `rejected_count_by_reason` are present in the summary artifact
- `selector_diagnostics.selected_window_id` is a canonical `btc-5m-...` window
- `session_diagnostics.empty_book_count`, `retry_count_by_source`, `retry_exhaustion_count_by_source`, and `termination_reason` are present in the summary artifact
- `session_diagnostics.polymarket_failure_count_by_class`, `polymarket_selector_refresh_count`, `polymarket_selector_rebind_count`, and `polymarket_rollover_grace_sample_count` are present in the summary artifact
- `session_diagnostics.polymarket_window_coverage` reports per-window quote coverage, empty-book counts, quote-unavailable counts, and a `good` / `degraded` / `unusable` verdict for each selected window
- `data/raw/chainlink/...` has non-empty rows with stamped `recv_ts`
- `data/normalized/chainlink_ticks/...` rows carry `oracle_source`, and boundary-validation runs should normally show `chainlink_stream_public_delayed`
- `data/normalized/exchange_quotes/...` contains non-empty `binance`, `coinbase`, and `kraken` rows
- `data/normalized/market_metadata_events/...` contains only admitted target-family rows, with slugs like `btc-updown-5m-<epoch>`
- `data/normalized/polymarket_quotes/...` market IDs stay inside the admitted target-family strip even if they roll across multiple 5-minute windows during the session

For a hardened pilot, also confirm:

- `session_diagnostics.termination_reason` is `completed`
- `sample_diagnostics.jsonl` contains `healthy` or `degraded` samples with per-source status detail
- `summary.partial.json` advances during the run and exposes `lifecycle_state`, `lifecycle_history`, `last_completed_sample_number`, `last_healthy_timestamp_by_source`, `selected_market_id`, `selected_market_slug`, and `selected_window_id`
- `summary.partial.json` also exposes `last_sample_started_at`, `last_artifact_flush_at`, `collector_outputs`, `chainlink_failure_started_at`, and the active circuit-breaker/watchdog configuration
- `admission_summary.json` reports family-compliance counts and off-family switch count from the final selected market/window binding per sample, while metadata-strip breadth and ambiguity are reported separately from family drift; it also includes degraded samples inside/outside rollover grace, Chainlink continuity, exchange venue continuity, mapped window count, open-anchor confidence breakdown, and `snapshot_eligible_sample_count`
- `admission_summary.json` now reports `chainlink_continuity.oracle_source_count` so the pilot can be judged on the actual oracle source used, not a generic Chainlink label
- unit regression coverage now pins the public-stream boundary-validation baseline, including the cross-midnight admission rollup, zero off-family drift, explicit `chainlink_stream_public_delayed` lineage, nonzero anchor confidence, and nonzero snapshot eligibility
- `sample_diagnostics.jsonl` shows 1-second effective `capture_interval_seconds` for `chainlink`, `exchange`, and `polymarket_quotes` during pilot/admission mode, with `boundary_burst_active` toggling near 5-minute boundaries
- `valid_empty_book` samples do not terminate the session by themselves; they now degrade the current window instead of incrementing the same hard-stop counter used for quote-unavailable or binding-invalid states
- any degraded Polymarket sample records `seconds_remaining`, `within_rollover_grace_window`, refresh-attempt flags, and final bound `market_id` / `window_id` in `source_results.polymarket_quotes.details`
- `admission_summary.json` now includes `empty_book_count_by_window`, `empty_book_count_by_slug`, and a per-window quote-coverage table with continuity flags plus `window_verdict`

If a long run fails before `summary.json` exists, summarize the checkpointed session with:

```bash
.venv/bin/python -m rtds.cli.summarize_partial_capture \
  artifacts/collect/date=YYYY-MM-DD/session=<session>/summary.partial.json
```

That command writes:

- `partial_session_summary.json`
- `partial_admission_summary.json` when the partial collector outputs are complete enough for replay-grade evaluation

## Pinned baseline sessions

The current pinned capture baselines are:

- [`20260316T101341416Z`](/home/ubuntu/testingproject/docs/baselines/20260316T101341416Z.md)
- [`20260317T033427850Z`](/home/ubuntu/testingproject/docs/baselines/20260317T033427850Z.md)
- [`20260320T071726065Z`](/home/ubuntu/testingproject/docs/baselines/20260320T071726065Z.md)
- [`20260321T131012752Z`](/home/ubuntu/testingproject/docs/baselines/20260321T131012752Z.md)
- [`20260324T040815584Z`](/home/ubuntu/testingproject/docs/baselines/20260324T040815584Z.md)
- [`20260325T052115572Z`](/home/ubuntu/testingproject/docs/baselines/20260325T052115572Z.md)
- [`20260326T055920480Z`](/home/ubuntu/testingproject/docs/baselines/20260326T055920480Z.md)
- [`20260327T093850581Z`](/home/ubuntu/testingproject/docs/baselines/20260327T093850581Z.md)
- [`20260329T002704901Z`](/home/ubuntu/testingproject/docs/baselines/20260329T002704901Z.md)
- [`20260401T112554963Z`](/home/ubuntu/testingproject/docs/baselines/20260401T112554963Z.md)

The original Task 7 reference bundle is still documented here:

- [`Task 7 Reference Inputs`](/home/ubuntu/testingproject/docs/baselines/task7_reference_inputs.md)

The current semantic freeze for promotion to `main` is:

- [`0005 Policy V1 And Admission V2`](/home/ubuntu/testingproject/docs/decisions/0005_policy_v1_and_admission_v2.md)

The current machine-readable cross-horizon analysis manifests are:

## Optimized post-run workflow

Use the optimized workflow for new daily sessions so the default closeout is cheap and the heavy cumulative refresh is optional.

Fast lane after a completed session:

```bash
./scripts/run_day_optimized_postrun.sh YYYY-MM-DD <session-id>
```

What that does:

- runs the per-session policy-stack replay
- runs the single-session calibrated baseline replay against the latest frozen calibration summary
- writes the day fast-lane summary under `artifacts/day_fast_lane/...`
- writes per-session rollups under `artifacts/session_rollups/...`

Use the heavy cumulative refresh only when the day is clearly a milestone day or when you want to validate the incremental path:

```bash
./scripts/run_day_optimized_postrun.sh YYYY-MM-DD <session-id> --checkpoint
```

That second form additionally:

- updates the cumulative calibration state under `artifacts/policy_v1/state/...`
- emits refreshed `good_only_calibration_summary.json` and `cross_horizon_summary.json`
- writes the tracker entry for the session

Recommended Day 8 order:

1. Run capture + shadow under the frozen contract.
2. Run `./scripts/run_day_optimized_postrun.sh YYYY-MM-DD <session-id>` immediately after completion.
3. Inspect the fast-lane summary and shadow classification.
4. Only run `--checkpoint` if Day 8 looks like a milestone day or if you want a validation rebuild.

Checkpoint cadence:

- run the heavy checkpoint every `3` clean/valid sessions
- run it after a major runtime patch
- run it before a formal report milestone
- run it after a new clean shadow baseline day if you do not already have two clean shadow baseline days

Heavy checkpoint scope:

- cumulative calibration refresh validation
- refreshed `cross_horizon_summary.json`
- block-level report regeneration
- clean-shadow-baseline comparison refresh

What not to do:

- do not run the heavy checkpoint after every daily close by default
- do not treat full-history recomputation as the normal fast-lane follow-up

- [`configs/baselines/analysis/policy_v1_cross_horizon.json`](/home/ubuntu/testingproject/configs/baselines/analysis/policy_v1_cross_horizon.json)
- [`configs/baselines/analysis/policy_v1_calibrated_baseline.json`](/home/ubuntu/testingproject/configs/baselines/analysis/policy_v1_calibrated_baseline.json)

## Post-run analysis lanes

The research contract is frozen. Post-run analysis is now split into two lanes so daily closes do not force a full cumulative rebuild every time.

Daily fast lane:

```bash
./scripts/run_day_fast_lane.sh YYYY-MM-DD <session-id>
```

- runs one-session `compare_policy_stacks`
- runs one-session `compare_calibrated_session` against the latest frozen calibration summary
- writes `artifacts/day_fast_lane/date=.../session=.../summary.json`
- writes `artifacts/day_fast_lane/date=.../session=.../report/report.md`
- writes per-session rollups under `artifacts/session_rollups/date=.../session=.../`:
  - `session_policy_rollup.json`
  - `session_calibration_rollup.json`
  - `session_shadow_rollup.json` when shadow artifacts exist
- use this lane for daily decision support: capture pass/fail, admission verdict, window-quality mix, raw `baseline_only`, calibrated `baseline_only`, and quick shadow Stage A

Checkpoint heavy lane:

```bash
./scripts/run_checkpoint_refresh.sh YYYY-MM-DD <session-id>
```

- reruns cumulative `compare_calibrated_baseline`
- reruns cumulative `build_policy_v1_baseline`
- updates the tracker entry for the supplied session against the refreshed cumulative artifacts
- prefers the rollup-driven checkpoint CLIs and falls back to the older full rebuild only when one or more pinned sessions are still missing `session_policy_rollup.json` or `session_calibration_rollup.json`
- the incremental calibration state lives at `artifacts/policy_v1/state/good_only_calibration_state_v1.json`
- the intended steady-state path is:
  - new day writes `session_calibration_rollup.json`
  - checkpoint merges that one rollup into the cumulative state
  - checkpoint emits refreshed `good_only_calibration_summary.json` and `cross_horizon_summary.json`
- use this lane less often, when you actually want the support map, cross-horizon report, and tracker to move

Legacy all-in-one chain:

```bash
./scripts/run_day_analysis_chain.sh YYYY-MM-DD <session-id>
```

- still exists
- still performs the old serial flow
- but it is now the expensive compatibility path rather than the intended daily-close default

The older Task 7 manifest remains here for the original pinned comparison slice:

- [`configs/baselines/analysis/task7_reference_runs.json`](/home/ubuntu/testingproject/configs/baselines/analysis/task7_reference_runs.json)

Refresh its admission summary after code changes with:

```bash
.venv/bin/python -m rtds.cli.refresh_capture_admission \
  --summary-path artifacts/collect/date=2026-03-16/session=20260316T101341416Z/summary.json \
  --baseline-manifest configs/baselines/capture/20260316T101341416Z.json
```

Replay that exact session with:

```bash
.venv/bin/python -m rtds.cli.replay_day \
  --date 2026-03-16 \
  --session-id 20260316T101341416Z \
  --rebuild-reference true \
  --rebuild-snapshots true
```

`--session-id` matters here because replay otherwise reads the whole UTC date partition.

Compare the pinned session across the expanded window-quality regimes with:

```bash
.venv/bin/python -m rtds.cli.compare_replay_regimes \
  --date 2026-03-16 \
  --session-id 20260316T101341416Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

Use [`configs/replay/task7_reference_comparison.yaml`](/home/ubuntu/testingproject/configs/replay/task7_reference_comparison.yaml) for both Task 7 reference sessions so replay matches the 1-second capture granularity instead of oversampling 1-second state at 250 ms.
- the comparison now runs these regimes: `good_only`, `degraded_only`, `degraded_light_only`, `degraded_medium_only`, `degraded_heavy_only`, `good_plus_degraded_light`, `good_plus_degraded_light_medium`, `good_plus_all_degraded`, and `all_windows`
- `admission_summary.json` is the source of truth for those window labels, and each selected window is now classified as `good`, `degraded_light`, `degraded_medium`, `degraded_heavy`, or `unusable`
- the classifier contract is now explicit and versioned in [`configs/replay/window_quality_classifier_v1.json`](/home/ubuntu/testingproject/configs/replay/window_quality_classifier_v1.json), and each admission summary emits `polymarket_continuity.window_quality_classifier`
- the main comparison excludes `unusable` windows and treats them as a footnote contamination check rather than as part of the main economic regime table
- `snapshot_eligible_sample_count` is currently a conservative capture-side proxy because `build_snapshots` is still a placeholder

Stress the same session under execution-sensitive degraded-regime assumptions with:

```bash
.venv/bin/python -m rtds.cli.compare_execution_sensitivity \
  --date 2026-03-16 \
  --session-id 20260316T101341416Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

- the execution-sensitivity matrix now writes `artifacts/replay_sensitivity/.../sensitivity_summary.json`
- the default variants are `baseline_execution`, `slippage_1_5x`, `slippage_2x`, `half_size`, `tight_spread_cap_0_02`, `strict_quote_coverage_0_95`, and `degraded_light_candidate_policy`
- use [`configs/replay/policies/good_only_baseline.yaml`](/home/ubuntu/testingproject/configs/replay/policies/good_only_baseline.yaml) as the first policy baseline
- use [`configs/replay/policies/degraded_light_exploratory.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_light_exploratory.yaml) only as a second-tier exploratory overlay
- capture admission is now `v2`: `verdict` is continuity-first and window-aware, while the old blunt session-wide result remains in `legacy_verdict`

Run the focused degraded follow-up on the 12-hour reference session with:

```bash
.venv/bin/python -m rtds.cli.analyze_degraded_regimes \
  --date 2026-03-17 \
  --session-id 20260317T033427850Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

- this writes `artifacts/replay_degraded_analysis/.../degraded_analysis_summary.json`
- it focuses on `degraded_light_only` and `degraded_medium_only`
- it stress-tests `baseline_execution`, `slippage_1_5x`, `slippage_2x`, and `half_size`
- it decomposes both regimes by `seconds_remaining_bucket`, `volatility_regime`, `spread_bucket`, `raw_edge_bucket`, `net_edge_bucket`, and `chainlink_confidence_state`
- current conclusion on the pinned 12-hour session: `degraded_medium` survives slippage stress but its strength concentrates in stronger-edge, wider-spread, and mid/high-volatility slices, so it remains exploratory rather than part of the first policy baseline

Run the window-aware policy stack on a pinned session with:

```bash
.venv/bin/python -m rtds.cli.compare_policy_stacks \
  --date 2026-03-17 \
  --session-id 20260317T033427850Z \
  --config configs/replay/task7_reference_comparison.yaml \
  --rebuild-reference true \
  --rebuild-snapshots true
```

- this writes `artifacts/replay_policy_stack/.../policy_stack_summary.json`
- the sanctioned stacks are [`baseline_only.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_only.yaml), [`baseline_plus_degraded_light.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_plus_degraded_light.yaml), and [`baseline_plus_degraded_light_gated_medium.yaml`](/home/ubuntu/testingproject/configs/replay/policy_stacks/baseline_plus_degraded_light_gated_medium.yaml)
- the sanctioned policy configs are [`good_only_baseline.yaml`](/home/ubuntu/testingproject/configs/replay/policies/good_only_baseline.yaml), [`degraded_light_exploratory.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_light_exploratory.yaml), and [`degraded_medium_context_gated.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_medium_context_gated.yaml)
- when `--session-id` is present, replay now loads that session across all matching `date=*/session=<id>` normalized partitions, so cross-midnight sessions replay as one contiguous run
- current result across the pinned 6-hour, 12-hour, 20-hour, and 24-hour sessions: `good` remains the clean baseline universe, `degraded_light` is a measurable exploratory overlay, and gated `degraded_medium` adds only a narrow extra slice rather than a new default trading universe

Run the formal cross-horizon policy-stack comparison with:

```bash
.venv/bin/python -m rtds.cli.compare_policy_horizons \
  --manifest configs/baselines/analysis/policy_v1_cross_horizon.json
```

- this writes `artifacts/replay_policy_horizon/.../comparison_summary.json`
- it compares the same three sanctioned stacks across the pinned 6-hour, 12-hour, 20-hour, and 24-hour sessions
- it preserves one fixed replay contract via [`configs/replay/task7_reference_comparison.yaml`](/home/ubuntu/testingproject/configs/replay/task7_reference_comparison.yaml)
- use this artifact to verify that `good` remains the clean baseline universe, `degraded_light` remains a weaker exploratory overlay, and gated `degraded_medium` remains a narrow incremental slice as horizon length increases, while also checking whether the first full-day replay changes the economic sign or support map materially

Build the first serious policy-v1 report plus the stage-1 `good_only` calibration artifact with:

```bash
.venv/bin/python -m rtds.cli.build_policy_v1_baseline
```

- this writes `artifacts/policy_v1/run_.../cross_horizon_summary.json`
- it also writes `artifacts/policy_v1/run_.../good_only_calibration_summary.json`
- the stage-1 calibration contract is [`configs/replay/calibration_good_only_v1.json`](/home/ubuntu/testingproject/configs/replay/calibration_good_only_v1.json)
- the calibration is intentionally coarse: `good_only` windows only, bootstrap confidence intervals, and support flags `sufficient`, `thin`, and `merge_required`
- use this artifact to decide where the current fair-value prior is informed enough to carry a coarse correction and where additional clean-window data is still more valuable than a finer fit

Apply the frozen stage-1 calibrator to `baseline_only` replay across the pinned horizons with:

```bash
.venv/bin/python -m rtds.cli.compare_calibrated_baseline \
  --manifest configs/baselines/analysis/policy_v1_calibrated_baseline.json
```

- this writes `artifacts/replay_calibrated_baseline/.../comparison_summary.json`
- it also writes per-session row-level outputs under `artifacts/replay_calibrated_baseline/.../sessions/session_<id>/rows.jsonl`
- each row persists raw and calibrated `F`, calibration bucket, support flag, raw and calibrated selected edges, and raw and calibrated simulated trade results
- the frozen calibrated baseline contract keeps `policy v1`, `admission semantics v2`, `window_quality_classifier_v1`, `calibration_good_only_v1`, `chainlink_stream_public_delayed`, and the task-7 replay assumptions fixed while testing the calibrator
- current result across the pinned 6-hour, 12-hour, 20-hour, and 24-hour sessions is mixed: calibration improves total PnL materially on the 6-hour, 20-hour, and 24-hour `baseline_only` runs, but it worsens the 12-hour `baseline_only` run, so the next step is calibrator diagnosis rather than policy expansion
