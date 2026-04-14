# testingproject

A data-first research system for **pricing, replaying, and evaluating 5-minute BTC Polymarket markers that resolve on Chainlink**.

The project is built around one core idea:

> the edge is not exotic math; the edge is **timestamped alignment**.

This repository exists to answer a very specific question with high discipline:

> At any moment inside a 5-minute BTC market, what is the best estimate of the contract's **oracle-relevant fair value**, given the information that was actually knowable at that time?

That question sounds small. It is not.

## Current status

The architecture described in this README is still the target design. The codebase now implements the core research spine, one canonical replay-day execution path, and a sanctioned bounded capture path that can materialize full-day raw and normalized public-endpoint sessions. It still does not implement the fully continuous streaming collector fleet or authenticated execution path described later in this document.

Implemented on `main`:

- canonical core IDs, enums, time and validation utilities
- Polymarket metadata discovery and candidate normalization
- deterministic market-to-window mapping
- deterministic Chainlink open/settle assignment
- persisted `window_reference` dataset writes
- normalized Binance/Coinbase/Kraken quote state
- normalized Polymarket executable quote state
- freshness, dispersion, and gap-quality modules
- composite nowcast, volatility, baseline fair value, and executable edge
- snapshot assembly, offline label attachment, taker-only replay simulation, and slice analysis
- canonical `replay_day` runner with deterministic artifact output
- sanctioned `./scripts/run_collectors.sh` phase-1 capture command that writes real raw and normalized JSONL files
- integration coverage for the replay-day artifact contract
- bounded capture resilience with retry/backoff, degraded-sample diagnostics, threshold-based early termination, and capture-session admission summaries
- crash-safe long-run capture checkpointing, lifecycle states, watchdogs, and partial-session summarization
- window-aware admission semantics `v2` with `legacy_verdict` preserved for comparison
- pinned 6-hour, 12-hour, 20-hour, and 24-hour baseline sessions with reproducible replay/admission contracts
- policy-v1 replay stacks and cross-horizon comparison across pinned sessions
- first serious policy-v1 report plus stage-1 coarse `good_only` calibration with uncertainty and support flags
- frozen raw-vs-calibrated `baseline_only` replay comparison across the pinned 6-hour, 12-hour, 20-hour, and 24-hour sessions
- execution-v0 shadow-sidecar boundaries plus a production-safe capture-output `live_state` adapter, frozen book-pricing, tradability, policy-decision, simple sizing, append-only shadow evidence, structured summary metrics, reconciliation outputs, and a live-forward fail-open shadow runtime
- a thin shadow launcher now exists to attach the execution sidecar to one active capture session without capture-side code changes

Not yet implemented end to end:

- dedicated `build_snapshots` CLI is still a placeholder
- raw event schemas are still conceptual rather than fully implemented in code
- the production live shadow path is implemented only for the capture-output `live_state` adapter boundary; there is still no websocket market-data path or authenticated execution path
- the full intended streaming collector fleet is not implemented; the current capture path is a bounded public-endpoint snapshot session rather than a continuously running service
- most downstream admission and replay logic still expects curated day partitions rather than a continuously running ingestion service

When the code is narrower than the design described below, the design should be read as the intended architecture and the narrower implementation as the current phase-1 state.

### Current execution-side evidence state

The execution sidecar is no longer just an interface freeze. The repo now has:

- a production-safe capture-output `live_state` adapter
- a live-forward shadow launcher
- append-only shadow decisions, order-state transitions, outcomes, and replay-comparison artifacts
- shutdown reconciliation for shadow summaries
- five clean live-forward shadow runtime comparison days on Day 4, Day 7, Day 8, Day 9, and Day 10

Current baseline interpretation:

- Day 4 and Day 7 are the strongest clean live-forward shadow runtime baselines for the early block
- Day 8, Day 9, and Day 10 repeated clean shadow runtime behavior, but their modeled-edge survival was materially weaker than Day 7
- Day 5 capture is valid, but Day 5 shadow remains quarantined as historical evidence because `future_state_leak_detected` appeared on 46 rows before the recv-time visibility fix
- Day 6 is a debugging specimen only: capture failed cleanly on a Kraken payload-shape issue and shadow mixed broad event-time skew with the old visibility-leak classification before the recv-vs-event split
- Day 10 capture, shadow, fast-lane replay/calibration, and edge-survival closeout completed cleanly; it preserved `7.09%` of calibrated modeled edge, better than Day 4/Day 8/Day 9 but still far below Day 7
- the expanded five-day minimum-edge experiment did not pass as a universal policy refinement: stricter filters preserved Day 7 and improved Day 8, but were flat-to-worse on Day 4/Day 9 and materially worse on Day 10
- the Day 5 leak was traced to Polymarket row visibility using `event_ts` before `recv_ts`; that narrow edge case is now patched on `main`
- the current shadow leak split is explicit: `future_recv_visibility_leak` is the true as-of violation, while `future_event_clock_skew` is tracked as a separate timestamp-quality class

So the current open execution-side problem is no longer runtime isolation. It is whether calibrated modeled edge survives live execution conditions consistently. The observed drags are live composite availability plus day-dependent side agreement; Day 8 and Day 10 showed that high availability can still fail economically when live-vs-replay directional agreement is weak.

### Current analysis workflow

The research contract is frozen while the workflow is being made cheaper:

- `policy v1`, `admission semantics v2`, the stage-1 `good_only` calibrator, the 3-venue composite rule, and the live-only shadow attach contract remain fixed
- the daily close now has two intended lanes:
  - fast lane: one-session capture/admission summary, one-session policy-stack replay, one-session calibrated baseline replay against the latest frozen calibration summary, and a quick shadow Stage A read
  - checkpoint lane: the expensive cumulative `compare_calibrated_baseline` plus `build_policy_v1_baseline` refresh and tracker update
- new sessions can now emit per-session rollups under `artifacts/session_rollups/date=.../session=.../`:
  - `session_policy_rollup.json`
  - `session_calibration_rollup.json`
  - `session_shadow_rollup.json`
- the cumulative calibration state now has a versioned home at `artifacts/policy_v1/state/good_only_calibration_state_v1.json`
- the intended checkpoint path is now rollup-driven rather than raw-row-driven: new days should update the cumulative calibration state by merging one session rollup, while a full rebuild remains available as a validation path
- until the historical pinned sessions are backfilled with calibration rollups, the checkpoint wrapper falls back to the older full rebuild path automatically

The goal is to keep daily research moving without paying the full-history recomputation tax after every capture day.

### Current backup posture

Bulk research data and generated artifacts are intentionally not tracked in Git.

As of the 2026-04-14 doc refresh:

- local raw/normalized data plus artifacts had grown into a large generated corpus dominated by raw Polymarket metadata JSONL
- the completed corpus through Day 9 was archived and uploaded to Google Drive under `testingproject_backups/day9_and_prior_20260413`
- Day 10 was archived, checksumed, `zstd -t` verified, and uploaded separately to Google Drive under `testingproject_backups/day10_20260412T123517467Z`
- Git remains the home for code, docs, configs, and small tracked reports; multi-GB raw/artifact backups belong in external storage, not normal Git branches

### Checkpoint cadence

Heavy cumulative refresh is no longer the default daily path.

Use the checkpoint lane:

- every `3` clean/valid sessions
- after a major runtime patch
- before a formal report milestone
- after a new clean shadow baseline day materially changes the evidence set or before a formal comparison refresh

A heavy checkpoint should include:

- cumulative calibration refresh validation
- refreshed `cross_horizon_summary.json`
- block-level report regeneration
- clean-shadow-baseline comparison refresh

Do not spend the heavy checkpoint cost after every daily close unless one of the conditions above is true.

### Current phase-1 deviation from target design

The original architecture still matters and remains the north star. The current capture implementation deviates from it in a few deliberate ways:

- It uses a bounded orchestration session instead of long-running collectors. Reason: the immediate success metric is to prove the repo can land coherent real files in a controlled smoke-test window before expanding to full-day operations.
- It uses public REST snapshots for Binance, Coinbase, Kraken, and Polymarket, and now prefers the public delayed Chainlink Data Streams BTC/USD endpoint with `latestRoundData` only as fallback. Reason: the public stream endpoint is the first official Chainlink source in this repo that is actually boundary-usable under the current anchor policy, while the old RPC snapshot path remains useful for continuity when the stream endpoint is unavailable.
- Polymarket metadata discovery currently pulls from the `up-or-down` event feed rather than the broader market search surface. Reason: that feed exposes the recurring BTC 5-minute family densely enough to admit the exact target strip without scanning thousands of unrelated events first.
- The live selector now admits only exact BTC 5-minute family candidates and binds them to canonical `window_id`s before quote capture. Reason: the repo’s canonical grammar treats `window_id` as primary and market binding as a downstream step, so live capture now follows that same contract.
- During bounded sessions, quote capture can roll between admitted family members as the live 5-minute window advances. Reason: the target family is recurring, so a 10-minute smoke run must stay on the same family while moving from one canonical window to the next.
- The sanctioned collector now has explicit bounded capture densities: smoke mode can stay coarse, while pilot/admission mode runs 1-second Chainlink, exchange, and Polymarket quote polling with slower metadata refresh plus optional 5-minute boundary burst scheduling. The denser presets also widen sample-based failure thresholds so a 1-second pilot does not inherit 60-second smoke tolerances verbatim. Reason: replay admission depends on boundary-local anchor and quote coverage, not just process survival.
- The bounded capture path now distinguishes three Polymarket quote states: `valid_empty_book`, `quote_unavailable`, and `binding_invalid`. Valid empty books degrade samples and count against per-window quote coverage, while quote-unavailable and binding-invalid states still drive the harder stop policy. Reason: thin but valid market states should not be treated as if the source or selector had failed.
- Pilot mode now judges Polymarket quote quality at both the sample and window level. Reason: one thin-book 5-minute market should be reported as a bad window, not automatically collapsed into a whole-session verdict before the next window is observed.
- Admission now sub-buckets impaired Polymarket windows into `degraded_light`, `degraded_medium`, `degraded_heavy`, and `unusable`, and replay comparison can evaluate those regimes separately. Reason: the current research question is no longer whether degraded windows exist, but which degraded windows remain economically usable for policy extraction.
- Each bounded capture session now writes an `admission_summary.json` artifact beside `summary.json`. Reason: pilot-length reruns now need replay-usable continuity diagnostics, not just process-survival logs. The admission rollup now treats final per-sample selected market/window bindings as the source of truth for family continuity and reports metadata-strip breadth or ambiguity separately so refresh rows do not masquerade as off-family drift. Because `build_snapshots` is still a placeholder, `snapshot_eligible_sample_count` is currently a conservative capture-side proxy based on family compliance, selected-window mapping, anchor confidence, and per-sample source completeness.
- Oracle lineage is now explicit in normalized `chainlink_ticks` and `window_reference` assignment outputs. Reason: the repo now distinguishes `chainlink_stream_public_delayed` from `chainlink_snapshot_rpc` so boundary validation can be judged on the actual oracle source used, not on an implied generic "Chainlink" label. See `docs/decisions/0004_oracle_source_boundary_capture.md`.
- The public-stream boundary-validation case is now pinned by admission-summary regression coverage, including zero off-family drift, explicit `chainlink_stream_public_delayed` lineage, nonzero anchor confidence across a midnight-spanning window set, and nonzero snapshot eligibility. Reason: the oracle-path/midnight-rollup combination is now a fixed acceptance baseline, not an anecdotal one-off run.
- The first structurally healthy 6-hour pilot is now pinned as a local baseline session at [`docs/baselines/20260316T101341416Z.md`](/home/ubuntu/testingproject/docs/baselines/20260316T101341416Z.md), with a machine-readable manifest in [`configs/baselines/capture/20260316T101341416Z.json`](/home/ubuntu/testingproject/configs/baselines/capture/20260316T101341416Z.json) and a rerunnable admission-refresh command. Reason: this run is the current “healthy but noisy” reference case and should remain replayable even as admission logic evolves.
- The original degraded-regime Task 7 tranche froze both the 6-hour baseline and the 12-hour pilot under one shared contract at [`docs/baselines/task7_reference_inputs.md`](/home/ubuntu/testingproject/docs/baselines/task7_reference_inputs.md), with a machine-readable manifest in [`configs/baselines/analysis/task7_reference_runs.json`](/home/ubuntu/testingproject/configs/baselines/analysis/task7_reference_runs.json) and a fixed replay config in [`configs/replay/task7_reference_comparison.yaml`](/home/ubuntu/testingproject/configs/replay/task7_reference_comparison.yaml). Reason: that early tranche needed reproducible economic comparisons across pinned sessions instead of ad hoc reruns with drifting configs, and it now remains as historical baseline context rather than the current top-level analysis contract.
- The 20-hour soak-validation session is now pinned as [`docs/baselines/20260320T071726065Z.md`](/home/ubuntu/testingproject/docs/baselines/20260320T071726065Z.md) with a machine-readable manifest in [`configs/baselines/capture/20260320T071726065Z.json`](/home/ubuntu/testingproject/configs/baselines/capture/20260320T071726065Z.json). Reason: long-run durability is no longer hypothetical; this run crossed the prior failure horizon and serves as the first successful soak baseline.
- The first full-day validation session is now pinned as [`docs/baselines/20260321T131012752Z.md`](/home/ubuntu/testingproject/docs/baselines/20260321T131012752Z.md) with a machine-readable manifest in [`configs/baselines/capture/20260321T131012752Z.json`](/home/ubuntu/testingproject/configs/baselines/capture/20260321T131012752Z.json). Reason: the hardened collector and admission-v2 path now have one completed 24-hour reference input, even though the first full-day policy-stack replay is economically mixed.
- The degraded-window classifier is now explicit and versioned in [`configs/replay/window_quality_classifier_v1.json`](/home/ubuntu/testingproject/configs/replay/window_quality_classifier_v1.json), and capture admission emits that classifier contract beside every per-window verdict table. Reason: the pinned baseline sessions need stable `good` / `degraded_light` / `degraded_medium` / `degraded_heavy` / `unusable` labels that can be reapplied consistently across the 6-hour, 12-hour, and 20-hour runs.
- The canonical replay runner now accepts `--session-id` for session-scoped rebuilds and, when present, loads that session across all matching UTC date partitions. Reason: pinned baseline sessions must be replayed against their own full normalized run, including cross-midnight captures, not against either an accidental union of the whole date partition or a silent truncation at midnight.
- The first policy split is now explicit: [`configs/replay/policies/good_only_baseline.yaml`](/home/ubuntu/testingproject/configs/replay/policies/good_only_baseline.yaml) is the baseline extraction universe, while [`configs/replay/policies/degraded_light_exploratory.yaml`](/home/ubuntu/testingproject/configs/replay/policies/degraded_light_exploratory.yaml) is a stricter exploratory overlay only. Reason: execution-sensitivity replay on the pinned 6-hour session showed that even `degraded_light` windows remain economically distinct from `good_only`, so degraded windows should not contaminate the first policy baseline even though a controlled second-tier overlay is now measurable.
- The focused degraded follow-up on the pinned 12-hour session now stress-tests `degraded_light_only` and `degraded_medium_only` under `baseline`, `1.5x slippage`, `2x slippage`, and `half_size`, and decomposes both regimes by `seconds_remaining_bucket`, `volatility_regime`, `spread_bucket`, `raw_edge_bucket`, `net_edge_bucket`, and `chainlink_confidence_state`. Reason: the current question is no longer whether degraded windows exist, but whether `degraded_medium` is a real regime and where its economics actually come from. The current result is that `degraded_medium` survives slippage stress but clusters in stronger-edge, wider-spread, and mid/high-volatility slices, so it remains exploratory and context-gated rather than baseline-admitted.
- Admission semantics are now `v2`: session-level continuity and snapshot eligibility determine whether a run is structurally usable, while window-level verdicts determine what can actually enter replay and policy extraction. The old blunt degraded-count result is preserved as `legacy_verdict` for comparison. Reason: the pinned 6-hour, 12-hour, and 20-hour sessions all showed that session-wide rejection was too blunt once family continuity, oracle continuity, exchange continuity, and snapshot eligibility were already strong.
- The first window-aware policy stack is now encoded in replay configs: baseline `good` windows only, exploratory `degraded_light` overlay, and tightly gated `degraded_medium` overlay for `large_positive_edge`, `mid/high_vol`, and `wide_spread` slices. Reason: policy-stack replay across the pinned 6-hour, 12-hour, and 20-hour sessions confirmed that the good-window baseline remains the clean extraction universe, while degraded overlays add narrower exploratory flow rather than a new merged default universe.
- The formal policy-v1 cross-horizon contract is now pinned in [`configs/baselines/analysis/policy_v1_cross_horizon.json`](/home/ubuntu/testingproject/configs/baselines/analysis/policy_v1_cross_horizon.json), and the consolidated comparison can be regenerated with `python -m rtds.cli.compare_policy_horizons`. Reason: the next question is no longer per-session viability, but whether the same three sanctioned stacks preserve or change their shape across the early 6-hour/12-hour/20-hour/validation baselines and the later daily block sessions.
- The first serious policy-v1 report and the first stage-1 `good_only` calibration pass can now be regenerated with `python -m rtds.cli.build_policy_v1_baseline`. Reason: the policy structure is no longer provisional, so the repo now needs one formal report plus one uncertainty-aware baseline calibration artifact instead of only scattered comparison summaries.
- The frozen raw-vs-calibrated `baseline_only` replay contract is now pinned in [`configs/baselines/analysis/policy_v1_calibrated_baseline.json`](/home/ubuntu/testingproject/configs/baselines/analysis/policy_v1_calibrated_baseline.json), and the comparison can be regenerated with `python -m rtds.cli.compare_calibrated_baseline`. Reason: the next diagnostic question is whether the current `good_only` calibrator actually improves baseline replay when applied only to buckets with sufficient support.
- The active semantic freeze is recorded in [`docs/decisions/0005_policy_v1_and_admission_v2.md`](/home/ubuntu/testingproject/docs/decisions/0005_policy_v1_and_admission_v2.md). Reason: `policy v1` and `admission semantics v2` are now live on `main`, so the repo needs one explicit decision record rather than only scattered baseline notes.
- The first baseline calibration contract is now frozen in [`docs/decisions/0006_stage1_good_only_calibration.md`](/home/ubuntu/testingproject/docs/decisions/0006_stage1_good_only_calibration.md) and [`configs/replay/calibration_good_only_v1.json`](/home/ubuntu/testingproject/configs/replay/calibration_good_only_v1.json). Reason: the first calibration pass must stay coarse, uncertainty-aware, and `good_only`-only until more clean windows accumulate.
- Long-running capture sessions are now crash-safe at the session-artifact layer: `sample_diagnostics.jsonl` appends as samples complete, raw/normalized session partitions are flushed incrementally during the run, `summary.partial.json` is checkpointed during the run, and each session carries an explicit lifecycle state/history (`running`, `degraded`, `completed`, `failed_cleanly`, `aborted_watchdog`, `aborted_source_failure`). Reason: the next validation step is 24-hour durability, so long sessions must either complete cleanly or fail with usable partial session state instead of disappearing silently.
- The bounded capture path now includes a forward-progress watchdog, periodic heartbeat logs, explicit network-failure classification on retry exhaustion, and a partial-session summarizer CLI. Reason: long validation runs must either recover from transient DNS/URL/network failures, abort cleanly instead of wedging, or leave behind enough artifacts to judge whether the partial session is still useful.

Those are implementation shortcuts, not architectural reversals. The intended endpoint is still a source-faithful, continuously operating collection layer that preserves the oracle-anchored replay contract.

To answer it correctly, the system has to know:

- the correct **Chainlink open anchor** for the exact 5-minute window,
- the best **fast nowcast** of BTC from multiple exchanges,
- how much **time remains** in the window,
- the relevant **short-horizon volatility regime**,
- the actually **executable** Polymarket price and size,
- and whether the data at that moment was **fresh enough to trust**.

This is why the project is organized as a **research and replay engine first**, not a bot.

---

## 1. Project thesis

The system studies **lag-based mispricing** in short-dated BTC binaries.

The thesis is that different parts of the market update at different speeds:

1. exchange prices move,
2. oracle-visible state catches up or publishes on its own cadence,
3. Polymarket reprices,
4. and market participants react with varying speed and quality.

The opportunity is not “predict BTC better than everyone.”
The opportunity is:

> price the contract more correctly than the current market quote during lag windows.

That makes this a **lag trading** project.

But not all lag is the same.

### Three lag shapes

The project treats the observed trader styles as different positions on the same lag curve:

- **Early fat lag** — wider dislocations, more room for model noise, less dependence on terminal-speed racing.
- **Mid-window repricing lag** — meaningful dislocations where direction, regime, and execution quality still matter materially.
- **Terminal compression lag** — very late-window, high-confidence repricing where seconds, freshness, and execution precision dominate.

This repository is currently designed for the **early-to-mid fat-lag zone** in **5-minute BTC markets**.

That is a deliberate strategic choice.
We are **not** trying to win the most latency-sensitive end of the curve first.

---

## 2. What this project is actually modeling

The fair-value target is **not** generic BTC direction.
It is the oracle-settled event:

\[
F_t = \Pr(P^{CL}_{settle} > P^{CL}_{open} \mid \mathcal{I}_t)
\]

Where:

- `P^CL_open` is the **Chainlink opening print** for the exact 5-minute window,
- `P^CL_settle` is the **Chainlink price that determines settlement**, and
- `I_t` is the information knowable at time `t`.

This is the most important design choice in the whole project.

It means:

- the model must be **oracle-anchored**,
- the fast exchange composite is a **nowcast**, not settlement truth,
- and replay must preserve **what was knowable at the time**, not what we know afterward.

---

## 3. Strategy in plain language

The strategy is:

1. **Anchor to Chainlink** for the open and settlement definition.
2. **Read faster exchange data** to estimate where the oracle-relevant price process is likely headed now.
3. **Compute a baseline fair value** from price displacement, time remaining, and short-horizon volatility.
4. **Adjust trust** in that baseline using data-quality state.
5. **Compare fair value to executable Polymarket prices**, not to pretty midpoints or hindsight prices.
6. **Only act when the gap survives friction**: fees, slippage, model uncertainty, and stale-data risk.

The goal is not to be theoretically elegant.
The goal is to be **empirically right often enough in the slices that matter**.

---

## 4. Methodology

The methodology has four layers.

### Layer A — Baseline analytic prior

Start with a disciplined, interpretable prior:

\[
z_t = \frac{\ln(M_t / P^{CL}_{open})}{\sigma_{eff,t}\sqrt{\tau_t}}
\]

\[
F^{base}_t = \Phi(z_t)
\]

Where:

- `M_t` = fast composite nowcast from multiple exchanges,
- `P^CL_open` = Chainlink open anchor,
- `tau_t` = seconds remaining,
- `sigma_eff,t` = effective short-horizon volatility.

This is the **physics prior**.
It gives the project a stable and auditable first fair value.

### Layer B — Quality gating

A fair-value formula is useless if the inputs are stale or compromised.
So the system must track, at minimum:

- Chainlink age,
- composite age,
- venue count,
- venue dispersion,
- quote spread,
- book size,
- gap events,
- and missing-anchor state.

This layer decides whether the baseline should be trusted, degraded, or rejected.

### Layer C — Empirical calibration

The market is not a clean textbook binary.
Even a reasonable analytic prior must be corrected against replay data.

The calibrated model will answer questions like:

> When the baseline says 0.82 under this quality state, what does the market actually resolve at?

Phase-1 calibration methods are expected to be simple and interpretable:

- logistic calibration,
- isotonic regression,
- monotone correction layers.

### Layer D — Executable edge

The signal is **not**:

`contract_price - fair_value`

The real question is whether buying Up or Down has **positive net executable edge** after:

- taker fees,
- slippage,
- model buffer,
- stale-data penalty,
- and any no-trade gating.

That distinction is central.
The project is built to study **tradeable fair value**, not just academic fair value.

---

## 5. Why the data system comes before the bot

The math is comparatively ordinary.
The hard part is the evidence.

Without the right data, the project cannot answer basic questions cleanly:

- Was the fair value wrong?
- Was the Chainlink open anchor wrong?
- Was the composite stale?
- Was the market quote actually executable?
- Was a signal fake because one venue glitched?
- Did the model fail, or did the data fail?

That is why the repository is built around:

- raw feed capture,
- deterministic normalization,
- canonical window mapping,
- replay snapshot construction,
- evaluation,
- and only then calibration and policy.

This project is intentionally **data-first**.

---

## 6. Architecture overview

The architecture is organized as a research pipeline.

### 6.1 Raw ingestion

Capture source truth from:

- exchange feeds,
- Chainlink RTDS,
- Polymarket market data,
- Polymarket metadata.

Raw data is preserved before interpretation.

### 6.2 Normalization

Translate venue-specific messages into a shared grammar.

Examples:

- exchange quotes,
- exchange trades,
- Chainlink ticks,
- Polymarket quotes,
- market metadata events.

### 6.3 Window mapping

Create a canonical 5-minute `window_id` and attach:

- the market,
- the Chainlink open anchor,
- the Chainlink settle value,
- and mapping confidence/status.

### 6.4 Quality and observability

Track feed freshness, silence, gaps, reconnects, dispersion, and degraded states.

### 6.5 Replay snapshots

Build one row per timestamped market state containing only information knowable at that moment.

### 6.6 Feature engine

Compute:

- composite nowcast,
- short-horizon volatility,
- baseline fair value,
- raw and net executable edge.

### 6.7 Replay, simulation, and evaluation

Attach labels, simulate tradability, and evaluate the model by time bucket, volatility regime, and quality state.

### 6.8 Calibration and policy

Turn baseline fair value into practical fair value and define no-trade rules.

---

## 7. Canonical operating philosophy

The project is built around these rules:

- **Anchor to the oracle** — because the contract settles on Chainlink.
- **Nowcast from multiple exchanges** — because they move faster and provide leading information.
- **Trust data conditionally** — never assume freshness.
- **Replay everything** — intuition is not enough.
- **Calibrate empirically** — theory alone will not match market behavior.
- **Trade only executable edge** — visual lag is not enough.

---

## 8. What the repository will contain

The repository is structured to mirror the methodology.

```text
testingproject/
├── README.md
├── docs/
│   ├── 01_canonical_schema_spine.md
│   ├── 02_window_reference_schema.md
│   ├── 03_replay_snapshot_schema.md
│   ├── 04_raw_normalized_feed_schema.md
│   └── decisions/
│       ├── 0001_window_id_format.md
│       ├── 0002_composite_method.md
│       └── 0003_snapshot_cadence.md
├── configs/
├── src/rtds/
│   ├── core/
│   ├── schemas/
│   ├── collectors/
│   ├── normalizers/
│   ├── storage/
│   ├── mapping/
│   ├── snapshots/
│   ├── features/
│   ├── replay/
│   ├── quality/
│   └── cli/
├── data/
├── scripts/
└── tests/
```

The purpose of this layout is to keep identity, ingestion, derived features, and evaluation clearly separated.

---

## 9. Core schema and design docs

The schema pack is the project grammar.

### `docs/01_canonical_schema_spine.md`
Defines the shared entities, IDs, timestamps, units, naming rules, and invariants used everywhere.

### `docs/02_window_reference_schema.md`
Defines how a Polymarket 5-minute BTC market maps to a canonical window and how the Chainlink open/settle values are assigned.

### `docs/03_replay_snapshot_schema.md`
Defines the modeling table: one row per timestamped market state, including identity, oracle state, nowcast, executable market state, derived features, quality flags, and offline labels.

### `docs/04_raw_normalized_feed_schema.md`
Defines the raw and normalized event layers, their grains, lineage, timestamp rules, and invariants.

### ADRs in `docs/decisions/`
These freeze key architectural choices so they do not drift implicitly in code.

Current ADRs:

- `0001_window_id_format.md`
- `0002_composite_method.md`
- `0003_snapshot_cadence.md`

---

## 10. Key architectural decisions already made

### 10.1 Canonical `window_id`

The project uses a deterministic window key based on the UTC start of the 5-minute interval.

Phase-1 format:

`btc-5m-YYYYMMDDTHHMMSSZ`

Example:

`btc-5m-20260313T120500Z`

### 10.2 Composite method

Phase-1 composite nowcast policy:

- use venue mids,
- reject stale venues,
- require at least 3 valid venues,
- use robust median-family aggregation,
- carry full diagnostics for replay and audit.

This keeps the nowcast simple, robust, and testable.

### 10.3 Snapshot cadence

Phase-1 snapshot policy is hybrid:

- fixed snapshots every 1 second,
- plus event-triggered snapshots on important state changes.

This gives complete coverage without losing short-lived transitions.

---

## 11. Planned source roles

Each source has a specific role.

### Exchange feeds
Used for the **fast composite nowcast**.

Likely phase-1 venues:

- Binance
- Coinbase
- Kraken
- optionally OKX / Bybit after the base pipeline is stable

### Chainlink / RTDS
Used for the **settlement-aligned oracle monitor** and open/settle proxy.

### Polymarket market data
Used for the **executable state**:

- bids,
- asks,
- sizes,
- market lifecycle information.

### Local timing instrumentation
Used for:

- receive timestamps,
- freshness,
- latency interpretation,
- replay correctness.

---

## 12. What the replay dataset must do

The replay dataset is the core research artifact.

Each snapshot row must preserve enough state to answer:

- What did the system know at that moment?
- What was the model fair value at that moment?
- Was the market tradable at that moment?
- What happened afterward?

A proper replay row includes, at minimum:

- exact timestamp,
- exact `window_id`,
- `market_id`,
- Chainlink open anchor,
- current Chainlink proxy state,
- composite nowcast,
- seconds remaining,
- volatility features,
- executable Up/Down bids and asks,
- size and spread,
- quality flags,
- and offline labels.

Without this table, fair-value work is mostly aesthetic.
With it, the project can actually learn.

---

## 13. Immediate implementation priorities

The next milestones are intentionally narrow.

### Priority 1 — Freeze the grammar

Implement the canonical IDs, enums, time utilities, schema objects, and validation rules.

### Priority 2 — Capture raw truth

Implement collectors and raw storage for the first live sources.

### Priority 3 — Normalize deterministically

Make raw inputs reproducible, versioned, and joinable.

### Priority 4 — Build the window reference table

Correct market-to-window mapping and open-anchor assignment are among the highest-risk pieces of the system.

### Priority 5 — Build replay snapshots

Once the inputs are aligned, build the core modeling table.

### Priority 6 — Compute baseline fair value

Only after the evidence table exists should the feature engine and evaluation pipeline be pushed forward.

---

## 14. What is deliberately out of scope for now

This repository is **not** optimizing the following yet:

- live order routing,
- maker logic,
- queue-position modeling,
- advanced machine learning,
- multi-asset expansion,
- dashboard polish,
- or production bot orchestration.

Those are downstream concerns.

The bottleneck today is not sophistication.
It is **alignment, trust, and evaluation integrity**.

---

## 15. Risks the project is designed to control

### Anchor ambiguity
If the open anchor is wrong, the fair value is wrong.

### Stale composite pretending to be alpha
A fake lag is still a fake lag even if the formula looks elegant.

### Quote illusion
A backtest that uses midpoint instead of executable price can be dangerously misleading.

### Schema drift
If tables invent their own meanings for IDs, prices, or timestamps, the replay engine becomes unreliable.

The schema pack and ADRs exist largely to prevent those failures.

---

## 16. Development phases

### Phase A — Bootstrap and guardrails
Repo setup, CLI skeleton, logging, linting, type-checking, test harness.

### Phase B — Canonical schema spine
Shared grammar for IDs, timestamps, units, and invariants.

### Phase C — Raw ingestion layer
Collectors for exchange feeds, Chainlink, and Polymarket.

### Phase D — Normalization and storage
Shared normalized tables and deterministic parquet layout.

### Phase E — Window mapping and anchor assignment
Canonical window reference table.

### Phase F — Quality and observability
Freshness, dispersion, gaps, and degraded-state logic.

### Phase G — Snapshot builder
Replay snapshot table at hybrid cadence.

### Phase H — Feature engine
Composite nowcast, volatility, baseline fair value, executable edge.

### Phase I — Replay and evaluation
Label attachment, simulation, slicing, and reporting.

### Phase J — Calibration and policy
Empirical fair-value correction and deployable no-trade rules.

---

## 17. Success criteria

The project is succeeding when it can reliably answer:

1. For any moment in any tracked 5-minute BTC market, what was the correct oracle-relevant open?
2. What did the fastest trustworthy multi-exchange nowcast say at that moment?
3. How much time remained?
4. What was the volatility state?
5. What price was actually executable on Polymarket?
6. Was the data trustworthy enough to trade?
7. What happened afterward?
8. Did the apparent edge survive fees, slippage, and model uncertainty?

If the system can answer those questions cleanly, fair value becomes a tractable modeling problem.
If it cannot, even a clever formula is decorative.

---

## 18. Current status

Current status: **policy-v1, long-run capture, and execution-side validation phase**.

Implemented:

- canonical schema spine and shared core utilities
- deterministic window mapping and Chainlink anchor assignment
- normalized exchange, Polymarket, and Chainlink session-scoped datasets
- quality, composite, volatility, fair-value, and executable-edge modules
- snapshot assembly, labeling, simulation, slice analysis, and session-scoped replay
- bounded and long-run capture resilience, partial-session artifacts, and replay-admission summaries
- window-aware admission semantics `v2`
- policy-v1 replay stacks with pinned early baselines plus the later daily block sessions
- first serious policy-v1 report, stage-1 coarse `good_only` calibration, and frozen raw-vs-calibrated baseline comparison
- ADRs for window IDs, composite method, snapshot cadence, oracle source, policy v1, and stage-1 calibration

Immediate next work:

- keep the current policy frozen; the blunt stricter minimum-edge filter stays a diagnostic candidate, not a default policy change
- continue collecting clean-shadow days under the fast lane unless a milestone condition justifies a heavy checkpoint
- keep Day 5 and Day 6 shadow evidence quarantined as diagnostic specimens

---

## 19. Bottom line

This project is building a:

> **Chainlink-open-anchored, multi-exchange-nowcasted, short-horizon-vol-scaled, empirically calibrated, quality-gated, execution-adjusted fair-value engine for 5-minute BTC Polymarket markers.**

That is the clearest summary of the strategy, methodology, and architecture.

The point is not to find a magical formula.
The point is to build a system that can measure the lag correctly, test it honestly, and act only where the evidence is strong enough.
