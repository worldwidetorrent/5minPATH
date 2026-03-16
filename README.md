# testingproject

A data-first research system for **pricing, replaying, and evaluating 5-minute BTC Polymarket markers that resolve on Chainlink**.

The project is built around one core idea:

> the edge is not exotic math; the edge is **timestamped alignment**.

This repository exists to answer a very specific question with high discipline:

> At any moment inside a 5-minute BTC market, what is the best estimate of the contract's **oracle-relevant fair value**, given the information that was actually knowable at that time?

That question sounds small. It is not.

## Current status

The architecture described in this README is still the target design. The codebase now implements the core research spine, one canonical replay-day execution path, and a sanctioned phase-1 capture command that materializes real raw and normalized files. It still does not implement the full long-running live collection stack described later in this document.

Implemented today:

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

Not yet implemented end to end:

- dedicated `build_snapshots` CLI is still a placeholder
- raw event schemas are still conceptual rather than fully implemented in code
- execution/fill schemas are still a placeholder
- the full intended streaming collector fleet is not implemented; the current capture path is a bounded public-endpoint snapshot session rather than a continuously running service
- most downstream admission and replay logic still expects curated day partitions rather than a continuously running ingestion service

When the code is narrower than the design described below, the design should be read as the intended architecture and the narrower implementation as the current phase-1 state.

### Current phase-1 deviation from target design

The original architecture still matters and remains the north star. The current capture implementation deviates from it in a few deliberate ways:

- It uses a bounded orchestration session instead of long-running collectors. Reason: the immediate success metric is to prove the repo can land coherent real files in a controlled smoke-test window before expanding to full-day operations.
- It uses public REST snapshots for Binance.US, Coinbase, Kraken, and Polymarket, and now prefers the public delayed Chainlink Data Streams BTC/USD endpoint with `latestRoundData` only as fallback. Reason: the public stream endpoint is the first official Chainlink source in this repo that is actually boundary-usable under the current anchor policy, while the old RPC snapshot path remains useful for continuity when the stream endpoint is unavailable.
- Polymarket metadata discovery currently pulls from the `up-or-down` event feed rather than the broader market search surface. Reason: that feed exposes the recurring BTC 5-minute family densely enough to admit the exact target strip without scanning thousands of unrelated events first.
- The live selector now admits only exact BTC 5-minute family candidates and binds them to canonical `window_id`s before quote capture. Reason: the repo’s canonical grammar treats `window_id` as primary and market binding as a downstream step, so live capture now follows that same contract.
- During bounded sessions, quote capture can roll between admitted family members as the live 5-minute window advances. Reason: the target family is recurring, so a 10-minute smoke run must stay on the same family while moving from one canonical window to the next.
- The sanctioned collector now has explicit bounded capture densities: smoke mode can stay coarse, while pilot/admission mode runs 1-second Chainlink, exchange, and Polymarket quote polling with slower metadata refresh plus optional 5-minute boundary burst scheduling. The denser presets also widen sample-based failure thresholds so a 1-second pilot does not inherit 60-second smoke tolerances verbatim. Reason: replay admission depends on boundary-local anchor and quote coverage, not just process survival.
- The bounded capture path now treats transient fetch failures and empty Polymarket books as degraded operational states instead of process-killing exceptions, and it reclassifies Polymarket 404s with selector refresh plus rollover-grace handling before declaring the market binding invalid. Reason: rollover-safe collection depends on distinguishing temporary quote unavailability from a truly stale market binding.
- Each bounded capture session now writes an `admission_summary.json` artifact beside `summary.json`. Reason: pilot-length reruns now need replay-usable continuity diagnostics, not just process-survival logs. The admission rollup now treats final per-sample selected market/window bindings as the source of truth for family continuity and reports metadata-strip breadth or ambiguity separately so refresh rows do not masquerade as off-family drift. Because `build_snapshots` is still a placeholder, `snapshot_eligible_sample_count` is currently a conservative capture-side proxy based on family compliance, selected-window mapping, anchor confidence, and per-sample source completeness.
- Oracle lineage is now explicit in normalized `chainlink_ticks` and `window_reference` assignment outputs. Reason: the repo now distinguishes `chainlink_stream_public_delayed` from `chainlink_snapshot_rpc` so boundary validation can be judged on the actual oracle source used, not on an implied generic "Chainlink" label. See `docs/decisions/0004_oracle_source_boundary_capture.md`.
- The public-stream boundary-validation case is now pinned by admission-summary regression coverage, including zero off-family drift, explicit `chainlink_stream_public_delayed` lineage, nonzero anchor confidence across a midnight-spanning window set, and nonzero snapshot eligibility. Reason: the oracle-path/midnight-rollup combination is now a fixed acceptance baseline, not an anecdotal one-off run.

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

Current status: **research-spine and replay-runner phase**.

Implemented:

- canonical schema spine and shared core utilities
- deterministic window mapping and Chainlink anchor assignment
- persisted `window_reference` dataset writes
- normalized exchange and Polymarket quote state
- quality, composite, volatility, fair-value, and executable-edge modules
- snapshot assembly, labeling, simulation, and slice analysis
- canonical `replay_day` runner and replay artifact contract
- ADRs for window IDs, composite method, and snapshot cadence

Immediate next work:

- implement the remaining live collection and raw-normalization path,
- replace placeholder Chainlink normalization,
- persist more replay-facing datasets beyond `window_reference`,
- run one real replay day and review the first research report,
- define the first explicit deploy/no-trade policy.

---

## 19. Bottom line

This project is building a:

> **Chainlink-open-anchored, multi-exchange-nowcasted, short-horizon-vol-scaled, empirically calibrated, quality-gated, execution-adjusted fair-value engine for 5-minute BTC Polymarket markers.**

That is the clearest summary of the strategy, methodology, and architecture.

The point is not to find a magical formula.
The point is to build a system that can measure the lag correctly, test it honestly, and act only where the evidence is strong enough.
