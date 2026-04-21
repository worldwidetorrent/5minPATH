# 5minPATH

`5minPATH` is a data-first research system for capturing, replaying, calibrating, and live-shadow-evaluating **5-minute BTC Polymarket markets that resolve on Chainlink**.

The repo was built to answer one narrow question:

> At any moment inside a 5-minute BTC market, what was the best oracle-relevant fair value that was actually knowable at that time?

## Status

**Usable research and measurement tool**

- Outcome: validated measurement engine
- Key result: calibration was consistently useful in replay; live edge survival was real but inconsistent
- Main live drags: availability first, directional disagreement second, fill loss minor
- Deployment: **not recommended** from the current evidence set
- Best fit: BTC 5-minute oracle-anchored markets
- Reuse model: capture/replay/shadow pipeline is reusable; market binding and modeling assumptions are configurable
- Operational boundary: validated for repeated bounded day-scale sessions, not for one indefinite multi-day daemon process

Project history and evidence:
- [Closeout decision](docs/decisions/0009_phase1_closeout.md)
- [Evidence index](docs/baselines/20260421_phase1_evidence_index.md)

## What The Repo Does Today

Out of the box, the repo can:

- run bounded capture sessions for the BTC 5-minute market family
- write raw and normalized JSONL datasets
- produce session summaries and admission artifacts
- rebuild replay state from captured sessions
- compare raw versus calibrated baseline behavior
- run live-forward shadow measurement against capture output
- generate artifact-first reports for capture, replay, calibration, and execution diagnostics

The strongest out-of-the-box fit is:

- recurring, oracle-anchored Polymarket market families
- especially the current BTC 5-minute family already wired into the repo

## What This Repo Does Not Do

This repo is **not**:

- a generic “point it at any Polymarket market and it just works” toolkit
- an authenticated trading system
- a production execution stack
- a polished dashboard product
- a guaranteed profitable or deployment-ready policy repo

The infrastructure is reusable. The current market binding, oracle anchoring, fair-value logic, and policy/calibration assumptions are still market-specific.

## What A New User Can Actually Do

For the BTC 5-minute family, a new user can clone the repo and:

- capture real market/oracle/quote data
- inspect raw and normalized files under `data/...`
- run replay and calibration analysis over completed sessions
- run live-forward shadow analysis for execution realism
- export the normalized data and artifacts into their own notebooks, scripts, or dashboards

What the repo gives them is primarily **files and reports**, not a hosted application:

- raw capture data under `data/raw/...`
- normalized datasets under `data/normalized/...`
- session, replay, calibration, and shadow artifacts under `artifacts/...`

## Where To Make Your Own Tweaks

If you want to adapt the tool for your own experiments, the main places to start are:

- `configs/replay/`
  - replay policy configs
  - calibration configs
  - classifier and comparison manifests
- `scripts/`
  - operational wrappers
  - analysis entrypoints
- `src/rtds/cli/`
  - replay, calibration, comparison, and analysis CLIs
- `src/rtds/`
  - core capture, normalization, replay, and execution modules

Practical rule:

- changes to policy, calibration, market binding, or gating should be treated as **new experiments**, not as equivalent to the current documented evidence set

## Quickstart

Most useful commands:

1. Bounded smoke capture:

```bash
./scripts/run_collectors.sh --capture-mode smoke --duration-seconds 600
```

2. Fast-lane closeout for a completed session:

```bash
./scripts/run_day_fast_lane.sh YYYY-MM-DD <session-id>
```

3. Optimized post-run closeout:

```bash
./scripts/run_day_optimized_postrun.sh YYYY-MM-DD <session-id>
```

For capture modes, operational notes, artifact paths, and longer examples:
- [Capture runbook](docs/05_capture_runbook.md)
- [Current capabilities and validation boundary](docs/current_capabilities.md)

## Current Capabilities

Implemented on `main`:

- canonical core IDs, enums, time and validation utilities
- deterministic market-to-window mapping and Chainlink anchor assignment
- raw and normalized capture for Chainlink, exchanges, Polymarket quotes, and market metadata
- quality, composite, volatility, baseline fair value, and executable edge modules
- replay snapshot assembly, labeling, taker-only replay simulation, and slice analysis
- policy-v1 replay stacks and stage-1 `good_only` calibration
- execution-v0 shadow sidecar with production-safe capture-output `live_state`
- append-only shadow decisions, order-state transitions, outcomes, and replay comparison artifacts
- fast-lane and checkpoint-lane daily analysis workflows

Not implemented end to end:

- authenticated execution / live order routing
- websocket-first production market-data fleet
- a dedicated `build_snapshots` CLI
- generic any-market Polymarket support without market-specific adaptation

## Current Research Takeaways

What the current evidence set establishes:

- there is real predictive structure in this market family under the repo’s oracle-anchored framing
- calibration is consistently useful in replay
- some modeled edge survives live-forward conditions on certain clean days
- the current technique does **not** convert that edge consistently enough for deployment

What the current evidence set rejects:

- “fills are the main problem”
- a blanket stricter minimum-edge rule
- a simple wide-delta exclusion rule
- a deployment recommendation

The strongest conditional finding so far is:

- wide live-vs-replay calibrated fair-value divergence is a real failure state
- the strongest second condition found was wide delta combined with replay up-side buckets (`lean_up` / `far_up`)

That was enough to validate the measurement engine, but not enough to justify a policy change.

## Repo Layout

Key paths:

- `data/raw/` — captured raw source data
- `data/normalized/` — normalized research inputs
- `artifacts/` — capture, replay, calibration, and shadow outputs
- `scripts/` — operational wrappers and analysis entrypoints
- `src/rtds/` — core library and CLI implementation
- `docs/` — schema docs, decisions, runbooks, baselines, and closeout notes

## Where To Read Next

Best starting docs:

- [Current capabilities and validation boundary](docs/current_capabilities.md)
- [Closeout decision](docs/decisions/0009_phase1_closeout.md)
- [Evidence index](docs/baselines/20260421_phase1_evidence_index.md)
- [Capture runbook](docs/05_capture_runbook.md)
- [Architecture overview](docs/architecture_overview.md)
- [Market thesis](docs/market_thesis.md)
- [Execution-v0 shadow boundary](docs/decisions/0008_execution_v0_shadow_boundary.md)
- [Stage-1 calibration decision](docs/decisions/0006_stage1_good_only_calibration.md)

Schema and design docs:

- [Canonical schema spine](docs/01_canonical_schema_spine.md)
- [Window reference schema](docs/02_window_reference_schema.md)
- [Replay snapshot schema](docs/03_replay_snapshot_schema.md)
- [Raw and normalized feed schema](docs/04_raw_normalized_feed_schema.md)

## Bottom Line

This repo is a **usable research and measurement engine**.

It successfully validated:

- durable capture
- deterministic replay and calibration
- live-forward shadow measurement

It did **not** validate:

- a deployment-ready policy
- a production trading system

That is still a valuable result, and the repo should be read in that frame.
