# Current Capabilities And Validation Boundary

This document describes `5minPATH` as a tool: what it can do today, what has actually been validated, what has not, and where to adapt it for new experiments.

## What The Tool Is Good At

The strongest out-of-the-box fit is:

- BTC 5-minute Polymarket markets
- recurring oracle-anchored market families with comparable structure
- research capture, replay, calibration, and live-forward shadow measurement

What the tool can do today:

- run bounded capture sessions
- write raw and normalized JSONL datasets
- produce admission and session-summary artifacts
- rebuild replay state from captured sessions
- compare raw and calibrated baseline behavior
- run live-forward shadow measurement against capture output
- generate artifact-first reports for diagnostics and research

## What Has Actually Been Validated

The evidence supports all of the following:

- repeated bounded capture sessions, including many day-scale runs
- repeated daily use with restart between sessions
- deterministic replay and calibration workflows over captured sessions
- live-forward shadow measurement over clean comparison days
- crash-safe partial-session artifacts and cleaner failure handling than the earliest run set

The evidence does **not** support all of the following:

- one single uninterrupted multi-day daemon process
- indefinitely running collectors with no restart
- long-lived memory / stale-state behavior over week-scale uptime
- authenticated trading or production execution

So the operational boundary is:

> validated for repeated bounded day-scale sessions, not for one indefinitely running multi-day process.

## Where Longer Continuous Runs May Still Break

If a user stretches the current tool beyond the validated boundary, the highest-risk areas are:

- cross-midnight partition rollover
- long-lived in-memory state drift
- file-tail / offset tracking across many rollovers
- repeated retry / circuit-breaker recovery over very long uptime
- selector rebinding across many market turnovers
- shadow sidecar shutdown / idle-tail behavior
- disk growth and artifact-rotation assumptions

That does not mean those paths are broken. It means they are not yet validated to the same standard as the bounded session workflow.

## What The Tool Does Not Try To Be

This repo is not:

- a generic any-market Polymarket toolkit
- a hosted application
- a dashboard product
- an authenticated trading system
- a deployment-ready policy package

## What A User Gets

Users primarily get files and reports:

- raw datasets under `data/raw/...`
- normalized datasets under `data/normalized/...`
- capture, replay, calibration, and shadow artifacts under `artifacts/...`

Those outputs are intended to feed:

- direct artifact inspection
- notebooks
- downstream scripts
- custom dashboards
- further market-specific research

## Where To Customize

If you want to adapt the tool for your own experiments, the main starting points are:

- `configs/replay/`
  - replay policy configs
  - calibration configs
  - classifier and comparison manifests
- `scripts/`
  - capture and post-run wrappers
  - analysis entrypoints
- `src/rtds/cli/`
  - replay, calibration, comparison, and analysis commands
- `src/rtds/`
  - capture, normalization, replay, and execution modules

Practical rule:

- changing policy, calibration, market binding, or gating means you are running a new experiment, not reproducing the currently documented evidence set

## Best Starting Docs

- [README](../README.md)
- [Capture runbook](05_capture_runbook.md)
- [Architecture overview](architecture_overview.md)
- [Market thesis](market_thesis.md)
- [Execution v0 shadow boundary](decisions/0008_execution_v0_shadow_boundary.md)
- [Closeout decision](decisions/0009_phase1_closeout.md)
