# Architecture Overview

This document gives the short-form architecture view for `5minPATH` as it exists in the current documented evidence set.

The repo is best understood as five stacked layers.

## 1. Capture and normalization

Purpose:

- collect raw source truth
- normalize it into deterministic research inputs

Primary sources:

- Chainlink BTC/USD
- Binance
- Coinbase
- Kraken
- Polymarket metadata
- Polymarket executable quotes

Primary outputs:

- `data/raw/...`
- `data/normalized/...`

Reference docs:

- [Capture runbook](05_capture_runbook.md)
- [Raw and normalized feed schema](04_raw_normalized_feed_schema.md)

## 2. Session gating and admission

Purpose:

- decide whether a captured session is structurally usable
- classify degraded windows instead of collapsing all imperfect sessions

Key ideas:

- admission semantics `v2`
- family continuity
- snapshot eligibility
- per-window quality classification

Reference docs:

- [Policy v1 and admission v2 decision](decisions/0005_policy_v1_and_admission_v2.md)

## 3. Replay state assembly

Purpose:

- reconstruct only what was knowable at each snapshot timestamp
- join reference, market, timing, and quality state into replayable rows

Primary outputs:

- replay artifacts under `artifacts/replay/...`

Reference docs:

- [Canonical schema spine](01_canonical_schema_spine.md)
- [Window reference schema](02_window_reference_schema.md)
- [Replay snapshot schema](03_replay_snapshot_schema.md)

## 4. Fair value, policy, and calibration

Purpose:

- compute baseline fair value
- replay policy behavior
- compare raw versus calibrated economics

Current implemented state:

- policy `v1`
- stage-1 `good_only` calibration
- fast-lane and checkpoint analysis workflow

Reference docs:

- [Stage-1 good-only calibration decision](decisions/0006_stage1_good_only_calibration.md)
- [Evidence index](baselines/20260421_phase1_evidence_index.md)

## 5. Live-forward shadow execution measurement

Purpose:

- test whether the modeled edge was actually there at decision time
- separate runtime safety from execution economics

Current boundary:

- execution `v0`
- shadow sidecar only
- fail-open relative to capture
- no authenticated trading

Primary outputs:

- `artifacts/shadow/<session_id>/...`

Reference docs:

- [Execution v0 shadow boundary](decisions/0008_execution_v0_shadow_boundary.md)

## Repo-level summary

The repo is not just a collector and not just a model.

It is a research and measurement system that:

1. captures real market state
2. rebuilds knowable-at-time replay rows
3. evaluates fair value and policy behavior
4. measures live-forward execution realism with a shadow sidecar

That stack is what the documented evidence set validated, even though it did not justify deployment.
