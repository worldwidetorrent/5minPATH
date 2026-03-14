# ADR 0003 — Snapshot Cadence

Status: Accepted  
Date: 2026-03-13  
Owners: Research / Data

## Context

The replay snapshot table is the core modeling dataset: one row per timestamped market/window state, containing only information knowable at that moment. To build this table, the system must decide **when** snapshots are emitted.

The cadence must balance:

- complete coverage of 5-minute market evolution
- preservation of important edge moments
- computational tractability
- deterministic replay
- auditability of why a snapshot exists

A pure event-driven approach risks inconsistent coverage across quiet versus busy periods. A pure fixed-interval approach risks missing important state transitions between ticks. This ADR fixes the phase-1 cadence policy.

## Decision

Phase-1 snapshot generation uses a **hybrid cadence**:

1. **fixed-interval snapshots every 1 second**, plus
2. **event-triggered snapshots** when important state changes occur between fixed intervals

Both snapshot types are stored in the same canonical snapshot schema, with a field that records snapshot origin.

## Fixed cadence policy

A fixed snapshot is emitted at every whole UTC second within an active window.

For a 5-minute window, this yields up to 301 boundary-inclusive second marks depending on implementation convention.

Recommended phase-1 convention:

- emit snapshots for `snapshot_ts` in `[window_start_ts, window_end_ts)` at one-second spacing
- do not emit a final post-close fixed snapshot by default
- handle labels and settlement in downstream tables rather than by adding a synthetic terminal fixed row

## Event-triggered policy

In addition to fixed snapshots, emit a snapshot immediately when any configured trigger occurs and the trigger timestamp is materially distinct from the most recent emitted snapshot.

### Phase-1 triggers

1. **Polymarket best quote change**
   - up bid changes
   - up ask changes
   - down bid changes
   - down ask changes

2. **Chainlink update**
   - a new Chainlink/RTDS tick arrives

3. **Composite move threshold**
   - absolute composite move since last snapshot exceeds configured threshold
   - threshold must be configurable in USD and/or basis points

4. **Quality-state transition**
   - freshness flag changes
   - contributing venue count crosses threshold
   - dispersion regime changes into or out of degraded state
   - chainlink gap flag changes

### Optional later triggers

Deferred until needed:

- large book-size change
- spread regime change beyond configurable band
- trade print arrival if trade feed adds information beyond quote changes

## Snapshot origin field

Each snapshot row must record how it was created.

Required field:

- `snapshot_origin`

Allowed phase-1 values:

- `fixed_1s`
- `event_polymarket_quote`
- `event_chainlink_tick`
- `event_composite_move`
- `event_quality_transition`

If multiple triggers fire at the same timestamp, one of two approaches is allowed:

1. store a primary origin plus `trigger_flags`, or
2. store a canonical origin priority order plus booleans for all triggers

Phase-1 recommendation:

- use `snapshot_origin` as the primary initiator
- also store trigger booleans where useful

## Deduplication rules

The system must avoid emitting semantically duplicate snapshots.

### Deduplication policy

If an event-triggered snapshot and a fixed snapshot occur at the same effective `snapshot_ts`, emit only one row unless configuration explicitly requests both.

Priority order for phase 1:

1. event-triggered snapshot wins over fixed snapshot at identical `snapshot_ts`
2. multiple event triggers at identical `snapshot_ts` collapse into one row with multiple trigger flags

## Why 1 second fixed cadence

A 5-minute market is short enough that one-second cadence is:

- dense enough for high-quality replay and calibration
- sparse enough to remain tractable in phase 1
- naturally aligned with reported Chainlink cadence expectations and exchange quote dynamics without attempting full tick-level materialization

## Why not tick-only snapshots

Rejected because:

- quiet periods would be underrepresented
- derived features become more difficult to compare across time
- evaluation by remaining time becomes noisier
- replay output depends too strongly on source message patterns

## Why not fixed-only snapshots

Rejected because:

- important quote and quality transitions can occur between fixed ticks
- lag opportunities may be short-lived and deserve explicit capture
- quality degradation moments are themselves research-relevant

## Consequences

### Positive

- complete temporal coverage of every active window
- explicit preservation of key state changes
- deterministic replay slices by second remaining
- good balance between coverage and storage cost

### Negative

- more storage and processing than a pure fixed or pure event model
- builder logic is slightly more complex because deduplication and trigger handling are required

This is acceptable.

## Implementation requirements

- `src/rtds/snapshots/builder.py` and `assembler.py` must implement the cadence.
- thresholds for composite move and quality transitions must live in config.
- snapshot rows must be reproducible from normalized events and config.
- all cadence behavior must be versioned.

## Acceptance tests

Tests must verify:

1. one fixed snapshot per second in an active window under quiet conditions
2. quote-change events create snapshots between fixed ticks
3. chainlink updates create snapshots between fixed ticks
4. quality-state transitions create snapshots
5. duplicate triggers at same timestamp collapse deterministically
6. identical replay inputs produce identical snapshot timestamps and origins

## Future extension path

Possible later refinements:

- adaptive cadence near expiry
- denser fixed cadence in the final 30 seconds
- sparse cadence in early quiet regions
- market-state-sensitive trigger thresholds

These are explicitly deferred until baseline replay quality is proven.
