# 0004 Oracle Source For Boundary Capture

## Status

Accepted for phase 1.

## Context

The original project design is Chainlink-open anchored and Chainlink-settle labeled. Phase-4 dense pilot scheduling proved that capture cadence was no longer the blocker: even with 1-second polling and boundary burst mode, the existing `latestRoundData` collector still produced too few distinct oracle `event_ts` values near 5-minute boundaries to satisfy the current anchor assignment policy.

That meant the bounded collector was operationally healthy but still not replay-admissible. The failure was not scheduling, resilience, or family continuity. It was oracle-source suitability for boundary assignment.

## Decision

For the current phase-1 capture path:

- prefer the public Chainlink Data Streams BTC/USD page-backed API as the oracle source
- preserve oracle-source lineage explicitly in normalized `chainlink_ticks`
- persist the assigned open and settle oracle source in `window_reference`
- keep the legacy `latestRoundData` RPC path as a fallback source, not as the preferred source

Current source identities:

- `chainlink_stream_public_delayed`
- `chainlink_snapshot_rpc`

## Why

- The public Data Streams page exposes BTC/USD stream reports at boundary-usable cadence through the same official Chainlink product family the target architecture already points toward.
- The legacy `latestRoundData` path is still useful as an availability fallback, but it is heartbeat/deviation driven and empirically failed the current boundary policy.
- Capturing explicit source lineage keeps the repo honest if a session mixes stream-backed and snapshot-backed oracle observations.

## Deviation From Target Architecture

This is not the final oracle architecture.

The target remains a source-faithful, continuously running Chainlink RTDS/Data Streams collector. The current implementation uses the public delayed Data Streams endpoint because it is officially exposed and materially improves boundary usability without introducing a non-Chainlink proxy source.

So the current phase-1 deviation is:

- public delayed Chainlink Data Streams instead of commercial real-time RTDS/Data Streams access
- `latestRoundData` fallback retained for continuity when the public stream endpoint is unavailable

## Consequences

Positive:

- anchor assignment can now be validated against a boundary-usable Chainlink stream source
- the admission pipeline can distinguish source quality from scheduler quality
- future runs can report which oracle source actually supported anchor assignment

Negative:

- the public Data Streams endpoint is delayed and therefore is not a production live-trading oracle feed
- the current collector still falls short of the final real-time architecture

## Revisit

Revisit this decision when a real-time RTDS/Data Streams access path is available, or if replay admission still fails after the stream-backed boundary validation.
