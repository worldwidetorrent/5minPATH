# ADR 0001 — Window ID Format

Status: Accepted  
Date: 2026-03-13  
Owners: Research / Data / Platform

## Context

The project models 5-minute BTC Polymarket markets that resolve against a Chainlink BTC/USD stream. Multiple downstream tables must refer to the same canonical resolution interval:

- window reference rows
- replay snapshot rows
- labels and outcomes
- execution records
- observability summaries

A stable, human-readable, filename-safe window identifier is required to keep joins deterministic across these layers.

The canonical schema spine defines `window_id` as a first-class identifier and the window reference schema requires exactly one canonical `window_id` per 5-minute interval. This ADR fixes the concrete format and construction rules.

## Decision

The canonical `window_id` format is:

`<asset_id_lower>-5m-<YYYYMMDDTHHMMSSZ>`

For phase 1, examples are:

- `btc-5m-20260313T120000Z`
- `btc-5m-20260313T120500Z`
- `btc-5m-20260313T121000Z`

The timestamp component is the **UTC window start timestamp**.

## Construction rules

1. `asset_id_lower` is the lowercase canonical asset identifier from the core schema.
2. The interval token is `5m` for phase 1.
3. The datetime token is UTC in compact ISO-like form:
   - `YYYYMMDD`
   - literal `T`
   - `HHMMSS`
   - literal `Z`
4. `window_start_ts` must be aligned to an exact 5-minute boundary.
5. For phase 1, `SS` must always be `00`.
6. No separators beyond hyphens are permitted.
7. Colons are forbidden.
8. Local timezones are forbidden.

## Why this format

This format is chosen because it is:

- deterministic
- sortable lexicographically
- readable in logs and filenames
- safe for parquet paths and shell tooling
- easy to derive from UTC timestamps without locale ambiguity

## Non-goals

The `window_id` does **not** encode:

- Polymarket market ID
- oracle round ID
- settlement direction
- confidence or mapping status

Those belong in separate columns.

## Invariants

The following invariants must hold:

1. `window_id` uniquely identifies one and only one canonical 5-minute UTC interval for one asset.
2. Two rows with the same `window_id` must share the same:
   - `asset_id`
   - `window_start_ts`
   - `window_end_ts`
   - `duration_seconds`
3. `window_end_ts = window_start_ts + 300 seconds` in phase 1.
4. `window_start_ts` must satisfy `minute % 5 == 0`, `second == 0`, and `microsecond == 0`.
5. `window_id` is derived from `window_start_ts`, not vice versa in ambiguous cases.

## Canonical parsing example

Given:

- `asset_id = BTC`
- `window_start_ts = 2026-03-13T12:15:00Z`

Then:

- `window_id = btc-5m-20260313T121500Z`

## Allowed derivations

A system may derive `window_id` from:

- a canonical `window_start_ts`
- a market metadata interval already validated against UTC boundaries
- a snapshot timestamp floored to the applicable 5-minute boundary

A system must **not** derive `window_id` from:

- user locale time
- display-formatted market text alone
- implicit browser timezone
- ambiguous relative dates

## Alternatives considered

### Alternative A — Use raw start timestamp only

Example:

`2026-03-13T12:15:00Z`

Rejected because:

- asset and interval are not encoded
- harder to distinguish if multiple strategies share time keys
- more fragile in filenames because of punctuation

### Alternative B — Integer epoch key

Example:

`1741877700`

Rejected because:

- not self-describing
- poor readability in logs and docs
- easy to misuse as event time instead of interval identity

### Alternative C — Include market ID in the window ID

Example:

`btc-5m-20260313T121500Z-pmkt-12345`

Rejected because:

- mixes interval identity with market identity
- breaks separation of identity from mapping
- prevents one window from being referenced independently of a specific market

## Consequences

### Positive

- cross-table joins become straightforward
- debugging is easier because IDs are interpretable by eye
- file and partition naming remains shell-safe
- market/window mapping remains one explicit relation rather than an overloaded key

### Negative

- future support for non-5-minute intervals will require either:
  - extending the interval token convention, or
  - defining separate window types cleanly

This is acceptable.

## Implementation requirements

- `src/rtds/mapping/window_ids.py` must be the authoritative code path for formatting and parsing.
- `tests/unit/test_window_ids.py` must include:
  - valid examples
  - invalid examples
  - round-trip format/parse tests
  - boundary alignment tests
- all downstream schemas must store both:
  - `window_id`
  - `window_start_ts`

## Validation rules

A `window_id` must be rejected if:

- asset token is unknown
- interval token is not supported
- timestamp token is malformed
- parsed timestamp is not UTC-aligned
- parsed timestamp is not on a 5-minute boundary

## Follow-on work

This ADR enables:

- window reference table implementation
- anchor assignment keyed by interval
- snapshot builder joins
- replay partitioning and day slicing
