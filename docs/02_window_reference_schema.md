# 02 — Window Reference Schema

Status: Draft  
Owners: Research / Data  
Version: 0.1.0  
Last Updated: 2026-03-13

## 1. Purpose

This document defines the canonical window reference schema for 5-minute BTC Polymarket markets that resolve against the Chainlink BTC/USD stream.

The window reference layer is the bridge between three domains:

- market identity at the execution venue
- canonical 5-minute time windows
- oracle open and settle values used for labeling and fair-value anchoring

This table is one of the highest-value datasets in the project. If it is wrong, replay labels, fair-value inputs, and evaluation slices all become unreliable.

## 2. Scope

This document defines:

- the grain of the window reference table
- required columns
- `window_id` rules
- market-to-window mapping rules
- open-anchor assignment rules
- settle assignment rules
- confidence and status fields
- invariants and fallback handling

This document does not define the replay snapshot table itself. It defines the reference data that snapshots depend on.

## 3. Grain

One row in the window reference table represents:

**one canonical 5-minute BTC resolution window, joined to at most one canonical Polymarket market for the phase-1 strategy, with the best-known open anchor and settle value metadata.**

For phase 1, assume a one-to-one operational mapping:

- one BTC 5-minute Polymarket market
- one canonical `window_id`
- one Chainlink open anchor
- one Chainlink settle value

If production reality later reveals one-to-many or many-to-one mappings, the schema may be extended, but this document defines the phase-1 canonical form.

## 4. Why this table exists

The fair-value target is not generic BTC direction. It is:

`Pr(Chainlink settle > Chainlink open | information known at t)`

So the system must know, for each market:

- which 5-minute interval it belongs to
- what the oracle-relevant open price was
- what the oracle-relevant final settle price was
- how confident we are in both assignments

This table is the authoritative source for those answers.

## 5. Core identifiers

### `window_id`

Type: string  
Required: yes  
Pattern: `<asset_id_lower>-5m-<YYYYMMDDTHHMMSSZ>`

Example:

- `btc-5m-20260313T12:05:00Z` is not allowed because colons complicate filenames
- `btc-5m-20260313T120500Z` is allowed and preferred

Definition:

The canonical UTC start timestamp of the 5-minute interval.

### `market_id`

Type: string  
Required: yes for mapped markets  
Definition:

Canonical Polymarket market identifier as defined in the core schema.

### `oracle_feed_id`

Type: string  
Required: yes  
Example:

- `chainlink:stream:BTC-USD`

## 6. Canonical columns

## 6.1 Identity block

### `window_id`
String, non-null.

### `asset_id`
String, non-null. Phase 1 fixed to `BTC`.

### `window_type`
Enum, non-null. Phase 1 fixed to `updown_5m`.

### `market_id`
String, nullable only if market discovery is incomplete. In normal phase-1 operation, non-null.

### `oracle_feed_id`
String, non-null.

### `market_title`
String, nullable.

### `market_slug`
String, nullable.

### `market_status`
Enum, non-null.

Suggested values:

- `active`
- `resolved`
- `cancelled`
- `unknown`

## 6.2 Window timing block

### `window_start_ts`
UTC timestamp, non-null.

### `window_end_ts`
UTC timestamp, non-null.

### `duration_seconds`
Integer, non-null. Must equal 300 in phase 1.

### `market_open_ts`
UTC timestamp, nullable.

Definition:

Best-known market open/listing time from Polymarket metadata or derived market lifecycle events.

### `market_close_ts`
UTC timestamp, nullable.

Definition:

Best-known market close time from metadata or lifecycle events.

## 6.3 Open anchor block

### `chainlink_open_anchor_price`
Decimal, nullable.

Definition:

The assigned Chainlink BTC/USD price used as the opening reference for the window.

### `chainlink_open_anchor_ts`
UTC timestamp, nullable.

Definition:

Timestamp of the Chainlink event selected as the open anchor.

### `chainlink_open_anchor_event_id`
String, nullable.

Definition:

Native Chainlink/RTDS event identifier or round identifier if available.

### `chainlink_open_anchor_method`
Enum, non-null.

Suggested values:

- `exact_boundary`
- `first_after_boundary`
- `last_before_boundary`
- `interpolated`
- `external_override`
- `missing`

### `chainlink_open_anchor_offset_ms`
Integer, nullable.

Definition:

Signed offset in milliseconds between `chainlink_open_anchor_ts` and `window_start_ts`.

Rules:

- positive means the chosen tick occurred after the boundary
- negative means it occurred before the boundary
- zero means exact boundary

### `chainlink_open_anchor_confidence`
Enum, non-null.

Suggested values:

- `high`
- `medium`
- `low`
- `none`

### `chainlink_open_anchor_status`
Enum, non-null.

Suggested values:

- `assigned`
- `missing`
- `ambiguous`
- `estimated`
- `overridden`

## 6.4 Settle block

### `chainlink_settle_price`
Decimal, nullable.

Definition:

The assigned Chainlink BTC/USD value used as the final resolution reference for the window.

### `chainlink_settle_ts`
UTC timestamp, nullable.

Definition:

Timestamp of the Chainlink event selected as the settle observation.

### `chainlink_settle_event_id`
String, nullable.

### `chainlink_settle_method`
Enum, non-null.

Suggested values:

- `exact_boundary`
- `first_after_boundary`
- `last_before_boundary`
- `interpolated`
- `external_override`
- `missing`

### `chainlink_settle_offset_ms`
Integer, nullable.

Definition:

Signed offset between `chainlink_settle_ts` and `window_end_ts`.

### `chainlink_settle_confidence`
Enum, non-null.

Suggested values:

- `high`
- `medium`
- `low`
- `none`

### `chainlink_settle_status`
Enum, non-null.

Suggested values:

- `assigned`
- `missing`
- `ambiguous`
- `estimated`
- `overridden`

## 6.5 Outcome block

### `resolved_up`
Boolean, nullable.

Definition:

Whether the market resolves Up under the project’s canonical interpretation of the assigned open and settle values.

Rule:

- `true` if `chainlink_settle_price > chainlink_open_anchor_price`
- `false` if `chainlink_settle_price <= chainlink_open_anchor_price`
- null only if one of the required prices is missing or the market outcome cannot be canonically assigned

### `settle_minus_open`
Decimal, nullable.

Definition:

`chainlink_settle_price - chainlink_open_anchor_price`

### `outcome_status`
Enum, non-null.

Suggested values:

- `resolved`
- `unresolved`
- `missing_anchor`
- `missing_settle`
- `ambiguous`
- `cancelled`

## 6.6 Mapping quality block

### `mapping_status`
Enum, non-null.

Suggested values:

- `mapped`
- `market_missing`
- `market_ambiguous`
- `anchor_missing`
- `settle_missing`
- `mapping_incomplete`

### `mapping_confidence`
Enum, non-null.

Suggested values:

- `high`
- `medium`
- `low`
- `none`

### `notes`
String, nullable.

Use only for operator/debug notes. Do not build critical logic from free text.

## 6.7 Lineage block

### `schema_version`
String, non-null.

### `normalizer_version`
String, nullable.

### `mapping_version`
String, non-null.

### `anchor_assignment_version`
String, non-null.

### `created_ts`
UTC timestamp, non-null.

### `updated_ts`
UTC timestamp, non-null.

## 7. Mapping rules

## 7.1 Canonical window construction

For phase 1, windows are 5-minute UTC intervals.

Construction rule:

1. floor a timestamp to the nearest UTC 5-minute boundary
2. assign that floored timestamp as `window_start_ts`
3. set `window_end_ts = window_start_ts + 300 seconds`
4. build `window_id` from the floored start timestamp

Example:

- any time from `12:05:00.000Z` inclusive to `12:09:59.999Z` inclusive maps to `btc-5m-20260313T120500Z`

## 7.2 Market-to-window mapping

The mapping process should use, in order of preference:

1. explicit market metadata that encodes the target interval
2. market title/slug parsing if explicit metadata is insufficient
3. market lifecycle timestamps as consistency checks
4. operator override only when automated mapping is impossible

A mapping is only canonical if:

- exactly one `window_id` is assigned
- the assigned interval is consistent with market metadata
- no competing market assignment exists for the same market in the same strategy scope

If these conditions fail, set `mapping_status` accordingly and do not silently force a result.

## 8. Open anchor assignment policy

## 8.1 Objective

Assign the best available Chainlink price that represents the window open boundary.

## 8.2 Candidate event search

Search Chainlink/RTDS ticks in a fixed phase-1 time band around
`window_start_ts`.

Phase-1 default search band:

- from `window_start_ts - 10 seconds`
- to `window_start_ts + 10 seconds`

Phase-1 silence-gap diagnostic band:

- if there is no tick in `window_start_ts +/- 3 seconds`, mark a
  `boundary_silence_gap` diagnostic even if a wider-band assignment still
  succeeds

## 8.3 Assignment priority

Use the first applicable rule:

1. exact boundary event at `window_start_ts`
2. earliest event strictly after `window_start_ts` and within `+10 seconds`
3. latest event strictly before `window_start_ts` and within `-10 seconds`
4. otherwise missing

If multiple ticks exist at the selected timestamp:

- if they carry different prices, mark the assignment `ambiguous`
- if they carry the same price, choose deterministically by event ID and emit a
  duplicate-tick diagnostic

## 8.4 Phase-1 recommended default

For phase 1:

- use exact boundary if available
- otherwise use first-after within tolerance
- otherwise use last-before within tolerance
- do not interpolate
- do not silently skip to a farther tick if a nearer earlier tick exists; the
  rule order is exact, then after, then before

Rationale:

- keeps assignment auditable
- avoids smoothing away real feed artifacts
- reflects the real operational challenge of sparse public RTDS updates

## 8.5 Confidence scoring guidance

Phase-1 numeric rules:

- `high`: exact boundary or assigned offset `<= 1000 ms`
- `medium`: assigned offset `> 1000 ms` and `<= 3000 ms`
- `low`: assigned offset `> 3000 ms` and `<= 10000 ms`
- `none`: missing or ambiguous

## 9. Settle assignment policy

## 9.1 Objective

Assign the best available Chainlink price that represents the end-of-window resolution boundary.

## 9.2 Candidate event search

Search Chainlink/RTDS ticks around `window_end_ts` using the same phase-1
policy as open-anchor assignment:

- search band `window_end_ts +/- 10 seconds`
- emit `boundary_silence_gap` if no tick exists in `window_end_ts +/- 3 seconds`

## 9.3 Assignment priority

Use the first applicable rule:

1. exact boundary event at `window_end_ts`
2. earliest event strictly after `window_end_ts` and within `+10 seconds`
3. latest event strictly before `window_end_ts` and within `-10 seconds`
4. otherwise missing

If multiple ticks exist at the selected timestamp:

- conflicting prices make the settle assignment `ambiguous`
- identical prices are accepted with a duplicate-tick diagnostic

Settle logic should remain separate from open logic in code even though the
rule order is the same.

## 10. Handling missing or ambiguous data

## 10.1 Missing market mapping

If no Polymarket market can be confidently mapped:

- persist the `window_id`
- set `market_id` null if necessary
- set `mapping_status = market_missing`
- set `mapping_confidence = none`

## 10.2 Ambiguous market mapping

If multiple markets could map to the same window and no deterministic rule resolves them:

- do not silently choose one
- set `mapping_status = market_ambiguous`
- retain operator-review notes if needed

## 10.3 Missing anchor

If the open anchor cannot be assigned:

- set `chainlink_open_anchor_price` null
- set `chainlink_open_anchor_status = missing`
- set `outcome_status = missing_anchor` unless an override later repairs it

## 10.4 Missing settle
n
If the settle value cannot be assigned:

- set `chainlink_settle_price` null
- set `chainlink_settle_status = missing`
- set `outcome_status = missing_settle`

## 10.5 Overrides

Any manual or external override must be explicit.

Requirements:

- set method to `external_override`
- set status to `overridden`
- preserve original computed lineage if stored elsewhere
- document override reason outside free text where practical

## 11. Recommended primary key and uniqueness rules

### Primary key

Recommended primary key for phase 1:

- `window_id`

### Uniqueness constraints

Recommended constraints:

- one row per `window_id`
- `market_id` unique within active phase-1 strategy scope
- if multiple historical versions are stored, uniqueness becomes `(window_id, mapping_version)` and a separate current-view table may be materialized

## 12. Validation rules and invariants

The following must hold for valid rows unless status fields explain otherwise.

1. `duration_seconds = 300`
2. `window_end_ts = window_start_ts + 300 seconds`
3. if `chainlink_open_anchor_price` is non-null, then `chainlink_open_anchor_status != missing`
4. if `chainlink_settle_price` is non-null, then `chainlink_settle_status != missing`
5. if both prices are non-null, then `resolved_up` must be non-null
6. if `resolved_up` is non-null, `outcome_status` should be `resolved` unless market cancellation logic overrides it
7. if `mapping_status = mapped`, then `mapping_confidence != none`

## 13. Example row

```json
{
  "window_id": "btc-5m-20260313T120500Z",
  "asset_id": "BTC",
  "window_type": "updown_5m",
  "market_id": "0xabc123",
  "oracle_feed_id": "chainlink:stream:BTC-USD",
  "market_title": "Bitcoin Up or Down - Mar 13, 12:10PM UTC",
  "market_slug": "btc-up-or-down-mar-13-1210pm-utc",
  "market_status": "resolved",
  "window_start_ts": "2026-03-13T12:05:00Z",
  "window_end_ts": "2026-03-13T12:10:00Z",
  "duration_seconds": 300,
  "market_open_ts": "2026-03-13T12:04:45Z",
  "market_close_ts": "2026-03-13T12:10:02Z",
  "chainlink_open_anchor_price": 83250.12,
  "chainlink_open_anchor_ts": "2026-03-13T12:05:00.100Z",
  "chainlink_open_anchor_event_id": null,
  "chainlink_open_anchor_method": "first_after_boundary",
  "chainlink_open_anchor_offset_ms": 100,
  "chainlink_open_anchor_confidence": "medium",
  "chainlink_open_anchor_status": "assigned",
  "chainlink_settle_price": 83302.44,
  "chainlink_settle_ts": "2026-03-13T12:10:00.090Z",
  "chainlink_settle_event_id": null,
  "chainlink_settle_method": "first_after_boundary",
  "chainlink_settle_offset_ms": 90,
  "chainlink_settle_confidence": "medium",
  "chainlink_settle_status": "assigned",
  "resolved_up": true,
  "settle_minus_open": 52.32,
  "outcome_status": "resolved",
  "mapping_status": "mapped",
  "mapping_confidence": "high",
  "notes": null,
  "schema_version": "0.1.0",
  "normalizer_version": "0.1.0",
  "mapping_version": "0.1.0",
  "anchor_assignment_version": "0.1.0",
  "created_ts": "2026-03-13T12:10:05Z",
  "updated_ts": "2026-03-13T12:10:05Z"
}
```

## 14. Operational guidance

This table should be built before replay snapshots.

Why:

- snapshots need the correct `window_id`
- snapshots need the open anchor
- labels need the settle value
- evaluation needs mapping confidence and status

This table should be treated as authoritative reference data, not a disposable intermediate.

## 15. Summary

The window reference schema gives the system a single source of truth for:

- what each 5-minute market refers to
- when the window starts and ends
- what oracle value anchors the open
- what oracle value determines the final label
- how trustworthy those assignments are

The next schema document should build on this by defining the replay snapshot table that materializes the knowable state of each market/window over time.
