# 04 — Raw and Normalized Feed Schema

Status: Draft  
Owners: Research / Data  
Version: 0.1.0  
Last Updated: 2026-03-14

## 1. Purpose

This document defines the raw-ingestion and normalized-feed schema for `testingproject`.

It sits directly under:

- `01_canonical_schema_spine.md`
- `02_window_reference_schema.md`
- `03_replay_snapshot_schema.md`

The role of this layer is simple:

1. preserve source truth exactly enough for replay and audit
2. translate heterogeneous venue messages into one canonical grammar
3. provide deterministic upstream inputs for window mapping, quality analysis, snapshot building, and fair-value features

The project depends on timestamp alignment, Chainlink-open anchoring, fast composite nowcasts, and executable Polymarket state. That means raw capture and normalization are not utility plumbing; they are part of the research edge.

## 2. Scope

This document defines:

- raw event layer goals and constraints
- normalized event layer goals and constraints
- canonical table grains
- required columns for phase-1 raw tables
- required columns for phase-1 normalized tables
- timestamp, lineage, and idempotency rules
- partitioning and storage conventions
- nullability and invariants
- relationships to window reference and replay snapshot layers

This document does not define:

- fair-value formulas
- window mapping policy details beyond required joins
- replay feature columns
- execution simulation rules
- long-term warehouse optimization

## 2.1 Implementation status and current deviations

This document remains the target design for the raw and normalized feed layers. The current code implements only part of that design.

Current implementation notes:

- normalized exchange quote state is implemented for Binance, Coinbase, and Kraken in `src/rtds/schemas/normalized.py` and `src/rtds/normalizers/exchange.py`
- normalized Polymarket executable quote state is implemented in `src/rtds/schemas/normalized.py` and `src/rtds/normalizers/polymarket.py`
- Polymarket metadata discovery currently includes both raw metadata capture and candidate normalization in `src/rtds/collectors/polymarket/metadata.py`
- a sanctioned phase-1 capture workflow now materializes real files under `data/raw/...` and `data/normalized/...` via `./scripts/run_collectors.sh`, either as a one-shot pass or a bounded smoke-test session
- the current capture implementation writes `market_metadata_events`, `chainlink_ticks`, `exchange_quotes`, and `polymarket_quotes` JSONL partitions directly, admits only exact BTC 5-minute Up/Down family metadata rows, and records selector diagnostics in the session summary artifact
- the canonical replay runner currently loads normalized JSONL datasets from `data/normalized/...` rather than invoking live collectors
- unified raw event schemas in `src/rtds/schemas/raw_events.py` are still a placeholder
- Chainlink normalization in `src/rtds/normalizers/chainlink.py` is still a placeholder
- the current live collection workflow is narrower than the target design: it uses bounded REST/RPC snapshot polling and Polymarket `up-or-down` tag-feed discovery instead of the eventual websocket / RTDS-first fleet
- persisted storage is implemented for `window_reference` plus the phase-1 raw and normalized JSONL partitions used by the live capture command

These are implementation gaps, not changes in architectural direction. The design intent here still governs how the raw and normalized layers should be completed.

## 3. Design principles

### 3.1 Raw means source-preserving, not source-perfect

A raw table does not need to preserve every network byte exactly as observed, but it must preserve enough information to:

- reconstruct source semantics
- re-run normalizers deterministically
- audit parser behavior
- diagnose stale feeds, reconnects, gaps, and malformed messages

As a default, persist the full decoded message payload plus collection metadata.

### 3.2 Normalized means venue-agnostic, not information-poor

Normalization should remove venue-specific naming and structural variation, but it must not erase distinctions that matter for modeling or audit.

Examples:

- `event_ts` and `recv_ts` must both survive normalization
- bid/ask state must not be compressed into a single ambiguous `price`
- source identifiers should remain available through lineage fields

### 3.3 Determinism over cleverness

Given the same raw input set and the same normalizer version, normalization must produce the same logical output.

### 3.4 Explicit grains

Every table must have one clearly stated grain. Mixed-grain tables are forbidden.

### 3.5 Lineage is mandatory

Any normalized row must be traceable to one or more raw events and to the code version that created it.

## 4. Why this layer exists

The project is trying to compute fair value for 5-minute BTC markets as:

`Pr(Chainlink settle > Chainlink open | information known at t)`

That requires, at minimum:

- a settlement-aligned Chainlink reference stream
- a fast multi-exchange nowcast stream
- executable Polymarket quote state
- exact event and receive timing
- quality diagnostics for stale, missing, or divergent inputs

Those requirements were already identified as central to the fair-value problem and to the replay dataset design. The replay table only works if the raw and normalized layers beneath it preserve those states faithfully. fileciteturn7file0 fileciteturn7file1 fileciteturn7file2

## 5. Phase-1 source coverage

Phase 1 covers the minimum sources needed for a BTC 5-minute research loop.

### 5.1 Exchange spot venues

Required phase-1 venues:

- Binance
- Coinbase
- Kraken

Optional phase-1 expansion once base pipeline is stable:

- OKX
- Bybit

These feeds support the fast composite nowcast.

### 5.2 Polymarket

Required:

- market metadata source
- market quote/order-book state feed
- trade feed if available and useful

These feeds support market mapping, executable pricing, and replay of quote state.

### 5.3 Chainlink / RTDS

Required:

- Chainlink BTC/USD stream or best publicly available RTDS proxy

Optional but useful if present:

- Binance RTDS channel for cross-checking relay quality

This feed supports open-anchor assignment, settle assignment, and live oracle-state monitoring.

## 6. Layer split

The feed layer is split into two tiers.

### Tier A — raw tables

Purpose:

- preserve source truth
- support parser re-runs
- support observability and diagnostics
- support legal/research traceability

### Tier B — normalized tables

Purpose:

- provide canonical inputs to downstream modules
- standardize time, identifiers, and field names
- enable replay and feature construction without venue-specific parsing logic

## 7. Raw layer schema

## 7.1 Raw layer common rules

All raw tables must include the following conceptual fields.

### Identity and source

- `raw_event_id`
- `venue_id`
- `source_type`
- `channel`
- `instrument_id` when inferable
- `market_id` when inferable
- `oracle_feed_id` when inferable

### Timing

- `event_ts_raw`
- `recv_ts`
- `proc_ts`

### Payload and parsing

- `raw_payload`
- `payload_format`
- `collector_session_id`
- `collector_host`
- `parser_version`
- `schema_version`

### Quality and control

- `is_control_message`
- `parse_status`
- `parse_error_code`
- `parse_error_message`

### Optional transport metadata

Use when available and worth preserving:

- `connection_id`
- `sequence_id`
- `message_type`
- `subscription_key`
- `compression_type`

## 7.2 Raw table grains

### `raw_exchange_messages`

Grain:

**one received exchange websocket or API message**

Purpose:

Preserve top-of-book, trade, ticker, or related payloads exactly enough to normalize later.

Required columns:

- `raw_event_id`
- `venue_id`
- `source_type` = `exchange_ws` or `exchange_http`
- `channel`
- `instrument_id` when derivable
- `event_ts_raw`
- `recv_ts`
- `proc_ts`
- `raw_payload`
- `payload_format`
- `collector_session_id`
- `parser_version`
- `schema_version`
- `parse_status`

Recommended optional columns:

- `sequence_id`
- `message_type`
- `connection_id`
- `subscription_key`
- `is_control_message`

Notes:

- Keep trade and quote payloads in the same raw table only if `channel` or `message_type` makes the distinction explicit.
- If a venue’s message volume or schema divergence makes a single raw table awkward, venue-specific raw tables are allowed as long as the same common columns exist.

### `raw_polymarket_messages`

Grain:

**one received Polymarket websocket or API message**

Required columns:

- `raw_event_id`
- `venue_id` = `polymarket`
- `source_type`
- `channel`
- `market_id` when derivable
- `event_ts_raw`
- `recv_ts`
- `proc_ts`
- `raw_payload`
- `payload_format`
- `collector_session_id`
- `parser_version`
- `schema_version`
- `parse_status`

Recommended optional columns:

- `condition_id`
- `token_id`
- `message_type`
- `sequence_id`
- `is_control_message`

This table may contain:

- quote or order-book deltas
- trade messages
- market state updates
- metadata refresh responses if no separate raw metadata table is used

### `raw_chainlink_messages`

Grain:

**one received Chainlink/RTDS message**

Required columns:

- `raw_event_id`
- `venue_id` = `chainlink`
- `source_type`
- `channel`
- `oracle_feed_id` when derivable
- `event_ts_raw`
- `recv_ts`
- `proc_ts`
- `raw_payload`
- `payload_format`
- `collector_session_id`
- `parser_version`
- `schema_version`
- `parse_status`

Recommended optional columns:

- `sequence_id`
- `round_id`
- `stream_id`
- `is_control_message`
- `message_type`

This table must preserve enough information to audit:

- missing or silent intervals
- source event timing vs local receipt timing
- open/settle assignment candidate events

### `raw_market_metadata_messages`

Grain:

**one received market-metadata response or event**

Required columns:

- `raw_event_id`
- `venue_id`
- `source_type`
- `channel` or `endpoint`
- `market_id` when derivable
- `recv_ts`
- `proc_ts`
- `raw_payload`
- `payload_format`
- `collector_session_id`
- `parser_version`
- `schema_version`
- `parse_status`

Recommended optional columns:

- `event_ts_raw`
- `http_status`
- `request_url`
- `etag`
- `response_version`

Use this for metadata that may affect mapping or market interpretation:

- titles
- slugs
- market start/end hints
- settlement descriptors
- asset references

## 7.3 Raw layer field semantics

### `raw_event_id`

Type: string  
Required: yes

Definition:

A unique identifier for the raw row. Prefer deterministic construction when possible, but uniqueness is more important than human readability.

Recommended:

- hash of `(venue_id, collector_session_id, recv_ts, raw_payload)`
- or a UUID generated at write time

### `source_type`

Type: enum/string  
Required: yes

Suggested values:

- `exchange_ws`
- `exchange_http`
- `market_ws`
- `market_http`
- `oracle_ws`
- `oracle_http`
- `metadata_http`

### `channel`

Type: string  
Required: yes where applicable

Definition:

Venue-native subscription or endpoint context. This is not normalized business meaning; it is the source route.

### `event_ts_raw`

Type: string or timestamp-like raw field  
Required: no

Definition:

The timestamp as provided by the source before canonical parsing. Keep this even if `event_ts` will later be parsed in normalized tables.

### `raw_payload`

Type: string/blob/json  
Required: yes

Definition:

The decoded raw message payload. If the transport payload is compressed or binary, the stored form must still preserve all information needed for later parsing.

### `parse_status`

Type: enum/string  
Required: yes

Suggested values:

- `ok`
- `partial`
- `failed`
- `ignored_control`

## 8. Normalized layer schema

## 8.1 Normalized layer common rules

All normalized tables must follow the canonical schema spine. In particular:

- UTC timestamps only
- explicit price names only
- explicit units in duration columns
- no generic `price` field
- required lineage/version fields

Every normalized row should be derivable from raw inputs and reproducible under a fixed normalizer version. This is the operational foundation for the snapshot dataset and replay workflow. fileciteturn7file1 fileciteturn7file3

## 8.2 Normalized table family

Phase-1 normalized tables are:

1. `exchange_quotes`
2. `exchange_trades`
3. `polymarket_quotes`
4. `polymarket_trades`
5. `chainlink_ticks`
6. `market_metadata_events`

Additional normalized tables may be added later, but phase-1 downstream code should depend only on this minimal set unless explicitly extended.

## 8.3 Common normalized columns

Every normalized table should include, where applicable:

### Canonical identity

- `venue_id`
- `instrument_id`
- `market_id`
- `oracle_feed_id`

### Canonical timing

- `event_ts`
- `recv_ts`
- `proc_ts`

### Lineage

- `raw_event_id`
- `normalizer_version`
- `schema_version`
- `created_ts`

### Status / diagnostics

- `normalization_status`
- `source_event_missing_ts_flag` when relevant

Not every table uses every identity column, but lineage and timing must be consistent.

## 9. Normalized table definitions

## 9.1 `exchange_quotes`

Grain:

**one normalized exchange quote observation for one venue-instrument at one event time**

Purpose:

Provide venue-level bid/ask state for composite nowcast construction and feed-quality analysis.

Required columns:

- `venue_id`
- `instrument_id`
- `asset_id`
- `event_ts`
- `recv_ts`
- `proc_ts`
- `best_bid`
- `best_ask`
- `mid_price`
- `bid_size`
- `ask_size`
- `raw_event_id`
- `normalizer_version`
- `schema_version`
- `created_ts`

Recommended optional columns:

- `quote_type`
- `quote_depth_level`
- `sequence_id`
- `source_event_missing_ts_flag`
- `crossed_market_flag`
- `locked_market_flag`

Invariants:

- `best_bid <= best_ask` unless the source itself is crossed and the row is explicitly flagged
- `mid_price = (best_bid + best_ask) / 2` when both sides exist
- `event_ts <= recv_ts` may be false in bad source data, but if so a diagnostic flag should exist or the anomaly should be observable elsewhere

Notes:

- If the source only provides one-sided updates, the normalized layer may either emit sparse rows or reconstruct state from the maintained local book; the chosen policy must be deterministic and documented in code.

## 9.2 `exchange_trades`

Grain:

**one normalized exchange trade for one venue-instrument at one trade event time**

Required columns:

- `venue_id`
- `instrument_id`
- `asset_id`
- `event_ts`
- `recv_ts`
- `proc_ts`
- `trade_price`
- `trade_size`
- `raw_event_id`
- `normalizer_version`
- `schema_version`
- `created_ts`

Recommended optional columns:

- `trade_id`
- `trade_side`
- `sequence_id`
- `aggressor_side`
- `source_event_missing_ts_flag`

Invariants:

- `trade_price > 0`
- `trade_size > 0`

Trade rows are useful for volatility estimation and diagnostics even if the phase-1 composite nowcast uses top-of-book mids.

## 9.3 `polymarket_quotes`

Grain:

**one normalized executable quote state observation for one Polymarket market at one event time**

Purpose:

Provide replayable top-of-book state for Up and Down sides.

Required columns:

- `venue_id` = `polymarket`
- `market_id`
- `asset_id`
- `event_ts`
- `recv_ts`
- `proc_ts`
- `up_bid`
- `up_ask`
- `down_bid`
- `down_ask`
- `up_bid_size_contracts`
- `up_ask_size_contracts`
- `down_bid_size_contracts`
- `down_ask_size_contracts`
- `raw_event_id`
- `normalizer_version`
- `schema_version`
- `created_ts`

Recommended optional columns:

- `token_yes_id`
- `token_no_id`
- `market_quote_type`
- `quote_sequence_id`
- `market_mid_up`
- `market_mid_down`
- `market_spread_up_abs`
- `market_spread_down_abs`
- `quote_completeness_flag`

Invariants:

- quote-side prices must be in `[0, 1]` when present
- size fields must be non-negative when present
- `up_bid <= up_ask` unless explicitly flagged as crossed
- `down_bid <= down_ask` unless explicitly flagged as crossed

Notes:

- This table should support taker-style edge estimation directly.
- Do not collapse Up and Down into a single ambiguous market price.

## 9.4 `polymarket_trades`

Grain:

**one normalized Polymarket trade event**

Required columns:

- `venue_id` = `polymarket`
- `market_id`
- `asset_id`
- `event_ts`
- `recv_ts`
- `proc_ts`
- `trade_price`
- `trade_size_contracts`
- `raw_event_id`
- `normalizer_version`
- `schema_version`
- `created_ts`

Recommended optional columns:

- `trade_id`
- `trade_side`
- `outcome_side`
- `maker_taker_flag`
- `sequence_id`

This table is optional in the sense that phase-1 fair value does not require it for core computation, but it is useful for market-state diagnostics and later execution analysis.

## 9.5 `chainlink_ticks`

Grain:

**one normalized Chainlink/RTDS price observation for one oracle feed at one event time**

Purpose:

Support:

- window open-anchor assignment
- settle assignment
- live oracle proxy state
- oracle freshness and gap analysis

Required columns:

- `venue_id` = `chainlink`
- `oracle_feed_id`
- `asset_id`
- `event_ts`
- `recv_ts`
- `proc_ts`
- `oracle_price`
- `raw_event_id`
- `normalizer_version`
- `schema_version`
- `created_ts`

Recommended optional columns:

- `oracle_event_id`
- `round_id`
- `sequence_id`
- `stream_id`
- `source_event_missing_ts_flag`
- `gap_candidate_flag`

Invariants:

- `oracle_price > 0`
- `event_ts` must be comparable in UTC to window boundaries

Notes:

- This is the canonical oracle-event table for phase 1.
- If RTDS lacks native IDs, do not fabricate oracle business IDs; use nullable lineage fields plus timestamp/row lineage.

## 9.6 `market_metadata_events`

Grain:

**one normalized market metadata observation or refresh event for one market**

Purpose:

Support:

- market discovery
- window mapping
- audit of metadata drift

Required columns:

- `venue_id`
- `market_id`
- `asset_id` when inferable
- `recv_ts`
- `proc_ts`
- `raw_event_id`
- `normalizer_version`
- `schema_version`
- `created_ts`

Recommended optional columns:

- `event_ts`
- `market_title`
- `market_slug`
- `market_status`
- `market_open_ts`
- `market_close_ts`
- `resolution_source_text`
- `condition_id`
- `token_yes_id`
- `token_no_id`

Notes:

- Metadata is allowed to be slowly changing.
- Preserve successive rows rather than overwriting history in-place.

## 10. Nullability rules

### 10.1 Raw tables

Raw rows should be permissive.

Allowed nulls include:

- `event_ts_raw`
- `instrument_id`
- `market_id`
- `oracle_feed_id`
- transport metadata fields

Required non-nulls include:

- `raw_event_id`
- `venue_id`
- `recv_ts`
- `proc_ts`
- `raw_payload`
- `parse_status`
- `schema_version`

### 10.2 Normalized tables

Normalized rows should be stricter.

A row should only be emitted into a normalized table if the minimum business fields for that table are present.

Examples:

- do not emit an `exchange_quotes` row without enough fields to compute a valid quote observation
- do not emit a `chainlink_ticks` row without a usable `oracle_price`
- do not emit a `polymarket_quotes` row if the market cannot be identified

When partial normalization is still useful, either:

- emit a row with explicit diagnostic flags, or
- route the event to an error table or observability stream

but do not silently coerce ambiguous missingness.

## 11. Deduplication and idempotency

## 11.1 Raw layer

Raw storage should be append-friendly and loss-minimizing.

Do not aggressively deduplicate raw events at ingest unless the transport clearly duplicates payloads and the dedupe rule is exact and safe.

## 11.2 Normalized layer

Normalized tables should support deterministic deduplication.

Recommended logical uniqueness keys:

- `exchange_quotes`: `(venue_id, instrument_id, event_ts, raw_event_id)`
- `exchange_trades`: `(venue_id, instrument_id, event_ts, trade_id or raw_event_id)`
- `polymarket_quotes`: `(market_id, event_ts, raw_event_id)`
- `polymarket_trades`: `(market_id, event_ts, trade_id or raw_event_id)`
- `chainlink_ticks`: `(oracle_feed_id, event_ts, oracle_event_id or raw_event_id)`
- `market_metadata_events`: `(market_id, recv_ts, raw_event_id)`

The exact physical dedupe mechanism may vary by storage engine. The logical row identity must not.

## 12. Partitioning and storage layout

The initial parquet-oriented layout should favor simple day-based retrieval.

Recommended layout:

- `data/raw/{table}/date=YYYY-MM-DD/part-*.parquet`
- `data/normalized/{table}/date=YYYY-MM-DD/part-*.parquet`

Optional secondary partitioning when needed:

- by `venue_id` for large raw tables
- by `asset_id` if multi-asset expansion arrives

Do not over-partition early. The first priority is stable retrieval and replay correctness.

## 13. Relationship to downstream schemas

## 13.1 Relationship to window reference schema

The window reference layer consumes:

- `chainlink_ticks`
- `market_metadata_events`
- optionally `polymarket_quotes` or lifecycle events for consistency checks

The normalized layer must therefore preserve:

- oracle event timing
- market identity
- metadata history

## 13.2 Relationship to replay snapshot schema

The snapshot builder consumes at minimum:

- `exchange_quotes`
- optionally `exchange_trades`
- `chainlink_ticks`
- `polymarket_quotes`
- `market_metadata_events` indirectly through window mapping

The replay schema depends on these normalized tables to provide the exact inputs already identified as necessary: Chainlink open anchor, composite now price, Chainlink current proxy, time state, executable Polymarket quote state, volatility inputs, and quality diagnostics. fileciteturn7file0 fileciteturn7file1

## 14. Required invariants

The following must hold across the feed layer.

### 14.1 Canonical naming

No normalized table may use an ambiguous `price` column.

### 14.2 UTC timestamps

All normalized timestamps must be stored in UTC.

### 14.3 Lineage

Every normalized row must be traceable to at least one `raw_event_id`.

### 14.4 Versioning

Every normalized row must carry at least:

- `schema_version`
- `normalizer_version`

### 14.5 Auditability

A researcher must be able to move from:

- a replay snapshot row
- to the normalized inputs
- to the underlying raw events

without ambiguity.

## 15. Recommended phase-1 implementation order

1. implement raw collectors and raw table writers
2. implement normalized tables for Binance, Coinbase, Kraken, Chainlink, and Polymarket
3. validate timestamp semantics with fixtures and one live collection day
4. build window mapping off `chainlink_ticks` and metadata
5. build snapshots off normalized quotes and oracle state

This ordering matches the broader project plan: freeze the schema spine, capture raw truth, normalize deterministically, map windows, then build replayable fair-value state. fileciteturn7file2 fileciteturn7file3

## 16. Out of scope for this document

The following belong elsewhere:

- specific collector implementation details
- websocket reconnect policy tuning
- volatility estimator formulas
- fair-value calibration method
- execution simulation details
- warehouse optimization for very large multi-month datasets

## 17. Summary

This schema layer exists to make the rest of the project trustworthy.

- raw tables preserve source truth
- normalized tables define one canonical grammar
- window mapping depends on normalized oracle and metadata events
- replay snapshots depend on normalized oracle, exchange, and market state

If this layer is weak, every downstream fair-value and replay result becomes suspect.
