# 03 — Replay Snapshot Schema

Status: Draft  
Owners: Research / Data  
Version: 0.1.0  
Last Updated: 2026-03-14

## 1. Purpose

This document defines the replay snapshot schema: the modeling table used to compute, replay, calibrate, and evaluate fair value for 5-minute BTC Polymarket markets tied to the Chainlink BTC/USD stream.

This is not the whole project schema. This is the research-facing state table that sits on top of the canonical schema spine and the window reference layer.

A replay snapshot must represent only information that was knowable at the snapshot timestamp. That is the central design constraint.

## 1.1 Implementation status and current deviations

This document remains the target design for the replay snapshot table. The current code implements a phase-1 snapshot schema and replay stack, but not the full wide-table design described below.

Current implementation notes:

- the persisted snapshot row is `SnapshotRecord` in `src/rtds/schemas/snapshot.py`
- current `SnapshotRecord` covers identity, window/reference fields, current Chainlink state, composite state, Polymarket executable quote state, and snapshot-level quality flags
- the canonical replay runner is `src/rtds/cli/replay_day.py`; it writes deterministic run artifacts under `artifacts/replay/YYYY-MM-DD/run_<timestamp>/`
- current replay artifacts are JSONL, CSV, JSON, and Markdown rather than parquet
- offline truth is attached later by `src/rtds/replay/attach_labels.py`; labels are not currently stored inside `SnapshotRecord`
- volatility, baseline fair value, executable edge, simulation outputs, and replay slices are currently represented as separate objects in their own modules rather than persisted as snapshot columns
- timing-derived fields such as `seconds_elapsed`, `seconds_remaining`, and `window_progress` are not yet stored on the snapshot row, even though downstream replay code computes or consumes equivalent timing state
- per-venue exchange state is currently stored as mappings such as `composite_per_venue_mids` and `composite_per_venue_ages`, not as fully exploded wide columns
- several planned fields in this document, including calibrated fair value and full execution-cost state, are still design intent rather than implemented snapshot columns

These differences should be read as phase-1 scoping decisions. The original design goal remains a fully knowable-at-time replay state table that can support fair value, replay, calibration, and evaluation.

## 2. Why this table exists

The fair-value problem is not solved by a formula alone. It requires a timestamp-aligned record of:

- the correct Chainlink open anchor for the window
- the best fast nowcast available at the moment
- the current Chainlink proxy state if available
- the executable Polymarket quote state
- the remaining time in the market
- the current volatility regime
- the data quality and staleness state
- the later realized outcome for offline evaluation

This table exists so the system can answer, for any moment inside a market:

- what was known then
- what fair value the model would have computed then
- whether a trade was executable then
- whether the eventual outcome justified the trade

## 3. Grain

One row equals:

**one timestamped snapshot of one market-window state, containing only state knowable at `snapshot_ts`, plus offline-attached labels for evaluation.**

More explicitly:

- one `window_id`
- one `market_id`
- one `snapshot_ts`
- one coherent state vector

The table may contain many rows per market/window.

## 4. Snapshot construction principles

### 4.1 As-of correctness

Every non-label field must be computable using only information available at or before `snapshot_ts`.

### 4.2 No future leakage

Labels and realized outcomes may be attached later for replay evaluation, but must be clearly separated from online-computable fields.

### 4.3 Explicit quality state

A snapshot without freshness and quality fields is incomplete. Staleness is part of the state, not an afterthought.

### 4.4 Executable state, not just theoretical price

Market quote fields must support executable edge estimation. Midpoints or last trades alone are insufficient.

## 5. Recommended build cadence

Phase-1 recommendation:

- fixed snapshots every 1 second
- optional event-driven snapshots on material market, oracle, or quality changes

Regardless of cadence, the row schema remains the same.

## 5.1 Current phase-1 run contract

The current canonical replay run writes a deterministic folder shaped like:

```text
artifacts/replay/YYYY-MM-DD/run_<timestamp>/
  config_effective.yaml
  reference/
    window_reference/date=YYYY-MM-DD/part-00000.jsonl
  snapshots/
    snapshots.jsonl
    labeled_snapshots.jsonl
  simulation/
    trades.jsonl
    summary.json
  slices/
    by_<dimension>.csv
  report/
    report.md
```

This is a phase-1 execution contract, not the final storage format. The design intent below still allows later migration to parquet or a wider persisted snapshot table.

## 6. Column groups

The snapshot schema is organized into eight blocks:

1. identity
2. timing
3. reference/oracle state
4. fast nowcast state
5. market executable state
6. derived features
7. quality and gating state
8. offline labels and evaluation outputs

## 7. Canonical columns

## 7.1 Identity block

### `snapshot_id`
Type: string  
Required: yes

Unique identifier for the snapshot row.

### `window_id`
Type: string  
Required: yes

References the canonical 5-minute window.

### `market_id`
Type: string  
Required: yes

References the Polymarket market.

### `asset_id`
Type: string  
Required: yes  
Phase 1 fixed to `BTC`.

### `oracle_feed_id`
Type: string  
Required: yes

References the canonical oracle feed used for settlement alignment.

## 7.2 Timing block

### `snapshot_ts`
Type: UTC timestamp  
Required: yes

The as-of time of the snapshot.

### `window_start_ts`
Type: UTC timestamp  
Required: yes

### `window_end_ts`
Type: UTC timestamp  
Required: yes

### `seconds_elapsed`
Type: integer  
Required: yes

Definition:

Number of whole or configured-resolution seconds elapsed since `window_start_ts` at `snapshot_ts`.

### `seconds_remaining`
Type: integer  
Required: yes

Definition:

`max(0, floor(window_end_ts - snapshot_ts))` according to the configured convention.

### `window_progress`
Type: decimal  
Required: yes

Definition:

A normalized progress fraction in `[0, 1]`.

## 7.3 Reference/oracle state block

These fields tie the snapshot to the settlement definition.

### `chainlink_open_anchor_price`
Type: decimal  
Required: no, but expected in valid mapped rows

### `chainlink_open_anchor_ts`
Type: UTC timestamp  
Required: no

### `chainlink_open_anchor_method`
Type: enum  
Required: yes

### `chainlink_open_anchor_confidence`
Type: enum  
Required: yes

### `chainlink_current_price_proxy`
Type: decimal  
Required: no

Definition:

Latest known Chainlink/RTDS price at or before `snapshot_ts`.

### `chainlink_current_event_ts`
Type: UTC timestamp  
Required: no

### `chainlink_current_recv_ts`
Type: UTC timestamp  
Required: no

### `chainlink_current_age_ms`
Type: integer  
Required: yes

Definition:

Age of the latest known Chainlink tick relative to `snapshot_ts` or local snapshot build time, depending on implementation. The exact formula must be consistent and documented in code.

### `chainlink_gap_flag`
Type: boolean  
Required: yes

Definition:

True if recent oracle activity indicates a suspicious silent interval or missing-tick condition according to configured thresholds.

### `chainlink_missing_flag`
Type: boolean  
Required: yes

True if no Chainlink proxy value is available for the snapshot.

### `chainlink_minus_composite`
Type: decimal  
Required: no

Definition:

`chainlink_current_price_proxy - composite_now_price` when both are available.

## 7.4 Fast nowcast state block

These fields capture the best fast estimate of current BTC price from multiple exchanges.

### `composite_now_price`
Type: decimal  
Required: no

Definition:

Aggregated current BTC/USD nowcast using valid per-venue inputs.

### `composite_method`
Type: enum/string  
Required: yes

Examples:

- `median_mid`
- `trimmed_median_mid`
- `trimmed_mean_mid`

### `contributing_venue_count`
Type: integer  
Required: yes

### `contributing_venues`
Type: array/string-encoded list  
Required: no

Use only if storage system supports it cleanly. Otherwise store individual boolean inclusion fields or a normalized side table.

### `composite_dispersion_abs`
Type: decimal  
Required: no

Definition:

Absolute dispersion of valid venue mids at snapshot time.

### `composite_dispersion_bps`
Type: decimal  
Required: no

### `composite_max_age_ms`
Type: integer  
Required: yes

### `composite_quality_score`
Type: decimal or integer  
Required: yes

Definition:

Configured score summarizing venue freshness, count, and agreement.

### Per-venue fields

For each phase-1 venue, include explicit columns.

Recommended per venue for Binance, Coinbase, Kraken, OKX, Bybit:

- `<venue>_mid_price`
- `<venue>_best_bid`
- `<venue>_best_ask`
- `<venue>_event_ts`
- `<venue>_recv_ts`
- `<venue>_age_ms`
- `<venue>_included_flag`

Example:

- `binance_mid_price`
- `binance_age_ms`
- `binance_included_flag`

Rationale:

Wide tables are acceptable here because replay and auditability benefit from explicit per-venue state.

## 7.5 Market executable state block

These fields define the state of the Polymarket order book relevant to taker-style execution.

### `up_bid`
Type: decimal in `[0,1]`  
Required: no

### `up_ask`
Type: decimal in `[0,1]`  
Required: no

### `down_bid`
Type: decimal in `[0,1]`  
Required: no

### `down_ask`
Type: decimal in `[0,1]`  
Required: no

### `up_bid_size_contracts`
Type: decimal/integer  
Required: no

### `up_ask_size_contracts`
Type: decimal/integer  
Required: no

### `down_bid_size_contracts`
Type: decimal/integer  
Required: no

### `down_ask_size_contracts`
Type: decimal/integer  
Required: no

### `market_quote_event_ts`
Type: UTC timestamp  
Required: no

### `market_quote_recv_ts`
Type: UTC timestamp  
Required: no

### `market_quote_age_ms`
Type: integer  
Required: yes

### `market_spread_up_abs`
Type: decimal  
Required: no

Definition:

`up_ask - up_bid`

### `market_spread_down_abs`
Type: decimal  
Required: no

### `market_mid_up`
Type: decimal  
Required: no

Definition:

`(up_bid + up_ask) / 2`

### `market_mid_down`
Type: decimal  
Required: no

### `last_trade_price`
Type: decimal  
Required: no

### `last_trade_ts`
Type: UTC timestamp  
Required: no

### `last_trade_age_ms`
Type: integer  
Required: yes

### `fee_rate_estimate`
Type: decimal  
Required: yes

### `slippage_estimate_up`
Type: decimal  
Required: yes

### `slippage_estimate_down`
Type: decimal  
Required: yes

### `target_trade_size_contracts`
Type: decimal/integer  
Required: yes

## 7.6 Derived feature block

These are model-facing fields that can be computed online from the snapshot state.

### `log_move_from_open`
Type: decimal  
Required: no

Definition:

`ln(composite_now_price / chainlink_open_anchor_price)` when both are available.

### `abs_move_from_open`
Type: decimal  
Required: no

Definition:

`composite_now_price - chainlink_open_anchor_price`

### `sigma_fast`
Type: decimal  
Required: no

Definition:

Fast realized volatility estimate over a short rolling horizon.

### `sigma_baseline`
Type: decimal  
Required: no

Definition:

Slower regime baseline volatility.

### `sigma_eff`
Type: decimal  
Required: no

Definition:

Configured blended effective volatility used by the baseline fair-value model.

### `return_velocity`
Type: decimal  
Required: no

Definition:

Recent signed price-change speed or return speed over a configured lookback.

### `return_acceleration`
Type: decimal  
Required: no

Definition:

Change in recent return velocity over a configured lookback.

### `z_base`
Type: decimal  
Required: no

Definition:

The baseline standardized distance score used by the analytic fair-value model.

### `fair_value_base`
Type: decimal in `[0,1]`  
Required: no

Definition:

The baseline analytic probability for Up before empirical calibration.

### `fair_value_calibrated`
Type: decimal in `[0,1]`  
Required: no

Definition:

Empirically corrected fair value. This may be null in early phases before calibration exists.

### `edge_up_raw`
Type: decimal  
Required: no

Definition:

`fair_value_model - up_ask` or the configured raw edge definition.

### `edge_down_raw`
Type: decimal  
Required: no

Definition:

`(1 - fair_value_model) - down_ask` or configured raw edge definition.

### `edge_up_net`
Type: decimal  
Required: no

Definition:

Raw edge adjusted for fees, slippage, and model/risk buffer.

### `edge_down_net`
Type: decimal  
Required: no

## 7.7 Quality and gating block

These fields capture whether the snapshot was trustworthy and tradable.

### `composite_stale_flag`
Type: boolean  
Required: yes

### `market_stale_flag`
Type: boolean  
Required: yes

### `oracle_stale_flag`
Type: boolean  
Required: yes

### `dispersion_high_flag`
Type: boolean  
Required: yes

### `insufficient_venues_flag`
Type: boolean  
Required: yes

### `anchor_missing_flag`
Type: boolean  
Required: yes

### `book_thin_flag`
Type: boolean  
Required: yes

### `too_close_to_expiry_flag`
Type: boolean  
Required: yes

### `market_tradable_flag`
Type: boolean  
Required: yes

Definition:

High-level execution viability flag after applying configured minimum quote completeness, size, and freshness checks.

### `snapshot_valid_for_model_flag`
Type: boolean  
Required: yes

Definition:

True only if required fields for fair-value computation are present and pass baseline quality gates.

### `snapshot_valid_for_trade_flag`
Type: boolean  
Required: yes

Definition:

True only if the snapshot passes stricter tradeability gates.

### `quality_state`
Type: enum/string  
Required: yes

Suggested values:

- `green`
- `yellow`
- `red`

Interpretation belongs to config and policy docs.

## 7.8 Offline labels and evaluation block

These fields are attached only in replay/evaluation jobs. They must not be used by online snapshot builders as inputs.

### `resolved_up`
Type: boolean  
Required: no during online build; expected in labeled replay datasets

### `chainlink_settle_price`
Type: decimal  
Required: no

### `chainlink_settle_ts`
Type: UTC timestamp  
Required: no

### `settle_minus_open`
Type: decimal  
Required: no

### `model_probability_error`
Type: decimal  
Required: no

Definition:

Difference between model fair value and realized binary outcome coding, or another configured error metric.

### `sim_trade_direction`
Type: enum/string  
Required: no

Suggested values:

- `buy_up`
- `buy_down`
- `no_trade`

### `sim_entry_price`
Type: decimal  
Required: no

### `sim_exit_price`
Type: decimal  
Required: no

### `sim_fee_paid`
Type: decimal  
Required: no

### `sim_slippage_paid`
Type: decimal  
Required: no

### `sim_pnl`
Type: decimal  
Required: no

### `sim_roi`
Type: decimal  
Required: no

### `sim_outcome`
Type: enum/string  
Required: no

Suggested values:

- `win`
- `loss`
- `flat`
- `no_trade`

### `label_version`
Type: string  
Required: no in unlabeled snapshots; required in labeled replay datasets

### `simulation_version`
Type: string  
Required: no in unlabeled snapshots; required in simulated replay datasets

## 8. Nullability rules

A snapshot may be persisted even if some fields are missing, but nullability must be informative, not sloppy.

### 8.1 Required for any snapshot row

The following must always be non-null:

- `snapshot_id`
- `window_id`
- `market_id`
- `asset_id`
- `oracle_feed_id`
- `snapshot_ts`
- `window_start_ts`
- `window_end_ts`
- `seconds_elapsed`
- `seconds_remaining`
- all required boolean quality flags
- `schema_version`
- `feature_version`

### 8.2 Required for a model-valid snapshot

A row should only have `snapshot_valid_for_model_flag = true` if at minimum:

- `chainlink_open_anchor_price` is non-null
- `composite_now_price` is non-null
- `sigma_eff` is non-null
- `seconds_remaining` is valid
- required freshness gates are passed

### 8.3 Required for a trade-valid snapshot

A row should only have `snapshot_valid_for_trade_flag = true` if in addition:

- relevant executable quotes are non-null
- book sizes meet configured minimums
- fee/slippage estimates are available
- higher tradeability gates are passed

## 9. Recommended keys and indexes

### Primary key

Recommended primary key:

- `snapshot_id`

### Recommended uniqueness constraint

- unique on `(window_id, market_id, snapshot_ts, feature_version)`

### Recommended clustering/sort keys

- `date_utc(snapshot_ts)`
- `window_id`
- `snapshot_ts`

## 10. Example row

```json
{
  "snapshot_id": "snap:btc-5m-20260313T120500Z:0xabc123:20260313T120703250Z",
  "window_id": "btc-5m-20260313T120500Z",
  "market_id": "0xabc123",
  "asset_id": "BTC",
  "oracle_feed_id": "chainlink:stream:BTC-USD",
  "snapshot_ts": "2026-03-13T12:07:03.250Z",
  "window_start_ts": "2026-03-13T12:05:00Z",
  "window_end_ts": "2026-03-13T12:10:00Z",
  "seconds_elapsed": 123,
  "seconds_remaining": 176,
  "window_progress": 0.41,
  "chainlink_open_anchor_price": 83250.12,
  "chainlink_open_anchor_ts": "2026-03-13T12:05:00.100Z",
  "chainlink_open_anchor_method": "first_after_boundary",
  "chainlink_open_anchor_confidence": "medium",
  "chainlink_current_price_proxy": 83182.75,
  "chainlink_current_event_ts": "2026-03-13T12:07:02.900Z",
  "chainlink_current_recv_ts": "2026-03-13T12:07:02.940Z",
  "chainlink_current_age_ms": 350,
  "chainlink_gap_flag": false,
  "chainlink_missing_flag": false,
  "chainlink_minus_composite": -4.10,
  "composite_now_price": 83186.85,
  "composite_method": "median_mid",
  "contributing_venue_count": 4,
  "composite_dispersion_abs": 7.40,
  "composite_dispersion_bps": 0.89,
  "composite_max_age_ms": 140,
  "composite_quality_score": 0.94,
  "binance_mid_price": 83187.10,
  "binance_best_bid": 83186.90,
  "binance_best_ask": 83187.30,
  "binance_event_ts": "2026-03-13T12:07:03.180Z",
  "binance_recv_ts": "2026-03-13T12:07:03.195Z",
  "binance_age_ms": 70,
  "binance_included_flag": true,
  "coinbase_mid_price": 83185.90,
  "coinbase_best_bid": 83185.70,
  "coinbase_best_ask": 83186.10,
  "coinbase_event_ts": "2026-03-13T12:07:03.120Z",
  "coinbase_recv_ts": "2026-03-13T12:07:03.155Z",
  "coinbase_age_ms": 130,
  "coinbase_included_flag": true,
  "kraken_mid_price": 83186.85,
  "kraken_best_bid": 83186.55,
  "kraken_best_ask": 83187.15,
  "kraken_event_ts": "2026-03-13T12:07:03.100Z",
  "kraken_recv_ts": "2026-03-13T12:07:03.130Z",
  "kraken_age_ms": 150,
  "kraken_included_flag": true,
  "okx_mid_price": 83184.95,
  "okx_best_bid": 83184.75,
  "okx_best_ask": 83185.15,
  "okx_event_ts": "2026-03-13T12:07:03.160Z",
  "okx_recv_ts": "2026-03-13T12:07:03.180Z",
  "okx_age_ms": 90,
  "okx_included_flag": true,
  "bybit_mid_price": null,
  "bybit_best_bid": null,
  "bybit_best_ask": null,
  "bybit_event_ts": null,
  "bybit_recv_ts": null,
  "bybit_age_ms": 999999,
  "bybit_included_flag": false,
  "up_bid": 0.28,
  "up_ask": 0.31,
  "down_bid": 0.70,
  "down_ask": 0.73,
  "up_bid_size_contracts": 250,
  "up_ask_size_contracts": 180,
  "down_bid_size_contracts": 320,
  "down_ask_size_contracts": 210,
  "market_quote_event_ts": "2026-03-13T12:07:03.210Z",
  "market_quote_recv_ts": "2026-03-13T12:07:03.240Z",
  "market_quote_age_ms": 40,
  "market_spread_up_abs": 0.03,
  "market_spread_down_abs": 0.03,
  "market_mid_up": 0.295,
  "market_mid_down": 0.715,
  "last_trade_price": 0.30,
  "last_trade_ts": "2026-03-13T12:07:02.800Z",
  "last_trade_age_ms": 450,
  "fee_rate_estimate": 0.010,
  "slippage_estimate_up": 0.005,
  "slippage_estimate_down": 0.005,
  "target_trade_size_contracts": 100,
  "log_move_from_open": -0.000760,
  "abs_move_from_open": -63.27,
  "sigma_fast": 0.000085,
  "sigma_baseline": 0.000064,
  "sigma_eff": 0.000079,
  "return_velocity": -0.000021,
  "return_acceleration": -0.000004,
  "z_base": -0.61,
  "fair_value_base": 0.2709,
  "fair_value_calibrated": null,
  "edge_up_raw": -0.0391,
  "edge_down_raw": -0.0009,
  "edge_up_net": -0.0541,
  "edge_down_net": -0.0159,
  "composite_stale_flag": false,
  "market_stale_flag": false,
  "oracle_stale_flag": false,
  "dispersion_high_flag": false,
  "insufficient_venues_flag": false,
  "anchor_missing_flag": false,
  "book_thin_flag": false,
  "too_close_to_expiry_flag": false,
  "market_tradable_flag": true,
  "snapshot_valid_for_model_flag": true,
  "snapshot_valid_for_trade_flag": false,
  "quality_state": "green",
  "resolved_up": false,
  "chainlink_settle_price": 83210.04,
  "chainlink_settle_ts": "2026-03-13T12:10:00.090Z",
  "settle_minus_open": -40.08,
  "model_probability_error": 0.2709,
  "sim_trade_direction": "no_trade",
  "sim_entry_price": null,
  "sim_exit_price": null,
  "sim_fee_paid": null,
  "sim_slippage_paid": null,
  "sim_pnl": 0.0,
  "sim_roi": 0.0,
  "sim_outcome": "no_trade",
  "schema_version": "0.1.0",
  "feature_version": "0.1.0",
  "label_version": "0.1.0",
  "simulation_version": "0.1.0"
}
```

## 11. Validation rules and invariants

Unless status flags explicitly explain otherwise, the following should hold.

1. `window_end_ts > window_start_ts`
2. `snapshot_ts >= window_start_ts`
3. `seconds_remaining >= 0`
4. `seconds_elapsed >= 0`
5. `0 <= window_progress <= 1`
6. binary quote fields are in `[0, 1]`
7. `up_ask >= up_bid` when both are present
8. `down_ask >= down_bid` when both are present
9. if `snapshot_valid_for_model_flag = true`, required model inputs must be non-null
10. if `snapshot_valid_for_trade_flag = true`, required executable quote fields must be non-null and pass gate thresholds
11. label fields must never influence online-computable feature fields in the same build stage

## 12. Recommended dataset variants

To keep responsibilities clean, materialize snapshots in three logical forms even if they share code.

### 12.1 Base snapshots

Contains online-computable fields only. No labels.

Use cases:

- live research
- debugging
- online signal computation

### 12.2 Labeled snapshots

Base snapshot plus window outcome fields.

Use cases:

- calibration
- model evaluation

### 12.3 Simulated snapshots

Labeled snapshot plus simulated trade outputs.

Use cases:

- PnL studies
- policy tuning

## 13. Versioning and lineage

Required lineage fields for all snapshot variants:

- `schema_version`
- `feature_version`

Required lineage for labeled datasets:

- `label_version`

Required lineage for simulated datasets:

- `simulation_version`

Recommended additional lineage:

- `snapshot_build_id`
- `source_date_utc`
- `quality_config_version`
- `fee_slippage_config_version`

## 14. Partitioning guidance

Recommended parquet partitioning:

- `date_utc = date(snapshot_ts)`
- optional `asset_id`

Do not partition by `window_id` or `market_id` as top-level partition columns because that will create excessive small files.

## 15. Summary

The replay snapshot schema is the central modeling table for the project.

It exists to make every fair-value decision replayable and auditable by recording, at each moment:

- the correct oracle-anchored window context
- the best fast nowcast available
- the executable market state
- the volatility and feature state
- the freshness and quality state
- the later realized outcome for offline study

This table should be built only after the canonical schema spine and the window reference schema are defined and implemented.
