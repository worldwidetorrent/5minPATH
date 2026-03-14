# ADR 0002 — Composite Nowcast Method

Status: Accepted  
Date: 2026-03-13  
Owners: Research / Data

## Context

The project needs a fast nowcast of current BTC price to compare against Polymarket contract pricing while markets settle on Chainlink BTC/USD. The fair-value engine therefore requires a robust, low-latency multi-venue estimate of current BTC/USD that is:

- faster than waiting on a public Chainlink relay
- resistant to one bad venue or stale input
- deterministic in replay
- simple enough to audit

The canonical project design already distinguishes:

- settlement anchor: Chainlink open and settle
- fast nowcast: exchange composite
- executable market state: Polymarket book

This ADR fixes the phase-1 composite construction method.

## Decision

Phase-1 `composite_now_price` is computed from **per-venue mid prices** using a **freshness-filtered robust median family**.

The method is:

1. collect the latest valid quote for each supported venue
2. discard venues that fail freshness or sanity filters
3. compute venue mid price as `(best_bid + best_ask) / 2`
4. aggregate surviving venue mids using:
   - simple median when valid venue count is 3 or 4
   - trimmed median / median-family robust aggregate when valid venue count is 5 or more
5. emit `composite_now_price` with full diagnostics:
   - contributing venues
   - venue ages
   - venue mids
   - dispersion metrics
   - composite quality flags

For phase 1, supported venues are:

- Binance
- Coinbase
- Kraken

Optional expansion after stability:

- OKX
- Bybit

## Why mid prices

Mid prices are chosen because they:

- avoid last-trade noise and irregular trade arrival timing
- better reflect current market state than a single trade print
- are easy to compute consistently across venues
- are suitable for a composite intended as a nowcast, not as an executable fill price

The composite is a state estimate, not a trade price.

## Freshness and validity requirements

A venue quote is eligible only if all of the following hold:

1. `best_bid` is present
2. `best_ask` is present
3. `best_bid <= best_ask`
4. quote age is below configured threshold
5. quote is not marked malformed or stale
6. venue is not explicitly quarantined by quality logic

A venue may also be rejected for outlier behavior under configured dispersion rules.

## Minimum venue count

The minimum valid venue count is **3** in phase 1.

If fewer than 3 venues are valid:

- `composite_now_price` is null
- `composite_missing_flag = true`
- the snapshot must carry degraded quality state
- trading logic should treat this as no-trade unless explicitly overridden in research mode

## Aggregation rules

### Case A — 3 valid venues

Use the median of the 3 mids.

### Case B — 4 valid venues

Use the median of the 4 mids defined as the average of the two center values after sorting.

### Case C — 5 or more valid venues

Use a robust median-family aggregate.

Phase-1 default:

- sort venue mids
- trim one high and one low venue when valid count >= 5
- take the median of the remaining set

This preserves robustness without overcomplicating the implementation.

## Required diagnostics

Every composite computation must preserve enough state for audit and replay. At minimum:

- `composite_method`
- `contributing_venue_count`
- `contributing_venues`
- `venue_mid_binance`, `venue_mid_coinbase`, etc.
- `venue_age_ms_binance`, etc.
- `composite_dispersion_abs`
- `composite_dispersion_bps`
- `composite_quality_score` or equivalent flags

## Why not volume weighting in phase 1

Volume-weighted or liquidity-weighted composites were considered but rejected for phase 1 because:

- venue liquidity data is not always symmetric or equally available
- weighting adds model surface area before replay reliability is proven
- robust medians are easier to audit when diagnosing false signals

A weighted method may be introduced later behind a versioned feature flag.

## Why not use Chainlink as the composite

Rejected because the project intentionally separates:

- settlement-aligned oracle state
- faster exchange-derived nowcast state

Using the public Chainlink relay as the nowcast would collapse that distinction and weaken the lag signal the strategy is trying to study.

## Why not use last trade

Rejected because:

- trades arrive irregularly
- one venue can have a stale last trade even when quotes moved
- single prints are noisier than quoted state

## Alternatives considered

### Alternative A — single exchange only

Rejected because one venue can be stale, glitched, or temporarily dislocated.

### Alternative B — arithmetic mean of mids

Rejected for phase 1 because one bad venue can move the mean more than desired.

### Alternative C — volume-weighted mean

Rejected for phase 1 due to complexity, data comparability concerns, and weaker auditability.

### Alternative D — microprice per venue

Deferred. This may become useful later if order-book depth is consistently available and trustworthy.

## Consequences

### Positive

- robust against one bad venue
- deterministic and easy to test
- easy to explain in replay and research reports
- naturally pairs with dispersion-based quality gating

### Negative

- ignores venue-specific liquidity differences in phase 1
- may lag slightly behind a more sophisticated weighted estimator in rare cases

This is acceptable for the current research goal.

## Implementation requirements

- `src/rtds/features/composite_nowcast.py` is the authoritative implementation.
- freshness thresholds come from config, not hard-coded constants.
- venue inclusion/exclusion must be reproducible in replay.
- output must carry `feature_version`.

## Acceptance tests

Unit tests must verify:

1. median behavior for 3 venues
2. median-of-four behavior for 4 venues
3. trimming behavior for 5+ venues
4. stale venue exclusion
5. malformed quote exclusion
6. null output when minimum venue count is not met
7. deterministic output for fixed inputs

## Future extension path

The method may later evolve to:

- weighted robust median
- quote-size-aware weighting
- venue trust scores
- venue-specific latency penalties

Any such change must produce a new feature version and new ADR or ADR update.
