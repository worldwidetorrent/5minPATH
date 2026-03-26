# Binance Exclusion Audit

Date: `2026-03-26`
Session: `20260326T055920480Z`

## Question

Why is Binance excluded from the live shadow composite path?

## Verdict

Binance is being excluded **before** freshness/outlier eligibility because its normalized rows are not exact `normalized` rows.

The exclusion path is:
1. Binance raw payload arrives
2. [`normalize_exchange_quote`](/home/ubuntu/testingproject/src/rtds/normalizers/exchange.py)
   sets:
   - `event_ts = parsed["event_ts"] or recv_ts`
   - `source_event_missing_ts_flag = parsed["event_ts"] is None`
3. [`_normalization_status(...)`](/home/ubuntu/testingproject/src/rtds/normalizers/exchange.py)
   returns `normalized_with_missing_event_ts`
4. [`_latest_valid_quotes_by_venue(...)`](/home/ubuntu/testingproject/src/rtds/features/composite_nowcast.py)
   drops any quote whose `normalization_status != "normalized"`
5. live shadow never gets Binance into the composite candidate set

## Exact counts

From normalized Day 3 Binance quotes for session `20260326T055920480Z`:
- total Binance normalized rows inspected: `6180`
- `normalization_status_excluded`: `6180`
- `valid_for_composite`: `0`
- `crossed_market`: `0`
- `locked_market`: `0`

Normalization status counts:
- `normalized_with_missing_event_ts`: `6180`

Source-event flag counts:
- `source_event_missing_ts_flag = true`: `6180`

Crossed / locked counts:
- `crossed_market_flag = false`: `6180`
- `locked_market_flag = false`: `6180`

Event vs receive delta:
- `recv_ts - event_ts` summary: min `0 ms`, p50 `0 ms`, p90 `0 ms`, max `0 ms`

That last point is consistent with the normalizer fallback:
- when Binance source event time is missing, `event_ts` is backfilled to `recv_ts`
- but the row is still marked `normalized_with_missing_event_ts`
- so it is still excluded later by the composite-validity rule

## Interpretation

This is not an outlier-rejection problem.

It is not a freshness-threshold problem.

It is not happening after venue eligibility.

It is an **upstream normalization-status exclusion**:
- Binance rows are reaching the system
- they are not crossed
- they are not locked
- but they are tagged as `normalized_with_missing_event_ts`
- and the composite builder only accepts exact `normalized`

## Bottom line

Binance exclusion happens:
- because of schema/normalization status
- before trust/outlier eligibility
- because the composite builder intentionally filters out non-`normalized` exchange rows

So the next Binance question is not “why is it an outlier?” It is:
- should `normalized_with_missing_event_ts` remain excluded for live shadow composite input,
- or should shadow diagnostic mode allow it under an explicit degraded-quality label?
