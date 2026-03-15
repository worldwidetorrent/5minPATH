# Phase-1 Capture Runbook

This repo now has one sanctioned capture path for the phase-1 data pull:

```bash
./scripts/run_collectors.sh
```

What it runs:

- Polymarket metadata collector
- Chainlink BTC/USD public Data Streams collector with RPC snapshot fallback
- Binance BTCUSDT quote collector
- Coinbase BTC-USD quote collector
- Kraken XBT/USD quote collector
- Polymarket quote collector for the selected BTC market

By default this is a one-shot capture pass, not a daemon. For bounded live work, the same sanctioned entrypoint now supports three operating profiles:

- `smoke`: coarse operational sanity checks
- `pilot`: denser replay-admission validation
- `admission`: same dense cadence profile intended for longer candidate-day work

The `pilot` and `admission` presets also widen sample-based failure thresholds for Chainlink, exchange, and Polymarket so 1-second sampling does not abort on a few seconds of transient loss.

## Deviation note

The original architecture still aims at source-faithful, continuously running collectors. The current phase-1 implementation is intentionally narrower:

- it is bounded-session polling, not long-running
- it uses public REST and RPC endpoints instead of the eventual websocket / RTDS-first stack
- it now prefers the public delayed Chainlink Data Streams BTC/USD endpoint instead of the old `latestRoundData` RPC-only path because the public stream feed is boundary-usable under the current anchor policy
- Polymarket metadata currently comes from the `up-or-down` event feed because that surface exposes the recurring BTC 5-minute family densely enough to select the exact target strip
- it persists the minimum real raw and normalized datasets needed to unblock replay-day admission work
- it now hardens the bounded acquisition path with retry/backoff, degraded-sample tracking, and threshold-based early termination because rollover-safe capture failed without that resilience layer
- it still falls back to `latestRoundData` RPC for Chainlink continuity when the public stream endpoint is unavailable, and it records oracle-source lineage explicitly in normalized ticks and window references

This deviation is deliberate. The immediate requirement is to prove the repo can produce real persisted files under the frozen layout without committing captured data or broadening the module surface prematurely.

## Start

Run from repo root:

```bash
./scripts/run_collectors.sh
```

Optional tuning:

```bash
./scripts/run_collectors.sh --timeout-seconds 30 --metadata-pages 2 --metadata-limit 500
```

10-minute smoke test:

```bash
./scripts/run_collectors.sh --capture-mode smoke --duration-seconds 600
```

20-minute boundary validation:

```bash
./scripts/run_collectors.sh --capture-mode pilot --duration-seconds 1200
```

2-hour pilot in `tmux`:

```bash
tmux new-session -d -s phaseb_pilot './scripts/run_collectors.sh --capture-mode pilot --duration-seconds 7200'
```

Optional per-source cadence override:

```bash
./scripts/run_collectors.sh \
  --capture-mode pilot \
  --metadata-poll-interval-seconds 30 \
  --chainlink-poll-interval-seconds 1 \
  --exchange-poll-interval-seconds 1 \
  --polymarket-quote-poll-interval-seconds 1 \
  --boundary-burst-enabled \
  --boundary-burst-window-seconds 15 \
  --boundary-burst-interval-seconds 1
```

Optional oracle-source override:

```bash
./scripts/run_collectors.sh --chainlink-source-preference snapshot_rpc
```

Optional resilience tuning:

```bash
./scripts/run_collectors.sh \
  --max-fetch-retries 3 \
  --base-backoff-seconds 0.5 \
  --max-backoff-seconds 5 \
  --max-consecutive-polymarket-failures 3 \
  --max-consecutive-polymarket-failures-in-grace 5 \
  --polymarket-rollover-grace-seconds 90
```

## Stop

- Normal operation: the command exits on its own after the configured pass or bounded session.
- If it hangs: press `Ctrl-C`.
- If running in `tmux`: `tmux kill-session -t phaseb_pilot`

## Logs

- Log file: `logs/collect_<session>.log`
- Summary artifact: `artifacts/collect/date=YYYY-MM-DD/session=<session>/summary.json`
- Admission artifact: `artifacts/collect/date=YYYY-MM-DD/session=<session>/admission_summary.json`
- Per-sample diagnostics: `artifacts/collect/date=YYYY-MM-DD/session=<session>/sample_diagnostics.jsonl`

## Output layout

Raw outputs:

- `data/raw/polymarket_metadata/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/raw/chainlink/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/raw/exchange/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/raw/polymarket_quotes/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`

Normalized outputs:

- `data/normalized/market_metadata_events/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/normalized/chainlink_ticks/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/normalized/exchange_quotes/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`
- `data/normalized/polymarket_quotes/date=YYYY-MM-DD/session=<session>/part-00000.jsonl`

## Health checks

Healthy collectors produce log lines showing:

- one selected BTC 5-minute target-family market with `market_id`, slug, and `window_id`
- one or more capture samples
- one or more Chainlink stream ticks or fallback rounds captured, with oracle-source lineage in diagnostics
- Binance, Coinbase, and Kraken quote snapshots captured on each sample
- one or more Polymarket quotes captured for the currently selected admitted family market
- capture-schedule details showing effective per-source interval and whether boundary burst mode is active for quote/oracle samples
- retry warnings only when a source is recovering, not on every sample
- degraded samples are logged explicitly when one source is temporarily impaired
- Polymarket 404s near rollover trigger metadata refresh and selector re-evaluation before the session treats the binding as invalid
- one summary artifact path

Non-empty output means:

- each of the raw directories above contains at least one `.jsonl` file
- each of the normalized directories above contains at least one `.jsonl` file

## First sanity checks

Run these from repo root:

```bash
find data/raw -type f | sort
find data/normalized -type f | sort
python3 - <<'PY'
from pathlib import Path
for root in (Path("data/raw"), Path("data/normalized")):
    rows = sum(path.read_text(encoding="utf-8").count("\n") for path in root.rglob("*.jsonl"))
    print(root, rows)
PY
```

Check the latest summary:

```bash
find artifacts/collect -name summary.json | sort | tail -n 1 | xargs cat
find artifacts/collect -name admission_summary.json | sort | tail -n 1 | xargs cat
```

For a smoke session, confirm:

- `sample_count` is greater than `1`
- `selector_diagnostics.candidate_count`, `admitted_count`, and `rejected_count_by_reason` are present in the summary artifact
- `selector_diagnostics.selected_window_id` is a canonical `btc-5m-...` window
- `session_diagnostics.empty_book_count`, `retry_count_by_source`, `retry_exhaustion_count_by_source`, and `termination_reason` are present in the summary artifact
- `session_diagnostics.polymarket_failure_count_by_class`, `polymarket_selector_refresh_count`, `polymarket_selector_rebind_count`, and `polymarket_rollover_grace_sample_count` are present in the summary artifact
- `data/raw/chainlink/...` has non-empty rows with stamped `recv_ts`
- `data/normalized/chainlink_ticks/...` rows carry `oracle_source`, and boundary-validation runs should normally show `chainlink_stream_public_delayed`
- `data/normalized/exchange_quotes/...` contains non-empty `binance`, `coinbase`, and `kraken` rows
- `data/normalized/market_metadata_events/...` contains only admitted target-family rows, with slugs like `btc-updown-5m-<epoch>`
- `data/normalized/polymarket_quotes/...` market IDs stay inside the admitted target-family strip even if they roll across multiple 5-minute windows during the session

For a hardened pilot, also confirm:

- `session_diagnostics.termination_reason` is `completed`
- `sample_diagnostics.jsonl` contains `healthy` or `degraded` samples with per-source status detail
- `admission_summary.json` reports family-compliance counts and off-family switch count from the final selected market/window binding per sample, while metadata-strip breadth and ambiguity are reported separately from family drift; it also includes degraded samples inside/outside rollover grace, Chainlink continuity, exchange venue continuity, mapped window count, open-anchor confidence breakdown, and `snapshot_eligible_sample_count`
- `admission_summary.json` now reports `chainlink_continuity.oracle_source_count` so the pilot can be judged on the actual oracle source used, not a generic Chainlink label
- `sample_diagnostics.jsonl` shows 1-second effective `capture_interval_seconds` for `chainlink`, `exchange`, and `polymarket_quotes` during pilot/admission mode, with `boundary_burst_active` toggling near 5-minute boundaries
- any `degraded_empty_book` sample does not terminate the session by itself
- any degraded Polymarket sample records `seconds_remaining`, `within_rollover_grace_window`, refresh-attempt flags, and final bound `market_id` / `window_id` in `source_results.polymarket_quotes.details`
- `snapshot_eligible_sample_count` is currently a conservative capture-side proxy because `build_snapshots` is still a placeholder
