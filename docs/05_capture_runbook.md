# Phase-1 Capture Runbook

This repo now has one sanctioned capture path for the phase-1 data pull:

```bash
./scripts/run_collectors.sh
```

What it runs:

- Polymarket metadata collector
- Chainlink BTC/USD latest-round collector
- Binance BTCUSDT quote collector
- Coinbase BTC-USD quote collector
- Kraken XBT/USD quote collector
- Polymarket quote collector for the selected BTC market

By default this is a one-shot capture pass, not a daemon. For live smoke testing, the same sanctioned entrypoint can run as a bounded polling session.

## Deviation note

The original architecture still aims at source-faithful, continuously running collectors. The current phase-1 implementation is intentionally narrower:

- it is bounded-session polling, not long-running
- it uses public REST and RPC endpoints instead of the eventual websocket / RTDS-first stack
- Polymarket metadata currently comes from active event pages because that surface exposes a broader live BTC candidate strip than the market listing feed did during smoke validation
- it persists the minimum real raw and normalized datasets needed to unblock replay-day admission work

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
./scripts/run_collectors.sh --duration-seconds 600 --poll-interval-seconds 60
```

## Stop

- Normal operation: the command exits on its own after the configured pass or bounded session.
- If it hangs: press `Ctrl-C`.

## Logs

- Log file: `logs/collect_<session>.log`
- Summary artifact: `artifacts/collect/date=YYYY-MM-DD/session=<session>/summary.json`

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

- one selected BTC Polymarket market
- one or more capture samples
- one or more Chainlink rounds captured
- Binance, Coinbase, and Kraken quote snapshots captured on each sample
- one or more Polymarket quotes captured for the selected market
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
```

For a smoke session, confirm:

- `sample_count` is greater than `1`
- `data/raw/chainlink/...` has non-empty rows with stamped `recv_ts`
- `data/normalized/exchange_quotes/...` contains non-empty `binance`, `coinbase`, and `kraken` rows
- `data/normalized/market_metadata_events/...` includes sane BTC market slugs and at least one inactive/prelisted-looking candidate row
- `data/normalized/polymarket_quotes/...` market IDs match the selected market in the summary artifact
