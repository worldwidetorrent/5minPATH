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

This is a one-shot capture pass, not a daemon. It is enough to make `data/raw/` and `data/normalized/` non-empty with real public data.

## Start

Run from repo root:

```bash
./scripts/run_collectors.sh
```

Optional tuning:

```bash
./scripts/run_collectors.sh --timeout-seconds 30 --metadata-pages 2 --metadata-limit 500
```

## Stop

- Normal operation: the command exits on its own after one capture pass.
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
- one Chainlink round captured
- three exchange quote snapshots captured
- one Polymarket quote captured
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
