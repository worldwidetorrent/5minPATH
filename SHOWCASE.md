# 5minPATH Showcase

## One-Line Summary

`5minPATH` is a research engine for testing whether modeled edge in 5-minute BTC prediction markets survives live market conditions.

## What It Does

- captures raw market, oracle, exchange, and Polymarket quote data
- normalizes captured data into replayable datasets
- runs replay and calibration comparisons
- runs live-forward shadow measurement against captured state
- reports where modeled signal survives or fails under live conditions

## Why It Matters

Backtests can overstate what was actually tradable. This project measures whether an oracle-anchored fair-value model still works after live state availability, directional agreement, and execution realism are considered.

## Key Result

The measurement engine was validated. The tested strategy was not deployment-effective enough.

Calibration repeatedly improved replay economics, and some modeled edge survived live-forward conditions on certain clean days. Across the six clean shadow baseline days, however, live edge survival was inconsistent. The main live drags were availability first and directional disagreement second; fill loss was minor.

## What Was Built

- bounded capture pipeline for BTC 5-minute Polymarket / Chainlink markets
- raw and normalized JSONL data layout
- session summaries and admission artifacts
- replay and calibrated replay tooling
- live-forward shadow execution measurement
- cross-day diagnostics for edge survival and failure modes
- closeout docs that state what the evidence does and does not support

## Built With Codex

This project was developed with Codex as an AI coding partner. Codex helped design, implement, test, debug, document, and polish the capture/replay/calibration/shadow workflow and optional dashboard.

The repo is also structured for future AI-assisted work: clear docs, repeatable commands, explicit configs, tests, and artifact-first outputs make it easier for Codex or another agent to inspect and extend new research experiments.

## What The Evidence Says

The strongest summary is:

- real signal exists under the replay/calibration framework
- calibration is useful in replay
- live survival is real on some days
- live survival is not consistent enough for deployment
- availability and side mismatch dominate live losses
- fill mechanics are not the main bottleneck

See [sample outputs](docs/examples/sample_outputs.md) for the compact six-day comparison.

## How To Run

Bootstrap:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Validate:

```bash
python -m pytest -q
python -m ruff check src tests
```

Run a bounded smoke capture:

```bash
./scripts/run_collectors.sh --capture-mode smoke --duration-seconds 600
```

## Current Status

This is a completed research/data pipeline and reusable measurement tool. It is not a trading bot, not a generic any-market Polymarket toolkit, and not a deployment recommendation.

Start with:

- [README](README.md)
- [Closeout decision](docs/decisions/0009_phase1_closeout.md)
- [Evidence index](docs/baselines/20260421_phase1_evidence_index.md)
- [How to use the repo](docs/how_to_use.md)
