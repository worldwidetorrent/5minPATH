# How To Use 5minPATH

This is the shortest practical path for using `5minPATH` as a tool.

The repo is best suited for:

- BTC 5-minute Polymarket markets
- bounded research sessions
- capture, replay, calibration, and shadow measurement

It is not validated as an indefinitely running multi-day daemon and it is not a live trading system.

## 1. Start Here

Read these first:

- [README](../README.md)
- [Current capabilities and validation boundary](current_capabilities.md)
- [Capture runbook](05_capture_runbook.md)

## 2. Bootstrap A Local Environment

From repo root:

Recommended interpreter: Python `3.12`

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Basic verification:

```bash
python -m pytest -q
python -m ruff check src tests
```

The repo currently assumes a local virtual environment for repeatable tooling. If `pytest` is not on your global `PATH`, that is expected; use the active venv or `.venv/bin/python`.

## 3. Run A First Capture

From repo root, start with a bounded smoke run:

```bash
./scripts/run_collectors.sh --capture-mode smoke --duration-seconds 600
```

If you want a denser validation run:

```bash
./scripts/run_collectors.sh --capture-mode pilot --duration-seconds 1200
```

For a full day-style bounded run, use the same sanctioned collector entrypoint with a longer duration or the team’s normal session orchestration.

## 4. Check What Landed

The first places to inspect are:

- `logs/collect_<session>.log`
- `artifacts/collect/date=YYYY-MM-DD/session=<session>/summary.json`
- `artifacts/collect/date=YYYY-MM-DD/session=<session>/admission_summary.json`
- `data/raw/...`
- `data/normalized/...`

Quick sanity checks:

```bash
find data/raw -type f | sort
find data/normalized -type f | sort
find artifacts/collect -name summary.json | sort | tail -n 1 | xargs cat
```

Healthy output means:

- raw files exist
- normalized files exist
- the session summary exists
- the run terminated cleanly or at least produced partial checkpoint artifacts

For deeper operational checks, use the [Capture runbook](05_capture_runbook.md).

## 5. Run Post-Run Analysis

For a completed session, the normal cheap daily path is the fast lane:

```bash
./scripts/run_day_fast_lane.sh YYYY-MM-DD <session-id>
```

If you want the fuller optimized closeout path:

```bash
./scripts/run_day_optimized_postrun.sh YYYY-MM-DD <session-id>
```

These workflows produce artifacts under `artifacts/...` for replay, calibration, and execution-side diagnostics.

For direct shadow runtime launches, the live launcher uses its own UTC startup time as the attach boundary unless you pass an explicit `--shadow-attach-ts`. That keeps `live_only_from_attach_ts` honest for real live runs while still allowing controlled fixture or replay-oriented launches.

## 6. Where The Outputs Go

Main output families:

- `data/raw/` for captured source data
- `data/normalized/` for normalized research inputs
- `artifacts/collect/` for capture summaries and admission artifacts
- `artifacts/replay...` for replay outputs
- `artifacts/diagnostics...` for comparison and analysis outputs
- `artifacts/shadow/` for live-forward shadow artifacts

This tool gives you files and reports, not a hosted UI.

## 7. What A User Can Do With The Data

Once a session is captured and closed out, you can:

- inspect market/oracle/quote behavior over time
- rebuild knowable-at-time replay rows
- compare raw versus calibrated replay behavior
- study live-forward shadow actionability and survival
- export the normalized files and artifact summaries into notebooks or your own dashboards

## 8. Where To Make Your Own Tweaks

Start here if you want to adapt the repo for your own experiments:

- `configs/replay/`
- `scripts/`
- `src/rtds/cli/`
- `src/rtds/`

Practical rule:

- if you change policy, calibration, market binding, or gating assumptions, you are running a new experiment rather than reproducing the documented evidence set

Useful related docs:

- [Architecture overview](architecture_overview.md)
- [Market thesis](market_thesis.md)
- [Stage-1 calibration decision](decisions/0006_stage1_good_only_calibration.md)
- [Execution-v0 shadow boundary](decisions/0008_execution_v0_shadow_boundary.md)

## 9. What Not To Assume

Do not assume this repo is:

- generic any-market Polymarket support with no adaptation
- an authenticated execution stack
- validated for indefinite continuous runtime
- a deployment-ready trading system

It is a usable research and measurement engine with a documented validation boundary.
