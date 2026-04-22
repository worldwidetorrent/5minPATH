# testingproject

`testingproject` is an early research-system branch for capturing, replaying, and evaluating **5-minute BTC Polymarket markets that resolve on Chainlink**.

This branch is best read as a **historical milestone** in the repo, not as the canonical current branch. It preserves an earlier capture/replay/admission state of the project without exposing the full strategy narrative on the landing page.

## Status

This branch contains an earlier version of the system focused on:

- bounded capture
- raw and normalized data writes
- deterministic replay-day analysis
- admission refresh and degraded-window review
- policy-stack comparison on captured sessions

It is not the most up-to-date version of the repo.

## What This Branch Does

Out of the box, this branch can:

- run bounded capture sessions for the BTC 5-minute market family
- write raw and normalized JSONL datasets
- refresh capture admission artifacts
- rebuild replay-day outputs for completed sessions
- compare replay regimes and policy stacks
- generate artifact-first research outputs

Main outputs:

- `data/raw/...`
- `data/normalized/...`
- `artifacts/...`

## What This Branch Does Not Do

This branch is not:

- a production trading system
- an authenticated execution stack
- a generic any-market Polymarket toolkit
- a polished product or dashboard
- the current canonical branch for this repository

It also predates later work that was added on `main`, including newer execution-shadow infrastructure, later calibration work, CI, and broader documentation cleanup.

## Bootstrap

Recommended interpreter: Python `3.11`

From repo root:

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

## Quickstart

Most useful commands on this branch:

1. Bounded smoke capture:

```bash
./scripts/run_collectors.sh --capture-mode smoke --duration-seconds 600
```

2. Refresh capture admission for a completed session:

```bash
python -m rtds.cli.refresh_capture_admission \
  --date YYYY-MM-DD \
  --session-id <session-id>
```

3. Replay one captured day:

```bash
python -m rtds.cli.replay_day \
  --date YYYY-MM-DD \
  --session-id <session-id>
```

4. Compare policy stacks:

```bash
python -m rtds.cli.compare_policy_stacks \
  --date YYYY-MM-DD \
  --session-id <session-id>
```

Convenience wrappers also exist under `scripts/`.

## Repo Layout

Key paths:

- `src/rtds/` — core library and CLI implementation
- `scripts/` — operational wrappers
- `data/` — raw and normalized datasets
- `artifacts/` — capture, replay, and analysis outputs
- `docs/` — schema docs, runbooks, ADRs, and baseline notes

## Where To Read Next

Best starting docs on this branch:

- [Capture runbook](docs/05_capture_runbook.md)
- [Canonical schema spine](docs/01_canonical_schema_spine.md)
- [Window reference schema](docs/02_window_reference_schema.md)
- [Replay snapshot schema](docs/03_replay_snapshot_schema.md)
- [Raw and normalized feed schema](docs/04_raw_normalized_feed_schema.md)
- [Policy v1 and admission v2 decision](docs/decisions/0005_policy_v1_and_admission_v2.md)

Historical baseline references:

- [20260316T101341416Z](docs/baselines/20260316T101341416Z.md)
- [20260317T033427850Z](docs/baselines/20260317T033427850Z.md)
- [Task 7 reference inputs](docs/baselines/task7_reference_inputs.md)

## Bottom Line

This branch is a **useful archival capture/replay milestone** for the project.

It is appropriate for:

- understanding the earlier capture and replay pipeline
- reproducing older bounded-session research artifacts
- reviewing the repo before the later execution-shadow and closeout work landed

It should not be treated as the current front door of the repository.
