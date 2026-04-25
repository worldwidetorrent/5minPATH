# Command Map

This page lists the shortest practical command paths for using `5minPATH`.

It is not an exhaustive list of every script in the repo. Most analysis scripts are historical diagnostics or evidence generators. Start with the documented paths below.

## Install And Validate

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
python -m pytest -q
python -m ruff check src tests
```

## Optional Dashboard

```bash
python -m pip install -e '.[dashboard]'
streamlit run dashboard/app.py
```

The dashboard is a read-only local showcase layer. It is not required for capture, replay, calibration, or shadow measurement.

## Capture

Bounded smoke capture:

```bash
./scripts/run_collectors.sh --capture-mode smoke --duration-seconds 600
```

See [Capture runbook](05_capture_runbook.md) for capture modes and operational notes.

## Daily Closeout

Cheap daily fast lane for a completed session:

```bash
./scripts/run_day_fast_lane.sh YYYY-MM-DD <session-id>
```

Use this for routine post-run summaries before considering any heavier checkpoint refresh.

## Heavier Closeout

Optimized post-run closeout:

```bash
./scripts/run_day_optimized_postrun.sh YYYY-MM-DD <session-id>
```

Checkpoint refresh:

```bash
./scripts/run_checkpoint_refresh.sh
```

Use heavier paths only when the session is a milestone, a formal report update, or a deliberate checkpoint.

## Shadow Sidecar

The live-forward shadow sidecar is launched through:

```bash
./scripts/run_shadow_live.sh
```

Shadow output is for live-forward tradability measurement. It is not an authenticated trading path.

## What Not To Run First

Do not begin by running every file in `scripts/`.

Most `analyze_*` scripts are historical diagnostics or evidence generators. They are preserved for reproducibility, but the primary user path is:

1. install and validate
2. bounded capture
3. daily fast-lane closeout
4. optional dashboard
5. targeted evidence scripts only when reproducing a specific note
