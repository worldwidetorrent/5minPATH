"""Evaluate an existing replay run by locating its report artifact."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Sequence

DEFAULT_OUTPUT_ROOT = "artifacts"


def main(argv: Sequence[str] | None = None) -> int:
    """Locate the canonical report for a replay run and print its path."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir")
    parser.add_argument("--date")
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)

    run_dir = _resolve_run_dir(args.run_dir, date_token=args.date, output_root=args.output_root)
    report_path = run_dir / "report" / "report.md"
    if not report_path.exists():
        raise FileNotFoundError(f"report artifact not found: {report_path}")
    print(report_path)
    return 0


def _resolve_run_dir(
    run_dir: str | None,
    *,
    date_token: str | None,
    output_root: str,
) -> Path:
    if run_dir:
        return Path(run_dir)
    if date_token is None:
        raise ValueError("either --run-dir or --date must be provided")

    trade_date = date.fromisoformat(date_token).isoformat()
    replay_root = Path(output_root) / "replay" / trade_date
    run_dirs = sorted(path for path in replay_root.glob("run_*") if path.is_dir())
    if not run_dirs:
        raise FileNotFoundError(f"no replay runs found under {replay_root}")
    return run_dirs[-1]


if __name__ == "__main__":
    raise SystemExit(main())
