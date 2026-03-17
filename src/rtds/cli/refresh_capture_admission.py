"""Rebuild and optionally validate one capture-session admission summary."""

from __future__ import annotations

import argparse
import json
from typing import Sequence

from rtds.collectors.session_baseline import (
    load_capture_session_baseline,
    refresh_capture_admission_from_summary,
    validate_admission_summary_against_baseline,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    admission_path = refresh_capture_admission_from_summary(args.summary_path)
    print(admission_path)

    if args.baseline_manifest is None:
        return 0

    admission_summary = json.loads(admission_path.read_text(encoding="utf-8"))
    baseline = load_capture_session_baseline(args.baseline_manifest)
    issues = validate_admission_summary_against_baseline(admission_summary, baseline)
    if issues:
        for issue in issues:
            print(issue)
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--summary-path",
        required=True,
        help="Path to artifacts/collect/.../summary.json for one finished capture session.",
    )
    parser.add_argument(
        "--baseline-manifest",
        help="Optional pinned baseline manifest to validate against after refresh.",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
