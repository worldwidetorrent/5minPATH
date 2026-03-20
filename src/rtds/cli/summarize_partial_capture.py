"""Summarize one incomplete capture session from crash-safe checkpoints."""

from __future__ import annotations

import argparse

from rtds.collectors.partial_session import evaluate_partial_capture_session


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build partial-session summary and admission artifacts from "
            "summary.partial.json."
        )
    )
    parser.add_argument("partial_summary_path")
    args = parser.parse_args(argv)

    evaluation = evaluate_partial_capture_session(args.partial_summary_path)
    print(evaluation.partial_summary_path)
    if evaluation.partial_admission_path is not None:
        print(evaluation.partial_admission_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
