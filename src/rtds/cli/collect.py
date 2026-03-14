"""Single sanctioned collection entry point."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

SANCTIONED_ENTRYPOINT = "scripts/run_collectors.sh"
DATA_SUBDIRECTORIES = ("raw", "normalized", "reference")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare the capture workspace and invoke the live collection workflow "
            "through the sanctioned orchestration path."
        )
    )
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--artifacts-root", default="artifacts")
    parser.add_argument("--logs-root", default="logs")
    parser.add_argument("--temp-root", default="tmp")
    parser.add_argument("--prepare-only", action="store_true")
    return parser


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _prepare_layout(
    data_root: Path,
    *,
    artifacts_root: Path,
    logs_root: Path,
    temp_root: Path,
) -> list[Path]:
    prepared_paths: list[Path] = []
    for subdirectory in DATA_SUBDIRECTORIES:
        path = data_root / subdirectory
        _ensure_directory(path)
        prepared_paths.append(path)

    for path in (artifacts_root, logs_root, temp_root):
        _ensure_directory(path)
        prepared_paths.append(path)

    return prepared_paths


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    prepared_paths = _prepare_layout(
        Path(args.data_root),
        artifacts_root=Path(args.artifacts_root),
        logs_root=Path(args.logs_root),
        temp_root=Path(args.temp_root),
    )

    entrypoint = os.environ.get("RTDS_COLLECTION_ENTRYPOINT")
    if entrypoint != SANCTIONED_ENTRYPOINT:
        print(
            "invoke capture via scripts/run_collectors.sh or make collect; "
            "do not run python -m rtds.cli.collect directly",
            file=sys.stderr,
        )
        return 2

    print(
        "prepared capture layout:\n" + "\n".join(f"- {path}" for path in prepared_paths),
        file=sys.stderr,
    )

    if args.prepare_only:
        print("prepare-only requested; no collectors were started", file=sys.stderr)
        return 0

    print(
        "live collectors are not implemented yet; capture orchestration is standardized, "
        "but no real collection session was started",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
