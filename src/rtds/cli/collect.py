"""Single sanctioned collection entry point."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Sequence

from rtds.collectors.phase1_capture import (
    DEFAULT_DURATION_SECONDS,
    DEFAULT_METADATA_LIMIT,
    DEFAULT_METADATA_PAGES,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    Phase1CaptureConfig,
    run_phase1_capture,
)
from rtds.core.time import format_utc_compact, utc_now

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
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--metadata-limit", type=int, default=DEFAULT_METADATA_LIMIT)
    parser.add_argument("--metadata-pages", type=int, default=DEFAULT_METADATA_PAGES)
    parser.add_argument("--duration-seconds", type=float, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
    )
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


def _build_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger(f"rtds.collect.{log_path.stem}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


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

    session_id = format_utc_compact(utc_now(), include_millis=True)
    log_path = Path(args.logs_root) / f"collect_{session_id}.log"
    logger = _build_logger(log_path)
    logger.info("prepared capture layout:\n%s", "\n".join(f"- {path}" for path in prepared_paths))

    if args.prepare_only:
        logger.info("prepare-only requested; no collectors were started")
        return 0

    config = Phase1CaptureConfig(
        data_root=Path(args.data_root),
        artifacts_root=Path(args.artifacts_root),
        logs_root=Path(args.logs_root),
        temp_root=Path(args.temp_root),
        session_id=session_id,
        timeout_seconds=args.timeout_seconds,
        metadata_limit=args.metadata_limit,
        metadata_pages=args.metadata_pages,
        duration_seconds=args.duration_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )
    try:
        result = run_phase1_capture(config, logger=logger)
    except Exception as exc:  # pragma: no cover - exercised through integration paths
        logger.exception("phase-1 capture failed")
        print(f"phase-1 capture failed: {exc}", file=sys.stderr)
        return 1

    logger.info("summary artifact written to %s", result.summary_path)
    print(result.summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
