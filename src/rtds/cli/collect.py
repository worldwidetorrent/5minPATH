"""Single sanctioned collection entry point."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Sequence

from rtds.collectors.admission_summary import write_capture_admission_summary
from rtds.collectors.phase1_capture import (
    DEFAULT_BASE_BACKOFF_SECONDS,
    DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS,
    DEFAULT_BOUNDARY_BURST_WINDOW_SECONDS,
    DEFAULT_CHAINLINK_POLL_INTERVAL_SECONDS,
    DEFAULT_CHAINLINK_SOURCE_PREFERENCE,
    DEFAULT_CHAINLINK_STREAMS_BASE_URL,
    DEFAULT_CHAINLINK_STREAMS_FEED_ID,
    DEFAULT_CHAINLINK_STREAMS_PAGE_URL,
    DEFAULT_DURATION_SECONDS,
    DEFAULT_EXCHANGE_POLL_INTERVAL_SECONDS,
    DEFAULT_MAX_BACKOFF_SECONDS,
    DEFAULT_MAX_CONSECUTIVE_CHAINLINK_FAILURES,
    DEFAULT_MAX_CONSECUTIVE_EXCHANGE_FAILURES,
    DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES,
    DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES_IN_GRACE,
    DEFAULT_MAX_CONSECUTIVE_SELECTION_FAILURES,
    DEFAULT_MAX_CONSECUTIVE_UNUSABLE_POLYMARKET_WINDOWS,
    DEFAULT_MAX_FETCH_RETRIES,
    DEFAULT_METADATA_LIMIT,
    DEFAULT_METADATA_PAGES,
    DEFAULT_METADATA_POLL_INTERVAL_SECONDS,
    DEFAULT_POLYMARKET_QUOTE_POLL_INTERVAL_SECONDS,
    DEFAULT_POLYMARKET_ROLLOVER_GRACE_SECONDS,
    DEFAULT_POLYMARKET_UNUSABLE_WINDOW_MIN_QUOTE_COVERAGE_RATIO,
    DEFAULT_TIMEOUT_SECONDS,
    Phase1CaptureConfig,
    run_phase1_capture,
)
from rtds.core.time import format_utc_compact, utc_now

SANCTIONED_ENTRYPOINT = "scripts/run_collectors.sh"
DATA_SUBDIRECTORIES = ("raw", "normalized", "reference")
CAPTURE_MODE_SMOKE = "smoke"
CAPTURE_MODE_PILOT = "pilot"
CAPTURE_MODE_ADMISSION = "admission"


def _capture_mode_defaults(mode: str) -> dict[str, object]:
    if mode == CAPTURE_MODE_PILOT:
        return {
            "metadata_poll_interval_seconds": 30.0,
            "chainlink_poll_interval_seconds": 1.0,
            "exchange_poll_interval_seconds": 1.0,
            "polymarket_quote_poll_interval_seconds": 1.0,
            "boundary_burst_enabled": True,
            "boundary_burst_window_seconds": DEFAULT_BOUNDARY_BURST_WINDOW_SECONDS,
            "boundary_burst_interval_seconds": DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS,
            "max_consecutive_selection_failures": DEFAULT_MAX_CONSECUTIVE_SELECTION_FAILURES,
            "max_consecutive_chainlink_failures": 15,
            "max_consecutive_exchange_failures": 15,
            "max_consecutive_polymarket_failures": 15,
            "max_consecutive_polymarket_failures_in_grace": 30,
            "max_consecutive_unusable_polymarket_windows": 2,
            "polymarket_unusable_window_min_quote_coverage_ratio": 0.20,
        }
    if mode == CAPTURE_MODE_ADMISSION:
        return {
            "metadata_poll_interval_seconds": 30.0,
            "chainlink_poll_interval_seconds": 1.0,
            "exchange_poll_interval_seconds": 1.0,
            "polymarket_quote_poll_interval_seconds": 1.0,
            "boundary_burst_enabled": True,
            "boundary_burst_window_seconds": DEFAULT_BOUNDARY_BURST_WINDOW_SECONDS,
            "boundary_burst_interval_seconds": DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS,
            "max_consecutive_selection_failures": DEFAULT_MAX_CONSECUTIVE_SELECTION_FAILURES,
            "max_consecutive_chainlink_failures": 15,
            "max_consecutive_exchange_failures": 15,
            "max_consecutive_polymarket_failures": 15,
            "max_consecutive_polymarket_failures_in_grace": 30,
            "max_consecutive_unusable_polymarket_windows": 1,
            "polymarket_unusable_window_min_quote_coverage_ratio": 0.50,
        }
    return {
        "metadata_poll_interval_seconds": DEFAULT_METADATA_POLL_INTERVAL_SECONDS,
        "chainlink_poll_interval_seconds": DEFAULT_CHAINLINK_POLL_INTERVAL_SECONDS,
        "exchange_poll_interval_seconds": DEFAULT_EXCHANGE_POLL_INTERVAL_SECONDS,
        "polymarket_quote_poll_interval_seconds": (
            DEFAULT_POLYMARKET_QUOTE_POLL_INTERVAL_SECONDS
        ),
        "boundary_burst_enabled": False,
        "boundary_burst_window_seconds": DEFAULT_BOUNDARY_BURST_WINDOW_SECONDS,
        "boundary_burst_interval_seconds": DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS,
        "max_consecutive_selection_failures": DEFAULT_MAX_CONSECUTIVE_SELECTION_FAILURES,
        "max_consecutive_chainlink_failures": DEFAULT_MAX_CONSECUTIVE_CHAINLINK_FAILURES,
        "max_consecutive_exchange_failures": DEFAULT_MAX_CONSECUTIVE_EXCHANGE_FAILURES,
        "max_consecutive_polymarket_failures": DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES,
        "max_consecutive_polymarket_failures_in_grace": (
            DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES_IN_GRACE
        ),
        "max_consecutive_unusable_polymarket_windows": (
            DEFAULT_MAX_CONSECUTIVE_UNUSABLE_POLYMARKET_WINDOWS
        ),
        "polymarket_unusable_window_min_quote_coverage_ratio": (
            DEFAULT_POLYMARKET_UNUSABLE_WINDOW_MIN_QUOTE_COVERAGE_RATIO
        ),
    }


def _resolve_capture_timing(args: argparse.Namespace) -> dict[str, object]:
    resolved = _capture_mode_defaults(args.capture_mode)
    if args.poll_interval_seconds is not None:
        resolved.update(
            {
                "metadata_poll_interval_seconds": args.poll_interval_seconds,
                "chainlink_poll_interval_seconds": args.poll_interval_seconds,
                "exchange_poll_interval_seconds": args.poll_interval_seconds,
                "polymarket_quote_poll_interval_seconds": args.poll_interval_seconds,
            }
        )
    for key in (
        "metadata_poll_interval_seconds",
        "chainlink_poll_interval_seconds",
        "exchange_poll_interval_seconds",
        "polymarket_quote_poll_interval_seconds",
        "boundary_burst_window_seconds",
        "boundary_burst_interval_seconds",
        "max_consecutive_selection_failures",
        "max_consecutive_chainlink_failures",
        "max_consecutive_exchange_failures",
        "max_consecutive_polymarket_failures",
        "max_consecutive_polymarket_failures_in_grace",
        "max_consecutive_unusable_polymarket_windows",
        "polymarket_unusable_window_min_quote_coverage_ratio",
    ):
        value = getattr(args, key)
        if value is not None:
            resolved[key] = value
    if args.boundary_burst_enabled is not None:
        resolved["boundary_burst_enabled"] = args.boundary_burst_enabled
    resolved["poll_interval_seconds"] = min(
        float(resolved["chainlink_poll_interval_seconds"]),
        float(resolved["exchange_poll_interval_seconds"]),
        float(resolved["polymarket_quote_poll_interval_seconds"]),
    )
    return resolved


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
        "--capture-mode",
        choices=(CAPTURE_MODE_SMOKE, CAPTURE_MODE_PILOT, CAPTURE_MODE_ADMISSION),
        default=CAPTURE_MODE_SMOKE,
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=None,
    )
    parser.add_argument("--metadata-poll-interval-seconds", type=float, default=None)
    parser.add_argument("--chainlink-poll-interval-seconds", type=float, default=None)
    parser.add_argument("--exchange-poll-interval-seconds", type=float, default=None)
    parser.add_argument("--polymarket-quote-poll-interval-seconds", type=float, default=None)
    parser.add_argument("--max-consecutive-unusable-polymarket-windows", type=int, default=None)
    parser.add_argument(
        "--polymarket-unusable-window-min-quote-coverage-ratio",
        type=float,
        default=None,
    )
    parser.add_argument(
        "--chainlink-source-preference",
        choices=("streams_public", "snapshot_rpc"),
        default=DEFAULT_CHAINLINK_SOURCE_PREFERENCE,
    )
    parser.add_argument(
        "--chainlink-streams-base-url",
        default=DEFAULT_CHAINLINK_STREAMS_BASE_URL,
    )
    parser.add_argument(
        "--chainlink-streams-page-url",
        default=DEFAULT_CHAINLINK_STREAMS_PAGE_URL,
    )
    parser.add_argument(
        "--chainlink-streams-feed-id",
        default=DEFAULT_CHAINLINK_STREAMS_FEED_ID,
    )
    parser.add_argument(
        "--boundary-burst-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument("--boundary-burst-window-seconds", type=float, default=None)
    parser.add_argument("--boundary-burst-interval-seconds", type=float, default=None)
    parser.add_argument("--max-fetch-retries", type=int, default=DEFAULT_MAX_FETCH_RETRIES)
    parser.add_argument(
        "--base-backoff-seconds",
        type=float,
        default=DEFAULT_BASE_BACKOFF_SECONDS,
    )
    parser.add_argument(
        "--max-backoff-seconds",
        type=float,
        default=DEFAULT_MAX_BACKOFF_SECONDS,
    )
    parser.add_argument(
        "--max-consecutive-selection-failures",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--max-consecutive-chainlink-failures",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--max-consecutive-exchange-failures",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--max-consecutive-polymarket-failures",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--max-consecutive-polymarket-failures-in-grace",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--polymarket-rollover-grace-seconds",
        type=float,
        default=DEFAULT_POLYMARKET_ROLLOVER_GRACE_SECONDS,
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

    timing = _resolve_capture_timing(args)
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
        poll_interval_seconds=float(timing["poll_interval_seconds"]),
        metadata_poll_interval_seconds=float(timing["metadata_poll_interval_seconds"]),
        chainlink_poll_interval_seconds=float(timing["chainlink_poll_interval_seconds"]),
        exchange_poll_interval_seconds=float(timing["exchange_poll_interval_seconds"]),
        polymarket_quote_poll_interval_seconds=float(
            timing["polymarket_quote_poll_interval_seconds"]
        ),
        max_fetch_retries=args.max_fetch_retries,
        base_backoff_seconds=args.base_backoff_seconds,
        max_backoff_seconds=args.max_backoff_seconds,
        max_consecutive_selection_failures=int(timing["max_consecutive_selection_failures"]),
        max_consecutive_chainlink_failures=int(timing["max_consecutive_chainlink_failures"]),
        max_consecutive_exchange_failures=int(timing["max_consecutive_exchange_failures"]),
        max_consecutive_polymarket_failures=int(timing["max_consecutive_polymarket_failures"]),
        max_consecutive_polymarket_failures_in_grace=int(
            timing["max_consecutive_polymarket_failures_in_grace"]
        ),
        max_consecutive_unusable_polymarket_windows=int(
            timing["max_consecutive_unusable_polymarket_windows"]
        ),
        polymarket_unusable_window_min_quote_coverage_ratio=float(
            timing["polymarket_unusable_window_min_quote_coverage_ratio"]
        ),
        polymarket_rollover_grace_seconds=args.polymarket_rollover_grace_seconds,
        boundary_burst_enabled=bool(timing["boundary_burst_enabled"]),
        boundary_burst_window_seconds=float(timing["boundary_burst_window_seconds"]),
        boundary_burst_interval_seconds=float(timing["boundary_burst_interval_seconds"]),
        chainlink_source_preference=str(args.chainlink_source_preference),
        chainlink_streams_base_url=str(args.chainlink_streams_base_url),
        chainlink_streams_page_url=str(args.chainlink_streams_page_url),
        chainlink_streams_feed_id=str(args.chainlink_streams_feed_id),
    )
    try:
        result = run_phase1_capture(config, logger=logger)
    except Exception as exc:  # pragma: no cover - exercised through integration paths
        logger.exception("phase-1 capture failed")
        print(f"phase-1 capture failed: {exc}", file=sys.stderr)
        return 1

    try:
        admission_summary_path = write_capture_admission_summary(result)
    except Exception as exc:  # pragma: no cover - exercised through integration paths
        logger.exception("phase-1 capture admission summary failed")
        print(f"phase-1 capture admission summary failed: {exc}", file=sys.stderr)
        return 1

    logger.info("summary artifact written to %s", result.summary_path)
    logger.info("admission summary artifact written to %s", admission_summary_path)
    print(result.summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
