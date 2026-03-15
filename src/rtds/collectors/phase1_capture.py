"""Phase-1 live capture orchestration."""

from __future__ import annotations

import json
import logging
import random
import socket
import time
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from rtds.collectors.polymarket.metadata import (
    PARSER_VERSION as POLYMARKET_METADATA_PARSER_VERSION,
)
from rtds.collectors.polymarket.metadata import (
    SCHEMA_VERSION as POLYMARKET_METADATA_SCHEMA_VERSION,
)
from rtds.collectors.polymarket.metadata import (
    MarketMetadataCandidate,
    RawMetadataMessage,
    normalize_market_payload,
)
from rtds.core.enums import VenueCode
from rtds.core.time import format_utc, parse_utc
from rtds.mapping.anchor_assignment import (
    DEFAULT_ORACLE_FEED_ID,
    ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
    ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
    ChainlinkTick,
)
from rtds.mapping.market_mapper import map_candidates_to_windows
from rtds.mapping.window_ids import daily_window_schedule
from rtds.normalizers.exchange import (
    normalize_binance_quote,
    normalize_coinbase_quote,
    normalize_kraken_quote,
)
from rtds.normalizers.polymarket import normalize_polymarket_quote
from rtds.schemas.normalized import ExchangeQuote, PolymarketQuote
from rtds.storage.writer import write_json_file, write_jsonl_rows

DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_METADATA_LIMIT = 500
DEFAULT_METADATA_PAGES = 1
DEFAULT_DURATION_SECONDS = 0.0
DEFAULT_POLL_INTERVAL_SECONDS = 60.0
DEFAULT_METADATA_POLL_INTERVAL_SECONDS = DEFAULT_POLL_INTERVAL_SECONDS
DEFAULT_CHAINLINK_POLL_INTERVAL_SECONDS = DEFAULT_POLL_INTERVAL_SECONDS
DEFAULT_EXCHANGE_POLL_INTERVAL_SECONDS = DEFAULT_POLL_INTERVAL_SECONDS
DEFAULT_POLYMARKET_QUOTE_POLL_INTERVAL_SECONDS = DEFAULT_POLL_INTERVAL_SECONDS
DEFAULT_MAX_FETCH_RETRIES = 2
DEFAULT_BASE_BACKOFF_SECONDS = 0.5
DEFAULT_MAX_BACKOFF_SECONDS = 5.0
DEFAULT_MAX_CONSECUTIVE_SELECTION_FAILURES = 3
DEFAULT_MAX_CONSECUTIVE_CHAINLINK_FAILURES = 3
DEFAULT_MAX_CONSECUTIVE_EXCHANGE_FAILURES = 3
DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES = 3
DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES_IN_GRACE = 5
DEFAULT_POLYMARKET_ROLLOVER_GRACE_SECONDS = 90.0
DEFAULT_BOUNDARY_BURST_WINDOW_SECONDS = 15.0
DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS = 1.0
DEFAULT_CHAINLINK_SOURCE_PREFERENCE = "streams_public"
DEFAULT_CHAINLINK_RPC_URL = "https://arb1.arbitrum.io/rpc"
DEFAULT_CHAINLINK_PROXY_ADDRESS = "0x6ce185860a4963106506C203335A2910413708e9"
DEFAULT_CHAINLINK_FEED_PAGE_URL = "https://data.chain.link/feeds/arbitrum/mainnet/btc-usd"
DEFAULT_CHAINLINK_STREAMS_BASE_URL = "https://data.chain.link"
DEFAULT_CHAINLINK_STREAMS_PAGE_URL = (
    "https://data.chain.link/streams/btc-usd-cexprice-streams"
)
DEFAULT_CHAINLINK_STREAMS_FEED_ID = (
    "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"
)
DEFAULT_BINANCE_BOOK_TICKER_URL = "https://api.binance.us/api/v3/ticker/bookTicker?symbol=BTCUSDT"
DEFAULT_COINBASE_BOOK_URL = "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1"
DEFAULT_KRAKEN_BOOK_URL = "https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=1"
DEFAULT_POLYMARKET_BOOK_URL = "https://clob.polymarket.com/book?token_id={token_id}"
PART_FILE_NAME = "part-00000.jsonl"
USER_AGENT = "testingproject-rtds/0.1.0"
RETRYABLE_HTTP_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
CORE_CAPTURE_SOURCES = ("chainlink", "exchange", "polymarket_quotes")
T = TypeVar("T")


@dataclass(slots=True, frozen=True)
class Phase1CaptureConfig:
    """Effective configuration for one phase-1 capture pass."""

    data_root: Path
    artifacts_root: Path
    logs_root: Path
    temp_root: Path
    session_id: str
    capture_started_at: datetime | None = None
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    metadata_limit: int = DEFAULT_METADATA_LIMIT
    metadata_pages: int = DEFAULT_METADATA_PAGES
    duration_seconds: float = DEFAULT_DURATION_SECONDS
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    metadata_poll_interval_seconds: float = DEFAULT_METADATA_POLL_INTERVAL_SECONDS
    chainlink_poll_interval_seconds: float = DEFAULT_CHAINLINK_POLL_INTERVAL_SECONDS
    exchange_poll_interval_seconds: float = DEFAULT_EXCHANGE_POLL_INTERVAL_SECONDS
    polymarket_quote_poll_interval_seconds: float = (
        DEFAULT_POLYMARKET_QUOTE_POLL_INTERVAL_SECONDS
    )
    max_fetch_retries: int = DEFAULT_MAX_FETCH_RETRIES
    base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS
    max_consecutive_selection_failures: int = DEFAULT_MAX_CONSECUTIVE_SELECTION_FAILURES
    max_consecutive_chainlink_failures: int = DEFAULT_MAX_CONSECUTIVE_CHAINLINK_FAILURES
    max_consecutive_exchange_failures: int = DEFAULT_MAX_CONSECUTIVE_EXCHANGE_FAILURES
    max_consecutive_polymarket_failures: int = DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES
    max_consecutive_polymarket_failures_in_grace: int = (
        DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES_IN_GRACE
    )
    polymarket_rollover_grace_seconds: float = DEFAULT_POLYMARKET_ROLLOVER_GRACE_SECONDS
    boundary_burst_enabled: bool = False
    boundary_burst_window_seconds: float = DEFAULT_BOUNDARY_BURST_WINDOW_SECONDS
    boundary_burst_interval_seconds: float = DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS
    chainlink_source_preference: str = DEFAULT_CHAINLINK_SOURCE_PREFERENCE
    chainlink_rpc_url: str = DEFAULT_CHAINLINK_RPC_URL
    chainlink_proxy_address: str = DEFAULT_CHAINLINK_PROXY_ADDRESS
    chainlink_feed_page_url: str = DEFAULT_CHAINLINK_FEED_PAGE_URL
    chainlink_streams_base_url: str = DEFAULT_CHAINLINK_STREAMS_BASE_URL
    chainlink_streams_page_url: str = DEFAULT_CHAINLINK_STREAMS_PAGE_URL
    chainlink_streams_feed_id: str = DEFAULT_CHAINLINK_STREAMS_FEED_ID
    binance_book_ticker_url: str = DEFAULT_BINANCE_BOOK_TICKER_URL
    coinbase_book_url: str = DEFAULT_COINBASE_BOOK_URL
    kraken_book_url: str = DEFAULT_KRAKEN_BOOK_URL
    polymarket_book_url_template: str = DEFAULT_POLYMARKET_BOOK_URL


@dataclass(slots=True, frozen=True)
class CollectorArtifactSet:
    """One collector's persisted outputs."""

    collector_name: str
    raw_path: Path
    normalized_path: Path
    raw_row_count: int
    normalized_row_count: int


@dataclass(slots=True, frozen=True)
class MetadataSelectionDiagnostics:
    """Exact-family selector diagnostics for one capture session."""

    selected_market_id: str
    selected_market_slug: str | None
    selected_window_id: str
    candidate_count: int
    admitted_count: int
    rejected_count_by_reason: dict[str, int]

    def to_summary_dict(self) -> dict[str, object]:
        return {
            "selected_market_id": self.selected_market_id,
            "selected_market_slug": self.selected_market_slug,
            "selected_window_id": self.selected_window_id,
            "candidate_count": self.candidate_count,
            "admitted_count": self.admitted_count,
            "rejected_count_by_reason": dict(sorted(self.rejected_count_by_reason.items())),
        }


@dataclass(slots=True, frozen=True)
class FetchResult:
    """Outcome of one outbound fetch with bounded retry handling."""

    status: str
    payload: Any | None
    attempts: int
    retries: int
    failure_type: str | None = None
    failure_message: str | None = None
    http_status: int | None = None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class SourceCaptureResult:
    """One source collector result for a single sample."""

    source_name: str
    status: str
    raw_rows: tuple[dict[str, object], ...]
    normalized_rows: tuple[object, ...]
    retries: int = 0
    failure_class: str | None = None
    failure_type: str | None = None
    failure_message: str | None = None
    http_status: int | None = None
    details: dict[str, object] = field(default_factory=dict)

    def to_summary_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "retries": self.retries,
            "raw_row_count": len(self.raw_rows),
            "normalized_row_count": len(self.normalized_rows),
            "failure_class": self.failure_class,
            "failure_type": self.failure_type,
            "failure_message": self.failure_message,
            "http_status": self.http_status,
            "details": dict(self.details),
        }


@dataclass(slots=True, frozen=True)
class SampleDiagnostics:
    """Per-sample health and degradation state."""

    sample_index: int
    sample_started_at: datetime
    sample_status: str
    selected_market_id: str | None
    selected_market_slug: str | None
    selected_window_id: str | None
    family_validation_status: str
    degraded_sources: tuple[str, ...]
    source_results: dict[str, SourceCaptureResult]
    termination_reason: str | None = None

    def to_summary_dict(self) -> dict[str, object]:
        return {
            "sample_index": self.sample_index,
            "sample_started_at": format_utc(self.sample_started_at, timespec="milliseconds"),
            "sample_status": self.sample_status,
            "selected_market_id": self.selected_market_id,
            "selected_market_slug": self.selected_market_slug,
            "selected_window_id": self.selected_window_id,
            "family_validation_status": self.family_validation_status,
            "degraded_sources": list(self.degraded_sources),
            "source_results": {
                source_name: result.to_summary_dict()
                for source_name, result in sorted(self.source_results.items())
            },
            "termination_reason": self.termination_reason,
        }


@dataclass(slots=True, frozen=True)
class SessionDiagnostics:
    """Aggregated session-level resilience diagnostics."""

    degraded_sample_count: int
    failed_sample_count: int
    empty_book_count: int
    retry_count_by_source: dict[str, int]
    retry_exhaustion_count_by_source: dict[str, int]
    source_failure_count_by_source: dict[str, int]
    max_consecutive_missing_by_source: dict[str, int]
    polymarket_failure_count_by_class: dict[str, int]
    polymarket_selector_refresh_count: int
    polymarket_selector_rebind_count: int
    polymarket_rollover_grace_sample_count: int
    termination_reason: str
    sample_diagnostics_path: Path

    def to_summary_dict(self) -> dict[str, object]:
        return {
            "degraded_sample_count": self.degraded_sample_count,
            "failed_sample_count": self.failed_sample_count,
            "empty_book_count": self.empty_book_count,
            "retry_count_by_source": dict(sorted(self.retry_count_by_source.items())),
            "retry_exhaustion_count_by_source": dict(
                sorted(self.retry_exhaustion_count_by_source.items())
            ),
            "source_failure_count_by_source": dict(
                sorted(self.source_failure_count_by_source.items())
            ),
            "max_consecutive_missing_by_source": dict(
                sorted(self.max_consecutive_missing_by_source.items())
            ),
            "polymarket_failure_count_by_class": dict(
                sorted(self.polymarket_failure_count_by_class.items())
            ),
            "polymarket_selector_refresh_count": self.polymarket_selector_refresh_count,
            "polymarket_selector_rebind_count": self.polymarket_selector_rebind_count,
            "polymarket_rollover_grace_sample_count": self.polymarket_rollover_grace_sample_count,
            "termination_reason": self.termination_reason,
            "sample_diagnostics_path": str(self.sample_diagnostics_path),
        }


@dataclass(slots=True, frozen=True)
class PolymarketQuoteResolution:
    """Resolved Polymarket quote capture for one sample."""

    result: SourceCaptureResult
    selected_market: MarketMetadataCandidate
    selected_window_id: str
    metadata_raw_rows: tuple[RawMetadataMessage, ...] = ()
    metadata_rows: tuple[MarketMetadataCandidate, ...] = ()
    admitted_window_ids: dict[str, str] = field(default_factory=dict)
    refresh_attempted: bool = False
    refresh_changed_binding: bool = False


@dataclass(slots=True, frozen=True)
class Phase1CaptureResult:
    """Persisted output summary for one capture session."""

    session_id: str
    capture_date: date
    selected_market_id: str
    selected_market_slug: str | None
    selected_market_question: str | None
    selected_window_id: str
    selector_diagnostics: MetadataSelectionDiagnostics
    duration_seconds: float
    poll_interval_seconds: float
    sample_count: int
    session_diagnostics: SessionDiagnostics
    summary_path: Path
    collectors: tuple[CollectorArtifactSet, ...]

    def to_summary_dict(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "capture_date": self.capture_date.isoformat(),
            "selected_market_id": self.selected_market_id,
            "selected_market_slug": self.selected_market_slug,
            "selected_market_question": self.selected_market_question,
            "selected_window_id": self.selected_window_id,
            "selector_diagnostics": self.selector_diagnostics.to_summary_dict(),
            "duration_seconds": self.duration_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "sample_count": self.sample_count,
            "session_diagnostics": self.session_diagnostics.to_summary_dict(),
            "collectors": [
                {
                    "collector_name": collector.collector_name,
                    "raw_path": str(collector.raw_path),
                    "normalized_path": str(collector.normalized_path),
                    "raw_row_count": collector.raw_row_count,
                    "normalized_row_count": collector.normalized_row_count,
                }
                for collector in self.collectors
            ],
        }


def run_phase1_capture(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> Phase1CaptureResult:
    """Run the sanctioned phase-1 capture contract for one bounded session."""

    capture_started_at = config.capture_started_at or datetime.now(UTC)
    capture_date = capture_started_at.date()

    metadata_raw, metadata_rows, selected_market, selector_diagnostics = (
        _collect_polymarket_metadata(
        config,
        logger=logger,
        )
    )
    active_metadata_rows = list(metadata_rows)
    metadata_output_raw = list(metadata_raw)
    metadata_output_rows = list(metadata_rows)
    _, active_admitted_window_ids, _ = _admitted_family_candidates(active_metadata_rows)
    chainlink_raw: list[dict[str, object]] = []
    chainlink_rows: list[ChainlinkTick] = []
    exchange_raw: list[dict[str, object]] = []
    exchange_rows: list[ExchangeQuote] = []
    polymarket_raw: list[dict[str, object]] = []
    polymarket_rows: list[PolymarketQuote] = []
    sample_diagnostics_rows: list[dict[str, object]] = []

    duration_seconds = max(config.duration_seconds, 0.0)
    poll_interval_seconds = max(config.poll_interval_seconds, 0.0)
    capture_started_monotonic = time.monotonic()
    deadline = (
        capture_started_monotonic + duration_seconds if duration_seconds > 0 else None
    )
    last_capture_monotonic: dict[str, float | None] = {
        source_name: None for source_name in CORE_CAPTURE_SOURCES
    }
    last_metadata_refresh_monotonic = capture_started_monotonic
    sample_count = 0
    degraded_sample_count = 0
    failed_sample_count = 0
    empty_book_count = 0
    retry_count_by_source: Counter[str] = Counter()
    retry_exhaustion_count_by_source: Counter[str] = Counter()
    source_failure_count_by_source: Counter[str] = Counter()
    polymarket_failure_count_by_class: Counter[str] = Counter()
    consecutive_missing_by_source: Counter[str] = Counter()
    max_consecutive_missing_by_source: Counter[str] = Counter()
    polymarket_selector_refresh_count = 0
    polymarket_selector_rebind_count = 0
    polymarket_rollover_grace_sample_count = 0
    termination_reason = "completed"

    while True:
        sample_started_at = datetime.now(UTC)
        now_monotonic = time.monotonic()
        metadata_interval_seconds = max(config.metadata_poll_interval_seconds, 0.0)
        if (
            metadata_interval_seconds > 0
            and now_monotonic - last_metadata_refresh_monotonic >= metadata_interval_seconds
        ):
            try:
                refreshed_metadata_raw, refreshed_metadata_rows, _, _ = (
                    _collect_polymarket_metadata(
                        config,
                        logger=logger,
                    )
                )
            except Exception as exc:
                logger.warning("scheduled metadata refresh failed: %s", exc)
            else:
                metadata_output_raw.extend(refreshed_metadata_raw)
                metadata_output_rows.extend(refreshed_metadata_rows)
                active_metadata_rows = list(refreshed_metadata_rows)
                _, active_admitted_window_ids, _ = _admitted_family_candidates(
                    active_metadata_rows
                )
                last_metadata_refresh_monotonic = now_monotonic
                logger.info(
                    "scheduled metadata refresh admitted %s target-family rows",
                    len(active_metadata_rows),
                )

        sample_market: MarketMetadataCandidate | None = None
        selected_window_id: str | None = None
        family_validation_status = "selected"
        source_results: dict[str, SourceCaptureResult] = {}

        try:
            sample_market = _select_market_for_current_time(
                active_metadata_rows,
                current_ts=sample_started_at,
            )
            selected_window_id = active_admitted_window_ids[sample_market.market_id]
            consecutive_missing_by_source["selection"] = 0
        except Exception as exc:
            family_validation_status = "selection_failed"
            consecutive_missing_by_source["selection"] += 1
            max_consecutive_missing_by_source["selection"] = max(
                max_consecutive_missing_by_source["selection"],
                consecutive_missing_by_source["selection"],
            )
            logger.warning("sample %s could not select an admitted market: %s", sample_count, exc)
            if (
                consecutive_missing_by_source["selection"]
                >= config.max_consecutive_selection_failures
            ):
                termination_reason = "selection_failure_threshold_exceeded"
            sample_result = SampleDiagnostics(
                sample_index=sample_count,
                sample_started_at=sample_started_at,
                sample_status="failed",
                selected_market_id=None,
                selected_market_slug=None,
                selected_window_id=None,
                family_validation_status=family_validation_status,
                degraded_sources=(),
                source_results={},
                termination_reason=(
                    termination_reason if termination_reason != "completed" else None
                ),
            )
            failed_sample_count += 1
            sample_diagnostics_rows.append(sample_result.to_summary_dict())
            if termination_reason != "completed":
                break
            if deadline is None:
                break
            sleep_seconds = _next_capture_sleep_seconds(
                config,
                now_monotonic=now_monotonic,
                current_ts=sample_started_at,
                selected_market=None,
                last_capture_monotonic=last_capture_monotonic,
                deadline=deadline,
            )
            if sleep_seconds is None or sleep_seconds <= 0:
                break
            logger.info("sleeping %.3f seconds before next capture sample", sleep_seconds)
            time.sleep(sleep_seconds)
            continue

        due_by_source = {
            source_name: _source_capture_due(
                config,
                source_name=source_name,
                current_ts=sample_started_at,
                selected_market=sample_market,
                last_capture_monotonic=last_capture_monotonic[source_name],
                now_monotonic=now_monotonic,
            )
            for source_name in CORE_CAPTURE_SOURCES
        }
        if not any(due_by_source.values()):
            if deadline is None:
                break
            sleep_seconds = _next_capture_sleep_seconds(
                config,
                now_monotonic=now_monotonic,
                current_ts=sample_started_at,
                selected_market=sample_market,
                last_capture_monotonic=last_capture_monotonic,
                deadline=deadline,
            )
            if sleep_seconds is None or sleep_seconds <= 0:
                break
            logger.info("sleeping %.3f seconds before next capture sample", sleep_seconds)
            time.sleep(sleep_seconds)
            continue

        sample_count += 1
        logger.info("starting capture sample %s", sample_count)
        if sample_market.market_id != selected_market.market_id:
            logger.info(
                "rolling Polymarket sample market to %s slug=%s window_id=%s",
                sample_market.market_id,
                sample_market.market_slug or "unknown",
                selected_window_id,
            )

        chainlink_result = _collect_due_chainlink_ticks(
            config,
            logger=logger,
            due=due_by_source["chainlink"],
            current_ts=sample_started_at,
            selected_market=sample_market,
        )
        exchange_result = _collect_due_exchange_quotes(
            config,
            logger=logger,
            due=due_by_source["exchange"],
            current_ts=sample_started_at,
            selected_market=sample_market,
        )
        polymarket_resolution = _resolve_due_polymarket_quote_capture(
            config,
            metadata_rows=active_metadata_rows,
            admitted_window_ids=active_admitted_window_ids,
            selected_market=sample_market,
            selected_window_id=selected_window_id,
            current_ts=sample_started_at,
            prior_consecutive_missing=consecutive_missing_by_source["polymarket_quotes"],
            logger=logger,
            due=due_by_source["polymarket_quotes"],
        )
        if polymarket_resolution.metadata_raw_rows:
            metadata_output_raw.extend(polymarket_resolution.metadata_raw_rows)
        if polymarket_resolution.metadata_rows:
            metadata_output_rows.extend(polymarket_resolution.metadata_rows)
            active_metadata_rows = list(polymarket_resolution.metadata_rows)
            active_admitted_window_ids = dict(polymarket_resolution.admitted_window_ids)
        if polymarket_resolution.refresh_attempted:
            polymarket_selector_refresh_count += 1
        if polymarket_resolution.refresh_changed_binding:
            polymarket_selector_rebind_count += 1
            sample_market = polymarket_resolution.selected_market
            selected_window_id = polymarket_resolution.selected_window_id
            logger.info(
                "refreshed Polymarket binding to %s slug=%s window_id=%s",
                sample_market.market_id,
                sample_market.market_slug or "unknown",
                selected_window_id,
            )
        polymarket_result = polymarket_resolution.result
        if polymarket_result.details.get("within_rollover_grace_window"):
            polymarket_rollover_grace_sample_count += 1
        source_results = {
            chainlink_result.source_name: chainlink_result,
            exchange_result.source_name: exchange_result,
            polymarket_result.source_name: polymarket_result,
        }

        for source_name, result in source_results.items():
            retry_count_by_source[source_name] += result.retries
            if result.status not in {"success", "not_due"}:
                source_failure_count_by_source[source_name] += 1
                if source_name == "polymarket_quotes" and result.failure_class is not None:
                    polymarket_failure_count_by_class[result.failure_class] += 1
                if result.status == "retry_exhausted":
                    retry_exhaustion_count_by_source[source_name] += 1
            if result.status == "not_due":
                continue
            missing_output = len(result.normalized_rows) == 0
            if missing_output:
                consecutive_missing_by_source[source_name] += 1
            else:
                consecutive_missing_by_source[source_name] = 0
            max_consecutive_missing_by_source[source_name] = max(
                max_consecutive_missing_by_source[source_name],
                consecutive_missing_by_source[source_name],
            )
            last_capture_monotonic[source_name] = now_monotonic

        if polymarket_result.status == "degraded_empty_book":
            empty_book_count += 1

        chainlink_raw.extend(chainlink_result.raw_rows)
        chainlink_rows.extend(chainlink_result.normalized_rows)
        exchange_raw.extend(exchange_result.raw_rows)
        exchange_rows.extend(exchange_result.normalized_rows)
        polymarket_raw.extend(polymarket_result.raw_rows)
        polymarket_rows.extend(polymarket_result.normalized_rows)

        degraded_sources = tuple(
            source_name
            for source_name, result in source_results.items()
            if result.status not in {"success", "not_due"}
        )
        terminal_source_failure = any(
            result.failure_class in {"terminal_invalid_market", "terminal_schema_failure"}
            for result in source_results.values()
        )
        if degraded_sources:
            if terminal_source_failure or all(
                result.status != "not_due" and len(result.normalized_rows) == 0
                for result in source_results.values()
            ):
                sample_status = "failed"
                failed_sample_count += 1
            else:
                sample_status = "degraded"
                degraded_sample_count += 1
            logger.warning(
                "sample %s completed with degraded sources: %s",
                sample_count,
                ", ".join(degraded_sources),
            )
        else:
            sample_status = "healthy"

        if consecutive_missing_by_source["chainlink"] >= config.max_consecutive_chainlink_failures:
            termination_reason = "chainlink_failure_threshold_exceeded"
        elif (
            consecutive_missing_by_source["exchange"]
            >= config.max_consecutive_exchange_failures
        ):
            termination_reason = "exchange_failure_threshold_exceeded"
        elif polymarket_result.failure_class == "terminal_invalid_market":
            termination_reason = "polymarket_market_invalid"
        elif polymarket_result.failure_class == "terminal_schema_failure":
            termination_reason = "polymarket_schema_failure"
        elif (
            consecutive_missing_by_source["polymarket_quotes"]
            >= _polymarket_failure_threshold(
                config,
                within_rollover_grace_window=bool(
                    polymarket_result.details.get("within_rollover_grace_window")
                ),
            )
        ):
            termination_reason = "polymarket_failure_threshold_exceeded"

        sample_result = SampleDiagnostics(
            sample_index=sample_count,
            sample_started_at=sample_started_at,
            sample_status=sample_status,
            selected_market_id=sample_market.market_id,
            selected_market_slug=sample_market.market_slug,
            selected_window_id=selected_window_id,
            family_validation_status=family_validation_status,
            degraded_sources=degraded_sources,
            source_results=source_results,
            termination_reason=(termination_reason if termination_reason != "completed" else None),
        )
        sample_diagnostics_rows.append(sample_result.to_summary_dict())

        if termination_reason != "completed":
            logger.error("terminating capture early: %s", termination_reason)
            break

        if deadline is None:
            break
        sleep_seconds = _next_capture_sleep_seconds(
            config,
            now_monotonic=now_monotonic,
            current_ts=sample_started_at,
            selected_market=sample_market,
            last_capture_monotonic=last_capture_monotonic,
            deadline=deadline,
        )
        if sleep_seconds is None or sleep_seconds <= 0:
            break
        logger.info("sleeping %.3f seconds before next capture sample", sleep_seconds)
        time.sleep(sleep_seconds)

    collectors = (
        _write_dataset(
            config,
            collector_name="polymarket_metadata",
            raw_dataset="polymarket_metadata",
            normalized_dataset="market_metadata_events",
            capture_date=capture_date,
            raw_rows=metadata_output_raw,
            normalized_rows=metadata_output_rows,
        ),
        _write_dataset(
            config,
            collector_name="chainlink",
            raw_dataset="chainlink",
            normalized_dataset="chainlink_ticks",
            capture_date=capture_date,
            raw_rows=chainlink_raw,
            normalized_rows=chainlink_rows,
        ),
        _write_dataset(
            config,
            collector_name="exchange",
            raw_dataset="exchange",
            normalized_dataset="exchange_quotes",
            capture_date=capture_date,
            raw_rows=exchange_raw,
            normalized_rows=exchange_rows,
        ),
        _write_dataset(
            config,
            collector_name="polymarket_quotes",
            raw_dataset="polymarket_quotes",
            normalized_dataset="polymarket_quotes",
            capture_date=capture_date,
            raw_rows=polymarket_raw,
            normalized_rows=polymarket_rows,
        ),
    )

    summary_path = (
        config.artifacts_root
        / "collect"
        / f"date={capture_date.isoformat()}"
        / f"session={config.session_id}"
        / "summary.json"
    )
    sample_diagnostics_path = summary_path.with_name("sample_diagnostics.jsonl")
    write_jsonl_rows(sample_diagnostics_path, sample_diagnostics_rows)
    session_diagnostics = SessionDiagnostics(
        degraded_sample_count=degraded_sample_count,
        failed_sample_count=failed_sample_count,
        empty_book_count=empty_book_count,
        retry_count_by_source=dict(retry_count_by_source),
        retry_exhaustion_count_by_source=dict(retry_exhaustion_count_by_source),
        source_failure_count_by_source=dict(source_failure_count_by_source),
        max_consecutive_missing_by_source=dict(max_consecutive_missing_by_source),
        polymarket_failure_count_by_class=dict(polymarket_failure_count_by_class),
        polymarket_selector_refresh_count=polymarket_selector_refresh_count,
        polymarket_selector_rebind_count=polymarket_selector_rebind_count,
        polymarket_rollover_grace_sample_count=polymarket_rollover_grace_sample_count,
        termination_reason=termination_reason,
        sample_diagnostics_path=sample_diagnostics_path,
    )
    result = Phase1CaptureResult(
        session_id=config.session_id,
        capture_date=capture_date,
        selected_market_id=selected_market.market_id,
        selected_market_slug=selected_market.market_slug,
        selected_market_question=selected_market.market_question,
        selected_window_id=selector_diagnostics.selected_window_id,
        selector_diagnostics=selector_diagnostics,
        duration_seconds=duration_seconds,
        poll_interval_seconds=poll_interval_seconds,
        sample_count=sample_count,
        session_diagnostics=session_diagnostics,
        summary_path=summary_path,
        collectors=collectors,
    )
    write_json_file(summary_path, result.to_summary_dict())

    for collector in collectors:
        logger.info(
            "%s wrote %s raw rows to %s and %s normalized rows to %s",
            collector.collector_name,
            collector.raw_row_count,
            collector.raw_path,
            collector.normalized_row_count,
            collector.normalized_path,
        )

    return result


def _collect_polymarket_metadata(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> tuple[
    list[RawMetadataMessage],
    list[MarketMetadataCandidate],
    MarketMetadataCandidate,
    MetadataSelectionDiagnostics,
]:
    raw_messages, normalized_candidates = _fetch_polymarket_market_pages(config, logger=logger)
    btc_candidates = [
        candidate
        for candidate in normalized_candidates
        if candidate.asset_id == "BTC"
        and candidate.closed_flag is not True
        and candidate.archived_flag is not True
    ]
    if not btc_candidates:
        raise RuntimeError("no BTC Polymarket markets were discovered")

    selected_market, admitted_candidates, selector_diagnostics = _select_target_family_candidates(
        btc_candidates,
    )
    logger.info(
        "selected Polymarket market %s slug=%s window_id=%s admitted=%s/%s",
        selected_market.market_id,
        selected_market.market_slug or "unknown",
        selector_diagnostics.selected_window_id,
        selector_diagnostics.admitted_count,
        selector_diagnostics.candidate_count,
    )
    return raw_messages, admitted_candidates, selected_market, selector_diagnostics


def _select_target_family_candidates(
    candidates: list[MarketMetadataCandidate],
) -> tuple[MarketMetadataCandidate, list[MarketMetadataCandidate], MetadataSelectionDiagnostics]:
    now_ts = datetime.now(UTC)
    admitted_candidates, admitted_candidates_by_market_id, rejected_count_by_reason = (
        _admitted_family_candidates(candidates)
    )
    if not admitted_candidates:
        reason_summary = ", ".join(
            f"{reason}={count}"
            for reason, count in sorted(rejected_count_by_reason.items())
        )
        raise RuntimeError(
            "no exact BTC 5-minute Up/Down markets were admitted"
            + (f" ({reason_summary})" if reason_summary else "")
        )

    selected_market = _select_market_for_current_time(admitted_candidates, current_ts=now_ts)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=selected_market.market_id,
        selected_market_slug=selected_market.market_slug,
        selected_window_id=admitted_candidates_by_market_id[selected_market.market_id],
        candidate_count=len(candidates),
        admitted_count=len(admitted_candidates),
        rejected_count_by_reason=dict(rejected_count_by_reason),
    )
    return selected_market, admitted_candidates, selector_diagnostics


def _admitted_family_candidates(
    candidates: list[MarketMetadataCandidate],
) -> tuple[list[MarketMetadataCandidate], dict[str, str], Counter[str]]:
    windows = [
        window
        for day in _candidate_schedule_days(candidates)
        for window in daily_window_schedule(day)
    ]
    mapping_batch = map_candidates_to_windows(windows, candidates)
    rejected_count_by_reason = Counter(
        assessment.reason
        for assessment in mapping_batch.assessments
        if not assessment.accepted
    )
    accepted_by_window_id: dict[str, list[str]] = {}
    for assessment in mapping_batch.assessments:
        if assessment.accepted and assessment.window_id is not None:
            accepted_by_window_id.setdefault(assessment.window_id, []).append(assessment.market_id)
    for market_ids in accepted_by_window_id.values():
        if len(market_ids) > 1:
            rejected_count_by_reason["window_ambiguous"] += len(market_ids)

    admitted_candidates_by_market_id = {
        record.polymarket_market_id: record.window_id
        for record in mapping_batch.records
        if record.mapping_status == "mapped" and record.polymarket_market_id is not None
    }
    admitted_candidates = sorted(
        (
            candidate
            for candidate in candidates
            if candidate.market_id in admitted_candidates_by_market_id
        ),
        key=lambda candidate: (
            admitted_candidates_by_market_id[candidate.market_id],
            candidate.market_slug or "",
            candidate.market_id,
        ),
    )
    return admitted_candidates, admitted_candidates_by_market_id, rejected_count_by_reason


def _candidate_schedule_days(candidates: list[MarketMetadataCandidate]) -> list[date]:
    days = {
        candidate.market_open_ts.date()
        for candidate in candidates
        if candidate.market_open_ts is not None
    } | {
        candidate.market_close_ts.date()
        for candidate in candidates
        if candidate.market_close_ts is not None
    }
    return sorted(days)


def _select_market_for_current_time(
    admitted_candidates: list[MarketMetadataCandidate],
    *,
    current_ts: datetime,
) -> MarketMetadataCandidate:
    windows = [
        window
        for day in _candidate_schedule_days(admitted_candidates)
        for window in daily_window_schedule(day)
    ]
    window_by_id = {window.window_id: window for window in windows}
    _, admitted_candidates_by_market_id, _ = _admitted_family_candidates(admitted_candidates)

    def _selection_priority(candidate: MarketMetadataCandidate) -> tuple[int, float, str]:
        window = window_by_id[admitted_candidates_by_market_id[candidate.market_id]]
        if window.window_start_ts <= current_ts < window.window_end_ts:
            return (0, 0.0, window.window_id)
        if window.window_start_ts > current_ts:
            return (
                1,
                (window.window_start_ts - current_ts).total_seconds(),
                window.window_id,
            )
        return (
            2,
            (current_ts - window.window_end_ts).total_seconds(),
            window.window_id,
        )

    return min(admitted_candidates, key=_selection_priority)


def _fetch_polymarket_market_pages(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger | None = None,
) -> tuple[list[RawMetadataMessage], list[MarketMetadataCandidate]]:
    raw_messages: list[RawMetadataMessage] = []
    candidates: list[MarketMetadataCandidate] = []
    offset = 0

    for _ in range(config.metadata_pages):
        params = {
            "tag_slug": "up-or-down",
            "closed": "false",
            "limit": config.metadata_limit,
            "offset": offset,
        }
        request_url = "https://gamma-api.polymarket.com/events?" + urlencode(params)
        recv_ts = datetime.now(UTC)
        response = _http_json(
            request_url,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_fetch_retries,
            base_backoff_seconds=config.base_backoff_seconds,
            max_backoff_seconds=config.max_backoff_seconds,
            source_name="polymarket_metadata",
            logger=logger,
        )
        if response.status != "success":
            raise RuntimeError(
                "Polymarket metadata fetch failed: "
                f"{response.failure_type} {response.failure_message}"
            )
        proc_ts = datetime.now(UTC)
        payload = response.payload
        if not isinstance(payload, list):
            raise RuntimeError("Polymarket events endpoint returned a non-list payload")
        raw_message = RawMetadataMessage(
            raw_event_id=_hash_metadata_page(
                session_id=config.session_id,
                request_url=request_url,
                recv_ts=recv_ts,
                payload=payload,
            ),
            venue_id=VenueCode.POLYMARKET.value,
            source_type="metadata_http",
            endpoint="/events",
            market_id=(
                str(
                    ((payload[0].get("markets") or [{}])[0].get("conditionId"))
                    or ((payload[0].get("markets") or [{}])[0].get("id"))
                    or payload[0].get("id")
                    or ""
                )
                if payload
                else None
            ),
            recv_ts=recv_ts,
            proc_ts=proc_ts,
            raw_payload=payload,
            payload_format="json",
            collector_session_id=config.session_id,
            parser_version=POLYMARKET_METADATA_PARSER_VERSION,
            schema_version=POLYMARKET_METADATA_SCHEMA_VERSION,
            parse_status="parsed",
            http_status=response.http_status or 200,
            request_url=request_url,
        )
        raw_messages.append(raw_message)
        for event_payload in payload:
            if not isinstance(event_payload, dict):
                continue
            markets = event_payload.get("markets")
            if not isinstance(markets, list):
                continue
            for market_payload in markets:
                if not isinstance(market_payload, dict):
                    continue
                candidates.append(
                    normalize_market_payload(
                        market_payload=market_payload,
                        event_payload=event_payload,
                        recv_ts=recv_ts,
                        proc_ts=proc_ts,
                        raw_event_id=raw_message.raw_event_id,
                    )
                )
        if len(payload) < config.metadata_limit:
            break
        offset += config.metadata_limit

    return raw_messages, candidates


def _collect_chainlink_snapshot_tick(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> SourceCaptureResult:
    recv_ts = datetime.now(UTC)
    decimals_result = _fetch_chainlink_decimals(config, logger=logger)
    if decimals_result.status != "success":
        return SourceCaptureResult(
            source_name="chainlink",
            status=decimals_result.status,
            raw_rows=(
                _failure_raw_row(
                    source_name="chainlink",
                    request_url=config.chainlink_rpc_url,
                    recv_ts=recv_ts,
                    status=decimals_result.status,
                    failure_type=decimals_result.failure_type,
                    failure_message=decimals_result.failure_message,
                    details={"rpc_method": "decimals"},
                ),
            ),
            normalized_rows=(),
            retries=decimals_result.retries,
            failure_type=decimals_result.failure_type,
            failure_message=decimals_result.failure_message,
            details={"rpc_method": "decimals"},
        )

    rpc_response = _rpc_json(
        config.chainlink_rpc_url,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": config.chainlink_proxy_address,
                    "data": "0xfeaf968c",
                },
                "latest",
            ],
        },
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_fetch_retries,
        base_backoff_seconds=config.base_backoff_seconds,
        max_backoff_seconds=config.max_backoff_seconds,
        source_name="chainlink",
        logger=logger,
    )
    if rpc_response.status != "success":
        return SourceCaptureResult(
            source_name="chainlink",
            status=rpc_response.status,
            raw_rows=(
                _failure_raw_row(
                    source_name="chainlink",
                    request_url=config.chainlink_rpc_url,
                    recv_ts=recv_ts,
                    status=rpc_response.status,
                    failure_type=rpc_response.failure_type,
                    failure_message=rpc_response.failure_message,
                    details={"rpc_method": "latestRoundData"},
                ),
            ),
            normalized_rows=(),
            retries=decimals_result.retries + rpc_response.retries,
            failure_type=rpc_response.failure_type,
            failure_message=rpc_response.failure_message,
            details={"rpc_method": "latestRoundData"},
        )
    proc_ts = datetime.now(UTC)
    latest_round = _decode_latest_round_data(rpc_response.payload["result"])
    round_id = str(latest_round["round_id"])
    updated_at = int(latest_round["updated_at"])
    event_ts = datetime.fromtimestamp(updated_at, tz=UTC)
    price = Decimal(latest_round["answer"]) / (Decimal(10) ** int(decimals_result.payload))
    tick = ChainlinkTick(
        event_id=f"chainlink:round:{round_id}",
        event_ts=event_ts,
        price=price,
        recv_ts=recv_ts,
        oracle_feed_id=DEFAULT_ORACLE_FEED_ID,
        round_id=round_id,
        oracle_source=ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
    )
    raw_row = {
        "raw_event_id": tick.event_id,
        "source_type": "evm_rpc_latest_round_data",
        "request_url": config.chainlink_rpc_url,
        "feed_page_url": config.chainlink_feed_page_url,
        "proxy_address": config.chainlink_proxy_address,
        "recv_ts": recv_ts,
        "proc_ts": proc_ts,
        "oracle_feed_id": DEFAULT_ORACLE_FEED_ID,
        "oracle_source": ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
        "round_id": round_id,
        "decimals": decimals_result.payload,
        "rpc_payload": rpc_response.payload,
    }
    logger.info("captured Chainlink round %s at %s", round_id, format_utc(event_ts))
    return SourceCaptureResult(
        source_name="chainlink",
        status="success",
        raw_rows=(raw_row,),
        normalized_rows=(tick,),
        retries=decimals_result.retries + rpc_response.retries,
        details={
            "oracle_source": ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
            "fallback_used": False,
        },
    )


def _chainlink_streams_live_reports_url(config: Phase1CaptureConfig) -> str:
    variables = json.dumps(
        {"feedId": config.chainlink_streams_feed_id},
        separators=(",", ":"),
    )
    return (
        f"{config.chainlink_streams_base_url.rstrip('/')}/api/query-timescale?"
        + urlencode(
            {
                "query": "LIVE_STREAM_REPORTS_QUERY",
                "variables": variables,
            }
        )
    )


def _chainlink_stream_event_id(
    *,
    feed_id: str,
    event_ts: datetime,
    price: Decimal,
    bid_price: Decimal | None,
    ask_price: Decimal | None,
) -> str:
    digest = sha256()
    digest.update(feed_id.encode("utf-8"))
    digest.update(format_utc(event_ts, timespec="milliseconds").encode("utf-8"))
    digest.update(str(price).encode("utf-8"))
    digest.update(str(bid_price).encode("utf-8"))
    digest.update(str(ask_price).encode("utf-8"))
    return f"chainlink:stream:{digest.hexdigest()[:16]}"


def _collect_chainlink_stream_tick(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> SourceCaptureResult:
    recv_ts = datetime.now(UTC)
    request_url = _chainlink_streams_live_reports_url(config)
    response = _http_json(
        request_url,
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_fetch_retries,
        base_backoff_seconds=config.base_backoff_seconds,
        max_backoff_seconds=config.max_backoff_seconds,
        source_name="chainlink",
        logger=logger,
    )
    if response.status != "success":
        return SourceCaptureResult(
            source_name="chainlink",
            status=response.status,
            raw_rows=(
                _failure_raw_row(
                    source_name="chainlink",
                    request_url=request_url,
                    recv_ts=recv_ts,
                    status=response.status,
                    failure_type=response.failure_type,
                    failure_message=response.failure_message,
                    details={
                        "oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
                        "query": "LIVE_STREAM_REPORTS_QUERY",
                    },
                ),
            ),
            normalized_rows=(),
            retries=response.retries,
            failure_type=response.failure_type,
            failure_message=response.failure_message,
            http_status=response.http_status,
            details={
                "oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
                "request_url": request_url,
            },
        )

    payload = response.payload
    nodes = (
        payload.get("data", {})
        .get("liveStreamReports", {})
        .get("nodes", [])
        if isinstance(payload, dict)
        else []
    )
    if not isinstance(nodes, list) or not nodes:
        return SourceCaptureResult(
            source_name="chainlink",
            status="degraded",
            raw_rows=(
                _failure_raw_row(
                    source_name="chainlink",
                    request_url=request_url,
                    recv_ts=recv_ts,
                    status="degraded",
                    failure_type="MissingStreamReports",
                    failure_message="public Chainlink stream query returned no report nodes",
                    details={
                        "oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
                        "query": "LIVE_STREAM_REPORTS_QUERY",
                    },
                ),
            ),
            normalized_rows=(),
            retries=response.retries,
            failure_class="stream_reports_missing",
            failure_type="MissingStreamReports",
            failure_message="public Chainlink stream query returned no report nodes",
            http_status=response.http_status,
            details={
                "oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
                "request_url": request_url,
            },
        )

    latest_node = max(
        nodes,
        key=lambda node: str(node.get("validFromTimestamp", "")),
    )
    proc_ts = datetime.now(UTC)
    event_ts = parse_utc(str(latest_node["validFromTimestamp"]))
    price = Decimal(str(latest_node["price"])) / (Decimal(10) ** 18)
    bid_price = (
        None
        if latest_node.get("bid") in (None, "")
        else Decimal(str(latest_node["bid"])) / (Decimal(10) ** 18)
    )
    ask_price = (
        None
        if latest_node.get("ask") in (None, "")
        else Decimal(str(latest_node["ask"])) / (Decimal(10) ** 18)
    )
    event_id = _chainlink_stream_event_id(
        feed_id=config.chainlink_streams_feed_id,
        event_ts=event_ts,
        price=price,
        bid_price=bid_price,
        ask_price=ask_price,
    )
    tick = ChainlinkTick(
        event_id=event_id,
        event_ts=event_ts,
        price=price,
        recv_ts=recv_ts,
        oracle_feed_id=DEFAULT_ORACLE_FEED_ID,
        oracle_source=ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
        bid_price=bid_price,
        ask_price=ask_price,
    )
    raw_row = {
        "raw_event_id": event_id,
        "source_type": "chainlink_stream_public_timescale",
        "request_url": request_url,
        "feed_page_url": config.chainlink_streams_page_url,
        "recv_ts": recv_ts,
        "proc_ts": proc_ts,
        "oracle_feed_id": DEFAULT_ORACLE_FEED_ID,
        "oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
        "stream_feed_id": config.chainlink_streams_feed_id,
        "query_name": "LIVE_STREAM_REPORTS_QUERY",
        "latest_report": latest_node,
        "response_headers": response.headers,
    }
    logger.info(
        "captured Chainlink stream tick at %s via %s",
        format_utc(event_ts, timespec="milliseconds"),
        ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
    )
    return SourceCaptureResult(
        source_name="chainlink",
        status="success",
        raw_rows=(raw_row,),
        normalized_rows=(tick,),
        retries=response.retries,
        details={
            "oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
            "fallback_used": False,
            "request_url": request_url,
            "stream_feed_id": config.chainlink_streams_feed_id,
        },
    )


def _collect_chainlink_ticks(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> SourceCaptureResult:
    if config.chainlink_source_preference == "snapshot_rpc":
        return _collect_chainlink_snapshot_tick(config, logger=logger)

    preferred_result = _collect_chainlink_stream_tick(config, logger=logger)
    if preferred_result.status == "success":
        return preferred_result

    fallback_result = _collect_chainlink_snapshot_tick(config, logger=logger)
    if fallback_result.status != "success":
        combined_raw_rows = preferred_result.raw_rows + fallback_result.raw_rows
        combined_retries = preferred_result.retries + fallback_result.retries
        return SourceCaptureResult(
            source_name="chainlink",
            status=fallback_result.status,
            raw_rows=combined_raw_rows,
            normalized_rows=(),
            retries=combined_retries,
            failure_class=fallback_result.failure_class or preferred_result.failure_class,
            failure_type=fallback_result.failure_type or preferred_result.failure_type,
            failure_message=(
                fallback_result.failure_message or preferred_result.failure_message
            ),
            http_status=fallback_result.http_status or preferred_result.http_status,
            details={
                "oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
                "preferred_oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
                "fallback_oracle_source": ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
                "fallback_used": True,
                "preferred_status": preferred_result.status,
                "fallback_status": fallback_result.status,
            },
        )

    return SourceCaptureResult(
        source_name="chainlink",
        status="success",
        raw_rows=preferred_result.raw_rows + fallback_result.raw_rows,
        normalized_rows=fallback_result.normalized_rows,
        retries=preferred_result.retries + fallback_result.retries,
        details={
            "oracle_source": ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
            "preferred_oracle_source": ORACLE_SOURCE_CHAINLINK_STREAM_PUBLIC_DELAYED,
            "fallback_oracle_source": ORACLE_SOURCE_CHAINLINK_SNAPSHOT_RPC,
            "fallback_used": True,
            "preferred_status": preferred_result.status,
        },
    )


def _collect_due_chainlink_ticks(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
    due: bool,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate,
) -> SourceCaptureResult:
    result = (
        _collect_chainlink_ticks(config, logger=logger)
        if due
        else _not_due_source_result("chainlink")
    )
    return _with_capture_schedule_details(
        result,
        source_name="chainlink",
        config=config,
        current_ts=current_ts,
        selected_market=selected_market,
    )


def _collect_exchange_quotes(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> SourceCaptureResult:
    raw_rows: list[dict[str, object]] = []
    normalized_rows: list[ExchangeQuote] = []
    total_retries = 0
    venue_statuses: dict[str, str] = {}
    failure_type: str | None = None
    failure_message: str | None = None

    for venue_id, request_url, shaper, normalizer in (
        (
            VenueCode.BINANCE.value,
            config.binance_book_ticker_url,
            _shape_binance_payload,
            normalize_binance_quote,
        ),
        (
            VenueCode.COINBASE.value,
            config.coinbase_book_url,
            _shape_coinbase_payload,
            normalize_coinbase_quote,
        ),
        (
            VenueCode.KRAKEN.value,
            config.kraken_book_url,
            lambda payload: _shape_kraken_payload(payload, recv_ts=datetime.now(UTC)),
            normalize_kraken_quote,
        ),
    ):
        recv_ts = datetime.now(UTC)
        fetch_result = _http_json(
            request_url,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_fetch_retries,
            base_backoff_seconds=config.base_backoff_seconds,
            max_backoff_seconds=config.max_backoff_seconds,
            source_name="exchange",
            logger=logger,
        )
        total_retries += fetch_result.retries
        if fetch_result.status != "success":
            venue_statuses[venue_id] = fetch_result.status
            failure_type = fetch_result.failure_type
            failure_message = fetch_result.failure_message
            raw_rows.append(
                _failure_raw_row(
                    source_name="exchange",
                    request_url=request_url,
                    recv_ts=recv_ts,
                    status=fetch_result.status,
                    failure_type=fetch_result.failure_type,
                    failure_message=fetch_result.failure_message,
                    venue_id=venue_id,
                )
            )
            continue
        shaped_payload = (
            _shape_kraken_payload(fetch_result.payload, recv_ts=recv_ts)
            if venue_id == VenueCode.KRAKEN.value
            else shaper(fetch_result.payload)
        )
        quote = normalizer(shaped_payload, recv_ts=recv_ts)
        raw_rows.append(
            _raw_capture_row(
                venue_id=venue_id,
                request_url=request_url,
                recv_ts=recv_ts,
                raw_payload=fetch_result.payload,
                raw_event_id=quote.raw_event_id,
            )
        )
        normalized_rows.append(quote)
        venue_statuses[venue_id] = "success"

    if len(normalized_rows) == 3:
        status = "success"
    elif normalized_rows:
        status = "degraded_partial"
    else:
        status = (
            "retry_exhausted"
            if any(result == "retry_exhausted" for result in venue_statuses.values())
            else "terminal_failure"
        )

    logger.info("captured %s exchange quote snapshots", len(normalized_rows))
    return SourceCaptureResult(
        source_name="exchange",
        status=status,
        raw_rows=tuple(raw_rows),
        normalized_rows=tuple(normalized_rows),
        retries=total_retries,
        failure_type=failure_type,
        failure_message=failure_message,
        details={
            "venue_statuses": dict(sorted(venue_statuses.items())),
            "captured_venues": sorted(quote.venue_id for quote in normalized_rows),
        },
    )


def _collect_due_exchange_quotes(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
    due: bool,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate,
) -> SourceCaptureResult:
    result = (
        _collect_exchange_quotes(config, logger=logger)
        if due
        else _not_due_source_result("exchange")
    )
    return _with_capture_schedule_details(
        result,
        source_name="exchange",
        config=config,
        current_ts=current_ts,
        selected_market=selected_market,
    )


def _collect_polymarket_quote(
    config: Phase1CaptureConfig,
    *,
    selected_market: MarketMetadataCandidate,
    selected_window_id: str,
    current_ts: datetime | None = None,
    logger: logging.Logger,
) -> SourceCaptureResult:
    recv_ts = datetime.now(UTC)
    effective_current_ts = current_ts or recv_ts
    rollover_context = _polymarket_rollover_context(
        selected_market,
        current_ts=effective_current_ts,
        grace_seconds=config.polymarket_rollover_grace_seconds,
    )
    base_details = {
        "selected_market_id": selected_market.market_id,
        "selected_market_slug": selected_market.market_slug,
        "selected_window_id": selected_window_id,
        "metadata_refresh_attempted": False,
        "metadata_refresh_changed_binding": False,
        **rollover_context,
    }
    if selected_market.token_yes_id is None or selected_market.token_no_id is None:
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="terminal_failure",
            raw_rows=(
                _failure_raw_row(
                    source_name="polymarket_quotes",
                    request_url="",
                    recv_ts=recv_ts,
                    status="terminal_failure",
                    failure_type="MissingTokenIds",
                    failure_message="selected Polymarket market is missing CLOB token ids",
                    market_id=selected_market.market_id,
                    details=base_details,
                ),
            ),
            normalized_rows=(),
            failure_class="terminal_invalid_market",
            failure_type="MissingTokenIds",
            failure_message="selected Polymarket market is missing CLOB token ids",
            details=base_details,
        )

    yes_url = config.polymarket_book_url_template.format(token_id=selected_market.token_yes_id)
    no_url = config.polymarket_book_url_template.format(token_id=selected_market.token_no_id)
    yes_book_result = _http_json(
        yes_url,
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_fetch_retries,
        base_backoff_seconds=config.base_backoff_seconds,
        max_backoff_seconds=config.max_backoff_seconds,
        source_name="polymarket_quotes",
        logger=logger,
    )
    no_book_result = _http_json(
        no_url,
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_fetch_retries,
        base_backoff_seconds=config.base_backoff_seconds,
        max_backoff_seconds=config.max_backoff_seconds,
        source_name="polymarket_quotes",
        logger=logger,
    )
    total_retries = yes_book_result.retries + no_book_result.retries
    failed_fetch = next(
        (
            result
            for result in (yes_book_result, no_book_result)
            if result.status != "success"
        ),
        None,
    )
    if failed_fetch is not None:
        failure_status, failure_class = _classify_polymarket_fetch_result(
            failed_fetch,
            within_rollover_grace_window=bool(
                rollover_context["within_rollover_grace_window"]
            ),
        )
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status=failure_status,
            raw_rows=(
                _failure_raw_row(
                    source_name="polymarket_quotes",
                    request_url=",".join((yes_url, no_url)),
                    recv_ts=recv_ts,
                    status=failure_status,
                    failure_type=failed_fetch.failure_type,
                    failure_message=failed_fetch.failure_message,
                    market_id=selected_market.market_id,
                    details=base_details,
                ),
            ),
            normalized_rows=(),
            retries=total_retries,
            failure_class=failure_class,
            failure_type=failed_fetch.failure_type,
            failure_message=failed_fetch.failure_message,
            http_status=failed_fetch.http_status,
            details=base_details,
        )

    try:
        payload, empty_sides = _build_polymarket_quote_payload(
            market_id=selected_market.market_id,
            yes_token_id=selected_market.token_yes_id,
            no_token_id=selected_market.token_no_id,
            yes_book=yes_book_result.payload,
            no_book=no_book_result.payload,
        )
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="terminal_failure",
            raw_rows=(
                _failure_raw_row(
                    source_name="polymarket_quotes",
                    request_url=",".join((yes_url, no_url)),
                    recv_ts=recv_ts,
                    status="terminal_failure",
                    failure_type=type(exc).__name__,
                    failure_message=str(exc),
                    market_id=selected_market.market_id,
                    details=base_details,
                ),
            ),
            normalized_rows=(),
            retries=total_retries,
            failure_class="terminal_schema_failure",
            failure_type=type(exc).__name__,
            failure_message=str(exc),
            details=base_details,
        )
    raw_payload = {
        "yes_book": yes_book_result.payload,
        "no_book": no_book_result.payload,
        "normalized_payload": payload,
    }
    if empty_sides:
        logger.warning(
            "captured degraded Polymarket quote for %s with empty levels: %s",
            selected_market.market_id,
            ", ".join(empty_sides),
        )
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="degraded_empty_book",
            raw_rows=(
                {
                    "raw_event_id": (
                        "polymarket-degraded:"
                        f"{selected_market.market_id}:{int(recv_ts.timestamp())}"
                    ),
                    "venue_id": VenueCode.POLYMARKET.value,
                    "source_type": "clob_book_snapshot",
                    "request_urls": [yes_url, no_url],
                    "recv_ts": recv_ts,
                    "proc_ts": datetime.now(UTC),
                    "market_id": selected_market.market_id,
                    "token_yes_id": selected_market.token_yes_id,
                    "token_no_id": selected_market.token_no_id,
                    "capture_status": "degraded_empty_book",
                    "empty_sides": list(empty_sides),
                    "selected_window_id": selected_window_id,
                    "selected_market_slug": selected_market.market_slug,
                    **rollover_context,
                    "raw_payload": raw_payload,
                },
            ),
            normalized_rows=(),
            retries=total_retries,
            failure_class="degraded_empty_book",
            details={
                "empty_sides": list(empty_sides),
                **base_details,
            },
        )
    try:
        quote = normalize_polymarket_quote(payload, recv_ts=recv_ts)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="terminal_failure",
            raw_rows=(
                _failure_raw_row(
                    source_name="polymarket_quotes",
                    request_url=",".join((yes_url, no_url)),
                    recv_ts=recv_ts,
                    status="terminal_failure",
                    failure_type=type(exc).__name__,
                    failure_message=str(exc),
                    market_id=selected_market.market_id,
                    details=base_details,
                ),
            ),
            normalized_rows=(),
            retries=total_retries,
            failure_class="terminal_schema_failure",
            failure_type=type(exc).__name__,
            failure_message=str(exc),
            details=base_details,
        )
    raw_row = {
        "raw_event_id": quote.raw_event_id,
        "venue_id": VenueCode.POLYMARKET.value,
        "source_type": "clob_book_snapshot",
        "request_urls": [yes_url, no_url],
        "recv_ts": recv_ts,
        "proc_ts": quote.proc_ts,
        "market_id": selected_market.market_id,
        "token_yes_id": selected_market.token_yes_id,
        "token_no_id": selected_market.token_no_id,
        "selected_window_id": selected_window_id,
        "selected_market_slug": selected_market.market_slug,
        **rollover_context,
        "capture_status": "success",
        "raw_payload": raw_payload,
    }
    logger.info("captured Polymarket quote for %s", selected_market.market_id)
    return SourceCaptureResult(
        source_name="polymarket_quotes",
        status="success",
        raw_rows=(raw_row,),
        normalized_rows=(quote,),
        retries=total_retries,
        details=base_details,
    )


def _resolve_polymarket_quote_capture(
    config: Phase1CaptureConfig,
    *,
    metadata_rows: list[MarketMetadataCandidate],
    admitted_window_ids: dict[str, str],
    selected_market: MarketMetadataCandidate,
    selected_window_id: str,
    current_ts: datetime,
    prior_consecutive_missing: int,
    logger: logging.Logger,
) -> PolymarketQuoteResolution:
    result = _collect_polymarket_quote(
        config,
        selected_market=selected_market,
        selected_window_id=selected_window_id,
        current_ts=current_ts,
        logger=logger,
    )
    if not _should_refresh_polymarket_binding(
        result,
        prior_consecutive_missing=prior_consecutive_missing,
    ):
        return PolymarketQuoteResolution(
            result=result,
            selected_market=selected_market,
            selected_window_id=selected_window_id,
        )

    refresh_context = {
        "metadata_refresh_attempted": True,
        "metadata_refresh_reason": result.failure_class or result.status,
    }
    try:
        refreshed_metadata_raw, refreshed_metadata_rows, _, _ = _collect_polymarket_metadata(
            config,
            logger=logger,
        )
        _, refreshed_admitted_window_ids, _ = _admitted_family_candidates(refreshed_metadata_rows)
        refreshed_market = _select_market_for_current_time(
            refreshed_metadata_rows,
            current_ts=current_ts,
        )
        refreshed_window_id = refreshed_admitted_window_ids[refreshed_market.market_id]
    except Exception as exc:
        logger.warning("Polymarket metadata refresh failed: %s", exc)
        return PolymarketQuoteResolution(
            result=_with_capture_details(
                result,
                **refresh_context,
                metadata_refresh_failure_type=type(exc).__name__,
                metadata_refresh_failure_message=str(exc),
            ),
            selected_market=selected_market,
            selected_window_id=selected_window_id,
        )

    binding_changed = (
        refreshed_market.market_id != selected_market.market_id
        or refreshed_window_id != selected_window_id
    )
    retry_result = _collect_polymarket_quote(
        config,
        selected_market=refreshed_market,
        selected_window_id=refreshed_window_id,
        current_ts=current_ts,
        logger=logger,
    )
    retry_result = _finalize_refreshed_polymarket_result(
        retry_result,
        binding_changed=binding_changed,
    )
    retry_result = _with_capture_details(
        retry_result,
        **refresh_context,
        metadata_refresh_changed_binding=binding_changed,
        refreshed_market_id=refreshed_market.market_id,
        refreshed_market_slug=refreshed_market.market_slug,
        refreshed_window_id=refreshed_window_id,
    )
    return PolymarketQuoteResolution(
        result=retry_result,
        selected_market=refreshed_market,
        selected_window_id=refreshed_window_id,
        metadata_raw_rows=tuple(refreshed_metadata_raw),
        metadata_rows=tuple(refreshed_metadata_rows),
        admitted_window_ids=refreshed_admitted_window_ids,
        refresh_attempted=True,
        refresh_changed_binding=binding_changed,
    )


def _resolve_due_polymarket_quote_capture(
    config: Phase1CaptureConfig,
    *,
    metadata_rows: list[MarketMetadataCandidate],
    admitted_window_ids: dict[str, str],
    selected_market: MarketMetadataCandidate,
    selected_window_id: str,
    current_ts: datetime,
    prior_consecutive_missing: int,
    logger: logging.Logger,
    due: bool,
) -> PolymarketQuoteResolution:
    if not due:
        return PolymarketQuoteResolution(
            result=_with_capture_schedule_details(
                _not_due_source_result("polymarket_quotes"),
                source_name="polymarket_quotes",
                config=config,
                current_ts=current_ts,
                selected_market=selected_market,
            ),
            selected_market=selected_market,
            selected_window_id=selected_window_id,
        )
    resolution = _resolve_polymarket_quote_capture(
        config,
        metadata_rows=metadata_rows,
        admitted_window_ids=admitted_window_ids,
        selected_market=selected_market,
        selected_window_id=selected_window_id,
        current_ts=current_ts,
        prior_consecutive_missing=prior_consecutive_missing,
        logger=logger,
    )
    return replace(
        resolution,
        result=_with_capture_schedule_details(
            resolution.result,
            source_name="polymarket_quotes",
            config=config,
            current_ts=current_ts,
            selected_market=resolution.selected_market,
        ),
    )


def _should_refresh_polymarket_binding(
    result: SourceCaptureResult,
    *,
    prior_consecutive_missing: int,
) -> bool:
    if result.status == "success":
        return False
    if result.failure_class in {"selector_refresh_required", "market_binding_stale"}:
        return True
    if (
        result.status == "degraded_empty_book"
        and result.details.get("within_rollover_grace_window")
    ):
        return True
    return prior_consecutive_missing >= 1


def _finalize_refreshed_polymarket_result(
    result: SourceCaptureResult,
    *,
    binding_changed: bool,
) -> SourceCaptureResult:
    within_rollover_grace_window = bool(result.details.get("within_rollover_grace_window"))
    if result.failure_class == "selector_refresh_required":
        if within_rollover_grace_window:
            return replace(
                result,
                status="degraded_retryable_http_404",
                failure_class="retryable_http_404",
            )
        return replace(
            result,
            status="terminal_failure",
            failure_class="terminal_invalid_market",
        )
    if result.failure_class == "market_binding_stale" and not binding_changed:
        return replace(
            result,
            status="terminal_failure",
            failure_class="terminal_invalid_market",
        )
    return result


def _with_capture_details(
    result: SourceCaptureResult,
    **extra_details: object,
) -> SourceCaptureResult:
    merged_details = dict(result.details)
    merged_details.update(extra_details)
    return replace(result, details=merged_details)


def _classify_polymarket_fetch_result(
    fetch_result: FetchResult,
    *,
    within_rollover_grace_window: bool,
) -> tuple[str, str]:
    if fetch_result.http_status == 404:
        if within_rollover_grace_window:
            return "selector_refresh_required", "selector_refresh_required"
        return "terminal_failure", "market_binding_stale"
    if fetch_result.http_status in RETRYABLE_HTTP_STATUS_CODES:
        return fetch_result.status, "retryable_http_5xx"
    if fetch_result.status == "retry_exhausted":
        return fetch_result.status, "retryable_http_5xx"
    return fetch_result.status, "terminal_invalid_market"


def _polymarket_rollover_context(
    selected_market: MarketMetadataCandidate,
    *,
    current_ts: datetime,
    grace_seconds: float,
) -> dict[str, object]:
    seconds_since_open: float | None = None
    seconds_remaining: float | None = None
    if selected_market.market_open_ts is not None:
        seconds_since_open = max(
            0.0,
            (current_ts - selected_market.market_open_ts).total_seconds(),
        )
    if selected_market.market_close_ts is not None:
        seconds_remaining = max(
            0.0,
            (selected_market.market_close_ts - current_ts).total_seconds(),
        )
    within_rollover_grace_window = False
    if seconds_since_open is not None and seconds_since_open <= grace_seconds:
        within_rollover_grace_window = True
    if seconds_remaining is not None and seconds_remaining <= grace_seconds:
        within_rollover_grace_window = True
    return {
        "seconds_since_open": seconds_since_open,
        "seconds_remaining": seconds_remaining,
        "within_rollover_grace_window": within_rollover_grace_window,
    }


def _polymarket_failure_threshold(
    config: Phase1CaptureConfig,
    *,
    within_rollover_grace_window: bool,
) -> int:
    if within_rollover_grace_window:
        return config.max_consecutive_polymarket_failures_in_grace
    return config.max_consecutive_polymarket_failures


def _source_capture_due(
    config: Phase1CaptureConfig,
    *,
    source_name: str,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate,
    last_capture_monotonic: float | None,
    now_monotonic: float,
) -> bool:
    interval_seconds = _source_capture_interval_seconds(
        config,
        source_name=source_name,
        current_ts=current_ts,
        selected_market=selected_market,
    )
    if interval_seconds <= 0:
        return last_capture_monotonic is None
    if last_capture_monotonic is None:
        return True
    return (now_monotonic - last_capture_monotonic) >= interval_seconds


def _next_capture_sleep_seconds(
    config: Phase1CaptureConfig,
    *,
    now_monotonic: float,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate | None,
    last_capture_monotonic: dict[str, float | None],
    deadline: float | None,
) -> float | None:
    sleep_candidates: list[float] = []
    if deadline is not None:
        remaining_seconds = deadline - now_monotonic
        if remaining_seconds <= 0:
            return None
        sleep_candidates.append(remaining_seconds)

    for source_name in CORE_CAPTURE_SOURCES:
        interval_seconds = _source_capture_interval_seconds(
            config,
            source_name=source_name,
            current_ts=current_ts,
            selected_market=selected_market,
        )
        last_capture = last_capture_monotonic.get(source_name)
        if last_capture is None:
            sleep_candidates.append(0.0)
            continue
        sleep_candidates.append(max(0.0, interval_seconds - (now_monotonic - last_capture)))

    transition_seconds = _boundary_burst_transition_seconds(
        config,
        current_ts=current_ts,
        selected_market=selected_market,
    )
    if transition_seconds is not None:
        sleep_candidates.append(transition_seconds)

    if not sleep_candidates:
        return None
    return min(sleep_candidates)


def _source_capture_interval_seconds(
    config: Phase1CaptureConfig,
    *,
    source_name: str,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate | None,
) -> float:
    interval_by_source = {
        "metadata": max(config.metadata_poll_interval_seconds, 0.0),
        "chainlink": max(config.chainlink_poll_interval_seconds, 0.0),
        "exchange": max(config.exchange_poll_interval_seconds, 0.0),
        "polymarket_quotes": max(config.polymarket_quote_poll_interval_seconds, 0.0),
    }
    interval_seconds = interval_by_source[source_name]
    if (
        source_name in CORE_CAPTURE_SOURCES
        and config.boundary_burst_enabled
        and _boundary_burst_active(
            config,
            current_ts=current_ts,
            selected_market=selected_market,
        )
    ):
        return min(interval_seconds, max(config.boundary_burst_interval_seconds, 0.0))
    return interval_seconds


def _boundary_burst_active(
    config: Phase1CaptureConfig,
    *,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate | None,
) -> bool:
    if not config.boundary_burst_enabled or selected_market is None:
        return False
    burst_window_seconds = max(config.boundary_burst_window_seconds, 0.0)
    if burst_window_seconds <= 0:
        return False
    open_seconds, close_seconds = _boundary_signed_offsets_seconds(
        selected_market,
        current_ts=current_ts,
    )
    return any(
        seconds is not None and abs(seconds) <= burst_window_seconds
        for seconds in (open_seconds, close_seconds)
    )


def _boundary_burst_transition_seconds(
    config: Phase1CaptureConfig,
    *,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate | None,
) -> float | None:
    if not config.boundary_burst_enabled or selected_market is None:
        return None
    burst_window_seconds = max(config.boundary_burst_window_seconds, 0.0)
    if burst_window_seconds <= 0:
        return None
    transition_candidates: list[float] = []
    for seconds in _boundary_signed_offsets_seconds(selected_market, current_ts=current_ts):
        if seconds is None:
            continue
        if seconds > burst_window_seconds:
            transition_candidates.append(seconds - burst_window_seconds)
        elif seconds >= 0:
            transition_candidates.append(seconds)
    if not transition_candidates:
        return None
    return min(transition_candidates)


def _boundary_signed_offsets_seconds(
    selected_market: MarketMetadataCandidate,
    *,
    current_ts: datetime,
) -> tuple[float | None, float | None]:
    open_seconds = None
    close_seconds = None
    if selected_market.market_open_ts is not None:
        open_seconds = (selected_market.market_open_ts - current_ts).total_seconds()
    if selected_market.market_close_ts is not None:
        close_seconds = (selected_market.market_close_ts - current_ts).total_seconds()
    return open_seconds, close_seconds


def _with_capture_schedule_details(
    result: SourceCaptureResult,
    *,
    source_name: str,
    config: Phase1CaptureConfig,
    current_ts: datetime,
    selected_market: MarketMetadataCandidate,
) -> SourceCaptureResult:
    interval_seconds = _source_capture_interval_seconds(
        config,
        source_name=source_name,
        current_ts=current_ts,
        selected_market=selected_market,
    )
    return _with_capture_details(
        result,
        capture_interval_seconds=interval_seconds,
        boundary_burst_active=_boundary_burst_active(
            config,
            current_ts=current_ts,
            selected_market=selected_market,
        ),
    )


def _not_due_source_result(source_name: str) -> SourceCaptureResult:
    return SourceCaptureResult(
        source_name=source_name,
        status="not_due",
        raw_rows=(),
        normalized_rows=(),
    )


def _write_dataset(
    config: Phase1CaptureConfig,
    *,
    collector_name: str,
    raw_dataset: str,
    normalized_dataset: str,
    capture_date: date,
    raw_rows: list[object],
    normalized_rows: list[object],
) -> CollectorArtifactSet:
    raw_path = _dataset_path(
        config.data_root / "raw",
        dataset_name=raw_dataset,
        capture_date=capture_date,
        session_id=config.session_id,
    )
    normalized_path = _dataset_path(
        config.data_root / "normalized",
        dataset_name=normalized_dataset,
        capture_date=capture_date,
        session_id=config.session_id,
    )
    write_jsonl_rows(raw_path, raw_rows)
    write_jsonl_rows(normalized_path, normalized_rows)
    return CollectorArtifactSet(
        collector_name=collector_name,
        raw_path=raw_path,
        normalized_path=normalized_path,
        raw_row_count=len(raw_rows),
        normalized_row_count=len(normalized_rows),
    )


def _dataset_path(
    root: Path,
    *,
    dataset_name: str,
    capture_date: date,
    session_id: str,
) -> Path:
    return (
        root
        / dataset_name
        / f"date={capture_date.isoformat()}"
        / f"session={session_id}"
        / PART_FILE_NAME
    )


def _raw_capture_row(
    *,
    venue_id: str,
    request_url: str,
    recv_ts: datetime,
    raw_payload: Any,
    raw_event_id: str,
) -> dict[str, object]:
    return {
        "raw_event_id": raw_event_id,
        "venue_id": venue_id,
        "source_type": "rest_snapshot",
        "request_url": request_url,
        "recv_ts": recv_ts,
        "proc_ts": datetime.now(UTC),
        "raw_payload": raw_payload,
    }


def _shape_binance_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "s": payload["symbol"],
        "b": payload["bidPrice"],
        "a": payload["askPrice"],
        "B": payload["bidQty"],
        "A": payload["askQty"],
        "e": "bookTicker",
    }


def _shape_coinbase_payload(payload: dict[str, Any]) -> dict[str, Any]:
    best_bid = payload["bids"][0]
    best_ask = payload["asks"][0]
    return {
        "channel": "ticker",
        "timestamp": payload.get("time"),
        "sequence_num": payload.get("sequence"),
        "events": [
            {
                "type": "ticker",
                "tickers": [
                    {
                        "product_id": "BTC-USD",
                        "best_bid": best_bid[0],
                        "best_bid_quantity": best_bid[1],
                        "best_ask": best_ask[0],
                        "best_ask_quantity": best_ask[1],
                        "time": payload.get("time"),
                        "sequence_num": payload.get("sequence"),
                    }
                ],
            }
        ],
    }


def _shape_kraken_payload(payload: dict[str, Any], *, recv_ts: datetime) -> dict[str, Any]:
    result = payload["result"]["XXBTZUSD"]
    best_bid = result["bids"][0]
    best_ask = result["asks"][0]
    return {
        "channel": "book",
        "data": [
            {
                "symbol": "BTC/USD",
                "timestamp": format_utc(recv_ts),
                "sequence": str(max(int(best_bid[2]), int(best_ask[2]))),
                "bids": [{"price": best_bid[0], "qty": best_bid[1]}],
                "asks": [{"price": best_ask[0], "qty": best_ask[1]}],
            }
        ],
    }


def _build_polymarket_quote_payload(
    *,
    market_id: str,
    yes_token_id: str,
    no_token_id: str,
    yes_book: dict[str, Any],
    no_book: dict[str, Any],
) -> tuple[dict[str, Any], tuple[str, ...]]:
    empty_sides: list[str] = []
    yes_bid = _best_price_level(yes_book.get("bids"), side="bid", reverse=True)
    if yes_bid is None:
        empty_sides.append("up_bid")
    yes_ask = _best_price_level(yes_book.get("asks"), side="ask", reverse=False)
    if yes_ask is None:
        empty_sides.append("up_ask")
    no_bid = _best_price_level(no_book.get("bids"), side="bid", reverse=True)
    if no_bid is None:
        empty_sides.append("down_bid")
    no_ask = _best_price_level(no_book.get("asks"), side="ask", reverse=False)
    if no_ask is None:
        empty_sides.append("down_ask")
    event_ts = datetime.fromtimestamp(
        max(int(yes_book["timestamp"]), int(no_book["timestamp"])) / 1000,
        tz=UTC,
    )
    return {
        "market_id": market_id,
        "asset_id": "BTC",
        "event_ts": format_utc(event_ts, timespec="milliseconds"),
        "token_yes_id": yes_token_id,
        "token_no_id": no_token_id,
        "quote_type": "orderbook_top",
        "sequence_id": f"{yes_book.get('hash', '')}:{no_book.get('hash', '')}",
        "outcomes": {
            "up": {
                "bid": yes_bid,
                "ask": yes_ask,
            },
            "down": {
                "bid": no_bid,
                "ask": no_ask,
            },
        },
    }, tuple(empty_sides)


def _best_price_level(levels: Any, *, side: str, reverse: bool) -> dict[str, str] | None:
    if not isinstance(levels, list) or not levels:
        return None
    chosen = sorted(levels, key=lambda level: Decimal(str(level["price"])), reverse=reverse)[0]
    return {
        "price": str(chosen["price"]),
        "size": str(chosen["size"]),
    }


def _fetch_chainlink_decimals(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> FetchResult:
    response = _rpc_json(
        config.chainlink_rpc_url,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": config.chainlink_proxy_address,
                    "data": "0x313ce567",
                },
                "latest",
            ],
        },
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_fetch_retries,
        base_backoff_seconds=config.base_backoff_seconds,
        max_backoff_seconds=config.max_backoff_seconds,
        source_name="chainlink",
        logger=logger,
    )
    if response.status != "success":
        return response
    return FetchResult(
        status="success",
        payload=int(response.payload["result"], 16),
        attempts=response.attempts,
        retries=response.retries,
        headers=response.headers,
    )


def _hash_metadata_page(
    *,
    session_id: str,
    request_url: str,
    recv_ts: datetime,
    payload: Any,
) -> str:
    digest = sha256()
    digest.update(session_id.encode("utf-8"))
    digest.update(request_url.encode("utf-8"))
    digest.update(format_utc(recv_ts, timespec="milliseconds").encode("utf-8"))
    digest.update(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode())
    return f"rawmeta:{digest.hexdigest()}"


def _decode_latest_round_data(result_hex: str) -> dict[str, int]:
    if not isinstance(result_hex, str) or not result_hex.startswith("0x"):
        raise RuntimeError("latestRoundData response must be a 0x-prefixed hex string")
    words = [result_hex[2 + index * 64 : 2 + (index + 1) * 64] for index in range(5)]
    if any(len(word) != 64 for word in words):
        raise RuntimeError("latestRoundData response was not the expected length")
    answer = int(words[1], 16)
    if answer >= 2**255:
        answer -= 2**256
    return {
        "round_id": int(words[0], 16),
        "answer": answer,
        "started_at": int(words[2], 16),
        "updated_at": int(words[3], 16),
        "answered_in_round": int(words[4], 16),
    }


def _http_json(
    url: str,
    *,
    timeout_seconds: float,
    max_retries: int,
    base_backoff_seconds: float,
    max_backoff_seconds: float,
    source_name: str,
    logger: logging.Logger | None = None,
) -> FetchResult:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )
    return _run_with_retries(
        source_name=source_name,
        operation=lambda: _execute_http_request(request, timeout_seconds=timeout_seconds),
        max_retries=max_retries,
        base_backoff_seconds=base_backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
        logger=logger,
    )


def _rpc_json(
    url: str,
    payload: dict[str, object],
    *,
    timeout_seconds: float,
    max_retries: int,
    base_backoff_seconds: float,
    max_backoff_seconds: float,
    source_name: str,
    logger: logging.Logger | None = None,
) -> FetchResult:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    return _run_with_retries(
        source_name=source_name,
        operation=lambda: _execute_rpc_request(request, timeout_seconds=timeout_seconds),
        max_retries=max_retries,
        base_backoff_seconds=base_backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
        logger=logger,
    )


def _execute_http_request(
    request: Request,
    *,
    timeout_seconds: float,
) -> tuple[int, dict[str, str], Any]:
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
        return response.status, dict(response.headers.items()), payload


def _execute_rpc_request(
    request: Request,
    *,
    timeout_seconds: float,
) -> tuple[int, dict[str, str], Any]:
    with urlopen(request, timeout=timeout_seconds) as response:
        decoded = json.loads(response.read().decode("utf-8"))
        if isinstance(decoded, dict) and decoded.get("error"):
            raise RuntimeError(f"rpc call failed: {decoded['error']}")
        return response.status, dict(response.headers.items()), decoded


def _run_with_retries(
    *,
    source_name: str,
    operation: Callable[[], tuple[int, dict[str, str], T]],
    max_retries: int,
    base_backoff_seconds: float,
    max_backoff_seconds: float,
    logger: logging.Logger | None = None,
) -> FetchResult:
    attempts = 0
    while True:
        attempts += 1
        try:
            status, headers, payload = operation()
            return FetchResult(
                status="success",
                payload=payload,
                attempts=attempts,
                retries=max(0, attempts - 1),
                http_status=status,
                headers=headers,
            )
        except Exception as exc:  # pragma: no cover - exercised by caller tests
            retryable, http_status = _classify_retryable_exception(exc)
            failure_type = type(exc).__name__
            failure_message = str(exc)
            if not retryable:
                return FetchResult(
                    status="terminal_failure",
                    payload=None,
                    attempts=attempts,
                    retries=max(0, attempts - 1),
                    failure_type=failure_type,
                    failure_message=failure_message,
                    http_status=http_status,
                )
            if attempts > max_retries:
                return FetchResult(
                    status="retry_exhausted",
                    payload=None,
                    attempts=attempts,
                    retries=max(0, attempts - 1),
                    failure_type=failure_type,
                    failure_message=failure_message,
                    http_status=http_status,
                )
            backoff_seconds = _retry_backoff_seconds(
                attempt=attempts,
                base_backoff_seconds=base_backoff_seconds,
                max_backoff_seconds=max_backoff_seconds,
            )
            if logger is not None:
                logger.warning(
                    "%s fetch attempt %s/%s failed with %s: %s; retrying in %.3fs",
                    source_name,
                    attempts,
                    max_retries + 1,
                    failure_type,
                    failure_message,
                    backoff_seconds,
                )
            time.sleep(backoff_seconds)


def _classify_retryable_exception(exc: Exception) -> tuple[bool, int | None]:
    if isinstance(exc, HTTPError):
        return exc.code in RETRYABLE_HTTP_STATUS_CODES, exc.code
    if isinstance(exc, URLError):
        return True, None
    if isinstance(exc, (TimeoutError, socket.timeout, ConnectionResetError, OSError)):
        return True, None
    return False, None


def _retry_backoff_seconds(
    *,
    attempt: int,
    base_backoff_seconds: float,
    max_backoff_seconds: float,
) -> float:
    capped_backoff = min(max_backoff_seconds, base_backoff_seconds * (2 ** max(0, attempt - 1)))
    return capped_backoff + random.uniform(0.0, min(0.5, capped_backoff))


def _failure_raw_row(
    *,
    source_name: str,
    request_url: str,
    recv_ts: datetime,
    status: str,
    failure_type: str | None,
    failure_message: str | None,
    venue_id: str | None = None,
    market_id: str | None = None,
    details: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "raw_event_id": f"{source_name}:{status}:{int(recv_ts.timestamp() * 1000)}",
        "venue_id": venue_id,
        "source_type": "capture_failure",
        "request_url": request_url,
        "recv_ts": recv_ts,
        "proc_ts": datetime.now(UTC),
        "market_id": market_id,
        "capture_status": status,
        "failure_type": failure_type,
        "failure_message": failure_message,
        "details": details or {},
    }


__all__ = [
    "CollectorArtifactSet",
    "DEFAULT_BASE_BACKOFF_SECONDS",
    "DEFAULT_DURATION_SECONDS",
    "DEFAULT_MAX_BACKOFF_SECONDS",
    "DEFAULT_MAX_CONSECUTIVE_CHAINLINK_FAILURES",
    "DEFAULT_MAX_CONSECUTIVE_EXCHANGE_FAILURES",
    "DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES",
    "DEFAULT_MAX_CONSECUTIVE_POLYMARKET_FAILURES_IN_GRACE",
    "DEFAULT_MAX_CONSECUTIVE_SELECTION_FAILURES",
    "DEFAULT_MAX_FETCH_RETRIES",
    "DEFAULT_METADATA_LIMIT",
    "DEFAULT_METADATA_PAGES",
    "DEFAULT_POLYMARKET_ROLLOVER_GRACE_SECONDS",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DEFAULT_TIMEOUT_SECONDS",
    "FetchResult",
    "MetadataSelectionDiagnostics",
    "Phase1CaptureConfig",
    "Phase1CaptureResult",
    "PolymarketQuoteResolution",
    "SampleDiagnostics",
    "SessionDiagnostics",
    "SourceCaptureResult",
    "run_phase1_capture",
]
