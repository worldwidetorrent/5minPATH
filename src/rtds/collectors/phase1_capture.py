"""Phase-1 live capture orchestration."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any
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
from rtds.core.time import format_utc
from rtds.mapping.anchor_assignment import DEFAULT_ORACLE_FEED_ID, ChainlinkTick
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
DEFAULT_CHAINLINK_RPC_URL = "https://arb1.arbitrum.io/rpc"
DEFAULT_CHAINLINK_PROXY_ADDRESS = "0x6ce185860a4963106506C203335A2910413708e9"
DEFAULT_CHAINLINK_FEED_PAGE_URL = "https://data.chain.link/feeds/arbitrum/mainnet/btc-usd"
DEFAULT_BINANCE_BOOK_TICKER_URL = "https://api.binance.us/api/v3/ticker/bookTicker?symbol=BTCUSDT"
DEFAULT_COINBASE_BOOK_URL = "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1"
DEFAULT_KRAKEN_BOOK_URL = "https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=1"
DEFAULT_POLYMARKET_BOOK_URL = "https://clob.polymarket.com/book?token_id={token_id}"
PART_FILE_NAME = "part-00000.jsonl"
USER_AGENT = "testingproject-rtds/0.1.0"


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
    chainlink_rpc_url: str = DEFAULT_CHAINLINK_RPC_URL
    chainlink_proxy_address: str = DEFAULT_CHAINLINK_PROXY_ADDRESS
    chainlink_feed_page_url: str = DEFAULT_CHAINLINK_FEED_PAGE_URL
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
class Phase1CaptureResult:
    """Persisted output summary for one capture session."""

    session_id: str
    capture_date: date
    selected_market_id: str
    selected_market_question: str | None
    duration_seconds: float
    poll_interval_seconds: float
    sample_count: int
    summary_path: Path
    collectors: tuple[CollectorArtifactSet, ...]

    def to_summary_dict(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "capture_date": self.capture_date.isoformat(),
            "selected_market_id": self.selected_market_id,
            "selected_market_question": self.selected_market_question,
            "duration_seconds": self.duration_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "sample_count": self.sample_count,
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

    metadata_raw, metadata_rows, selected_market = _collect_polymarket_metadata(
        config,
        logger=logger,
    )
    chainlink_raw: list[dict[str, object]] = []
    chainlink_rows: list[ChainlinkTick] = []
    exchange_raw: list[dict[str, object]] = []
    exchange_rows: list[ExchangeQuote] = []
    polymarket_raw: list[dict[str, object]] = []
    polymarket_rows: list[PolymarketQuote] = []

    duration_seconds = max(config.duration_seconds, 0.0)
    poll_interval_seconds = max(config.poll_interval_seconds, 0.0)
    deadline = time.monotonic() + duration_seconds if duration_seconds > 0 else None
    sample_count = 0

    while True:
        sample_count += 1
        logger.info("starting capture sample %s", sample_count)
        session_chainlink_raw, session_chainlink_rows = _collect_chainlink_ticks(
            config,
            logger=logger,
        )
        session_exchange_raw, session_exchange_rows = _collect_exchange_quotes(
            config,
            logger=logger,
        )
        session_polymarket_raw, session_polymarket_rows = _collect_polymarket_quote(
            config,
            selected_market=selected_market,
            logger=logger,
        )
        chainlink_raw.extend(session_chainlink_raw)
        chainlink_rows.extend(session_chainlink_rows)
        exchange_raw.extend(session_exchange_raw)
        exchange_rows.extend(session_exchange_rows)
        polymarket_raw.extend(session_polymarket_raw)
        polymarket_rows.extend(session_polymarket_rows)

        if deadline is None:
            break
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            break
        if poll_interval_seconds <= 0:
            break
        sleep_seconds = min(poll_interval_seconds, remaining_seconds)
        logger.info("sleeping %.3f seconds before next capture sample", sleep_seconds)
        time.sleep(sleep_seconds)

    collectors = (
        _write_dataset(
            config,
            collector_name="polymarket_metadata",
            raw_dataset="polymarket_metadata",
            normalized_dataset="market_metadata_events",
            capture_date=capture_date,
            raw_rows=metadata_raw,
            normalized_rows=metadata_rows,
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
    result = Phase1CaptureResult(
        session_id=config.session_id,
        capture_date=capture_date,
        selected_market_id=selected_market.market_id,
        selected_market_question=selected_market.market_question,
        duration_seconds=duration_seconds,
        poll_interval_seconds=poll_interval_seconds,
        sample_count=sample_count,
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
) -> tuple[list[RawMetadataMessage], list[MarketMetadataCandidate], MarketMetadataCandidate]:
    raw_messages, normalized_candidates = _fetch_polymarket_market_pages(config)
    btc_candidates = [
        candidate
        for candidate in normalized_candidates
        if candidate.asset_id == "BTC"
        and candidate.closed_flag is not True
        and candidate.archived_flag is not True
    ]
    if not btc_candidates:
        raise RuntimeError("no BTC Polymarket markets were discovered")

    preferred = [
        candidate
        for candidate in btc_candidates
        if candidate.active_flag is not False
        and candidate.token_yes_id is not None
        and candidate.token_no_id is not None
        and "chainlink" in (candidate.resolution_source_text or "").lower()
    ]
    fallback = [
        candidate
        for candidate in btc_candidates
        if candidate.active_flag is not False
        and candidate.token_yes_id is not None
        and candidate.token_no_id is not None
    ]
    if not fallback:
        raise RuntimeError("no active BTC Polymarket markets with token ids were discovered")
    selected_market = preferred[0] if preferred else fallback[0]
    logger.info(
        "selected Polymarket market %s (%s)",
        selected_market.market_id,
        selected_market.market_question or selected_market.market_title or "unknown",
    )
    return raw_messages, btc_candidates, selected_market


def _fetch_polymarket_market_pages(
    config: Phase1CaptureConfig,
) -> tuple[list[RawMetadataMessage], list[MarketMetadataCandidate]]:
    raw_messages: list[RawMetadataMessage] = []
    candidates: list[MarketMetadataCandidate] = []
    offset = 0

    for _ in range(config.metadata_pages):
        params = {
            "active": "true",
            "closed": "false",
            "limit": config.metadata_limit,
            "offset": offset,
        }
        request_url = "https://gamma-api.polymarket.com/events?" + urlencode(params)
        recv_ts = datetime.now(UTC)
        status, _, payload = _http_json(request_url, timeout_seconds=config.timeout_seconds)
        proc_ts = datetime.now(UTC)
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
            http_status=status,
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


def _collect_chainlink_ticks(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> tuple[list[dict[str, object]], list[ChainlinkTick]]:
    recv_ts = datetime.now(UTC)
    decimals = _fetch_chainlink_decimals(config)
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
    )
    proc_ts = datetime.now(UTC)
    latest_round = _decode_latest_round_data(rpc_response["result"])
    round_id = str(latest_round["round_id"])
    updated_at = int(latest_round["updated_at"])
    event_ts = datetime.fromtimestamp(updated_at, tz=UTC)
    price = Decimal(latest_round["answer"]) / (Decimal(10) ** int(decimals))
    tick = ChainlinkTick(
        event_id=f"chainlink:round:{round_id}",
        event_ts=event_ts,
        price=price,
        recv_ts=recv_ts,
        oracle_feed_id=DEFAULT_ORACLE_FEED_ID,
        round_id=round_id,
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
        "round_id": round_id,
        "decimals": decimals,
        "rpc_payload": rpc_response,
    }
    logger.info("captured Chainlink round %s at %s", round_id, format_utc(event_ts))
    return [raw_row], [tick]


def _collect_exchange_quotes(
    config: Phase1CaptureConfig,
    *,
    logger: logging.Logger,
) -> tuple[list[dict[str, object]], list[ExchangeQuote]]:
    raw_rows: list[dict[str, object]] = []
    normalized_rows: list[ExchangeQuote] = []

    binance_recv_ts = datetime.now(UTC)
    _, _, binance_payload = _http_json(
        config.binance_book_ticker_url,
        timeout_seconds=config.timeout_seconds,
    )
    binance_quote = normalize_binance_quote(
        _shape_binance_payload(binance_payload),
        recv_ts=binance_recv_ts,
    )
    raw_rows.append(
        _raw_capture_row(
            venue_id=VenueCode.BINANCE.value,
            request_url=config.binance_book_ticker_url,
            recv_ts=binance_recv_ts,
            raw_payload=binance_payload,
            raw_event_id=binance_quote.raw_event_id,
        )
    )
    normalized_rows.append(binance_quote)

    coinbase_recv_ts = datetime.now(UTC)
    _, _, coinbase_payload = _http_json(
        config.coinbase_book_url,
        timeout_seconds=config.timeout_seconds,
    )
    coinbase_quote = normalize_coinbase_quote(
        _shape_coinbase_payload(coinbase_payload),
        recv_ts=coinbase_recv_ts,
    )
    raw_rows.append(
        _raw_capture_row(
            venue_id=VenueCode.COINBASE.value,
            request_url=config.coinbase_book_url,
            recv_ts=coinbase_recv_ts,
            raw_payload=coinbase_payload,
            raw_event_id=coinbase_quote.raw_event_id,
        )
    )
    normalized_rows.append(coinbase_quote)

    kraken_recv_ts = datetime.now(UTC)
    _, _, kraken_payload = _http_json(
        config.kraken_book_url,
        timeout_seconds=config.timeout_seconds,
    )
    kraken_quote = normalize_kraken_quote(
        _shape_kraken_payload(kraken_payload, recv_ts=kraken_recv_ts),
        recv_ts=kraken_recv_ts,
    )
    raw_rows.append(
        _raw_capture_row(
            venue_id=VenueCode.KRAKEN.value,
            request_url=config.kraken_book_url,
            recv_ts=kraken_recv_ts,
            raw_payload=kraken_payload,
            raw_event_id=kraken_quote.raw_event_id,
        )
    )
    normalized_rows.append(kraken_quote)

    logger.info("captured %s exchange quote snapshots", len(normalized_rows))
    return raw_rows, normalized_rows


def _collect_polymarket_quote(
    config: Phase1CaptureConfig,
    *,
    selected_market: MarketMetadataCandidate,
    logger: logging.Logger,
) -> tuple[list[dict[str, object]], list[PolymarketQuote]]:
    if selected_market.token_yes_id is None or selected_market.token_no_id is None:
        raise RuntimeError("selected Polymarket market is missing CLOB token ids")

    yes_url = config.polymarket_book_url_template.format(token_id=selected_market.token_yes_id)
    no_url = config.polymarket_book_url_template.format(token_id=selected_market.token_no_id)
    recv_ts = datetime.now(UTC)
    _, _, yes_book = _http_json(yes_url, timeout_seconds=config.timeout_seconds)
    _, _, no_book = _http_json(no_url, timeout_seconds=config.timeout_seconds)
    payload = _build_polymarket_quote_payload(
        market_id=selected_market.market_id,
        yes_token_id=selected_market.token_yes_id,
        no_token_id=selected_market.token_no_id,
        yes_book=yes_book,
        no_book=no_book,
    )
    quote = normalize_polymarket_quote(payload, recv_ts=recv_ts)
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
        "raw_payload": {
            "yes_book": yes_book,
            "no_book": no_book,
            "normalized_payload": payload,
        },
    }
    logger.info("captured Polymarket quote for %s", selected_market.market_id)
    return [raw_row], [quote]


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
) -> dict[str, Any]:
    yes_bid = _best_price_level(yes_book.get("bids"), side="bid", reverse=True)
    yes_ask = _best_price_level(yes_book.get("asks"), side="ask", reverse=False)
    no_bid = _best_price_level(no_book.get("bids"), side="bid", reverse=True)
    no_ask = _best_price_level(no_book.get("asks"), side="ask", reverse=False)
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
    }


def _best_price_level(levels: Any, *, side: str, reverse: bool) -> dict[str, str]:
    if not isinstance(levels, list) or not levels:
        raise RuntimeError(f"Polymarket {side} book is empty")
    chosen = sorted(levels, key=lambda level: Decimal(str(level["price"])), reverse=reverse)[0]
    return {
        "price": str(chosen["price"]),
        "size": str(chosen["size"]),
    }


def _fetch_chainlink_decimals(config: Phase1CaptureConfig) -> int:
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
    )
    return int(response["result"], 16)


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
) -> tuple[int, dict[str, str], Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
        return response.status, dict(response.headers.items()), payload


def _rpc_json(
    url: str,
    payload: dict[str, object],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
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
    with urlopen(request, timeout=timeout_seconds) as response:
        decoded = json.loads(response.read().decode("utf-8"))
    if "error" in decoded:
        raise RuntimeError(f"rpc call failed: {decoded['error']}")
    return decoded


__all__ = [
    "CollectorArtifactSet",
    "DEFAULT_DURATION_SECONDS",
    "DEFAULT_METADATA_LIMIT",
    "DEFAULT_METADATA_PAGES",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DEFAULT_TIMEOUT_SECONDS",
    "Phase1CaptureConfig",
    "Phase1CaptureResult",
    "run_phase1_capture",
]
