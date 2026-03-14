"""Polymarket metadata discovery and normalization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any, Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from rtds.core.enums import VenueCode
from rtds.core.ids import validate_polymarket_market_id
from rtds.core.time import ensure_utc, parse_utc, utc_now
from rtds.core.types import UTCDateTime

SCHEMA_VERSION = "0.1.0"
PARSER_VERSION = "0.1.0"
NORMALIZER_VERSION = "0.1.0"
DEFAULT_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_TIMEOUT_SECONDS = 10.0


class MetadataTransport(Protocol):
    """Protocol for fetching Gamma API JSON payloads."""

    def __call__(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        timeout: float,
    ) -> tuple[int, dict[str, str], Any]:
        """Return status, headers, and decoded JSON payload."""


@dataclass(slots=True, frozen=True)
class RawMetadataMessage:
    """Raw metadata response captured for later audit."""

    raw_event_id: str
    venue_id: str
    source_type: str
    endpoint: str
    market_id: str | None
    recv_ts: UTCDateTime
    proc_ts: UTCDateTime
    raw_payload: Any
    payload_format: str
    collector_session_id: str
    parser_version: str
    schema_version: str
    parse_status: str
    http_status: int | None = None
    request_url: str | None = None
    etag: str | None = None
    response_version: str | None = None


@dataclass(slots=True, frozen=True)
class MarketMetadataCandidate:
    """Normalized candidate market metadata, prior to window mapping."""

    venue_id: str
    market_id: str
    recv_ts: UTCDateTime
    proc_ts: UTCDateTime
    raw_event_id: str
    normalizer_version: str
    schema_version: str
    created_ts: UTCDateTime
    event_id: str | None = None
    event_ts: UTCDateTime | None = None
    asset_id: str | None = None
    market_title: str | None = None
    market_question: str | None = None
    market_slug: str | None = None
    market_status: str | None = None
    market_open_ts: UTCDateTime | None = None
    market_close_ts: UTCDateTime | None = None
    active_flag: bool | None = None
    closed_flag: bool | None = None
    archived_flag: bool | None = None
    token_yes_id: str | None = None
    token_no_id: str | None = None
    condition_id: str | None = None
    gamma_market_id: str | None = None
    resolution_source_text: str | None = None
    rules_text: str | None = None
    source_text: str | None = None
    category: str | None = None
    subcategory: str | None = None


@dataclass(slots=True, frozen=True)
class MetadataDiscoveryBatch:
    """Batch of raw messages and normalized candidate rows."""

    raw_messages: list[RawMetadataMessage]
    candidates: list[MarketMetadataCandidate]


def _default_transport(
    endpoint: str,
    params: dict[str, Any],
    *,
    timeout: float,
) -> tuple[int, dict[str, str], Any]:
    """Fetch JSON from the Polymarket Gamma API via stdlib HTTP."""

    query = urlencode({key: value for key, value in params.items() if value is not None})
    request_url = (
        f"{DEFAULT_BASE_URL}{endpoint}?{query}"
        if query
        else f"{DEFAULT_BASE_URL}{endpoint}"
    )
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "testingproject-rtds/0.1.0",
        },
        method="GET",
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
        return response.status, dict(response.headers.items()), payload


def _json_dumps_stable(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _hash_raw_event_id(
    *,
    endpoint: str,
    collector_session_id: str,
    recv_ts: UTCDateTime,
    payload: Any,
) -> str:
    digest = sha256()
    digest.update(VenueCode.POLYMARKET.value.encode("utf-8"))
    digest.update(endpoint.encode("utf-8"))
    digest.update(collector_session_id.encode("utf-8"))
    digest.update(recv_ts.isoformat().encode("utf-8"))
    digest.update(_json_dumps_stable(payload).encode("utf-8"))
    return f"rawmeta:{digest.hexdigest()}"


def _parse_optional_ts(value: Any) -> UTCDateTime | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return parse_utc(value)
    return None


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
    return None


def _coerce_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return []
        try:
            decoded = json.loads(candidate)
        except json.JSONDecodeError:
            return [candidate]
        return decoded if isinstance(decoded, list) else [decoded]
    return [value]


def _infer_asset_id(*, question: str | None, title: str | None, category: str | None) -> str | None:
    haystacks = [question or "", title or "", category or ""]
    combined = " ".join(haystacks).upper()
    if "BTC" in combined or "BITCOIN" in combined:
        return "BTC"
    return None


def _derive_market_status(active_flag: bool | None, closed_flag: bool | None) -> str | None:
    if closed_flag is True:
        return "resolved"
    if active_flag is True:
        return "active"
    if active_flag is False:
        return "inactive"
    return None


def _primary_market_id(market_payload: dict[str, Any]) -> str:
    """Prefer `conditionId`, then fall back to Gamma's `id`."""

    candidate = _coerce_string(market_payload.get("conditionId")) or _coerce_string(
        market_payload.get("id")
    )
    if candidate is None:
        raise ValueError("market payload must contain `conditionId` or `id`")
    return str(validate_polymarket_market_id(candidate))


def _extract_token_ids(market_payload: dict[str, Any]) -> tuple[str | None, str | None]:
    token_ids = [
        _coerce_string(value) for value in _coerce_list(market_payload.get("clobTokenIds"))
    ]
    cleaned = [token_id for token_id in token_ids if token_id is not None]
    yes_token = cleaned[0] if len(cleaned) >= 1 else None
    no_token = cleaned[1] if len(cleaned) >= 2 else None
    return yes_token, no_token


def _pick_metadata_event_ts(
    market_payload: dict[str, Any],
    event_payload: dict[str, Any] | None,
) -> UTCDateTime | None:
    for key in ("updatedAt", "createdAt", "creationDate", "published_at"):
        parsed = _parse_optional_ts(market_payload.get(key))
        if parsed is not None:
            return parsed
    if event_payload is not None:
        for key in ("updatedAt", "createdAt", "creationDate", "published_at"):
            parsed = _parse_optional_ts(event_payload.get(key))
            if parsed is not None:
                return parsed
    return None


def _normalize_market_candidate(
    *,
    market_payload: dict[str, Any],
    event_payload: dict[str, Any] | None,
    raw_message: RawMetadataMessage,
) -> MarketMetadataCandidate:
    market_id = _primary_market_id(market_payload)
    event_id = _coerce_string((event_payload or {}).get("id"))
    question = _coerce_string(market_payload.get("question"))
    event_title = _coerce_string((event_payload or {}).get("title"))
    category = _coerce_string(market_payload.get("category")) or _coerce_string(
        (event_payload or {}).get("category")
    )
    subcategory = _coerce_string((event_payload or {}).get("subcategory"))
    active_flag = _coerce_bool(market_payload.get("active"))
    if active_flag is None:
        active_flag = _coerce_bool((event_payload or {}).get("active"))
    closed_flag = _coerce_bool(market_payload.get("closed"))
    if closed_flag is None:
        closed_flag = _coerce_bool((event_payload or {}).get("closed"))
    archived_flag = _coerce_bool(market_payload.get("archived"))
    if archived_flag is None:
        archived_flag = _coerce_bool((event_payload or {}).get("archived"))
    yes_token_id, no_token_id = _extract_token_ids(market_payload)

    return MarketMetadataCandidate(
        venue_id=VenueCode.POLYMARKET.value,
        market_id=market_id,
        recv_ts=raw_message.recv_ts,
        proc_ts=raw_message.proc_ts,
        raw_event_id=raw_message.raw_event_id,
        normalizer_version=NORMALIZER_VERSION,
        schema_version=SCHEMA_VERSION,
        created_ts=raw_message.proc_ts,
        event_id=event_id,
        event_ts=_pick_metadata_event_ts(market_payload, event_payload),
        asset_id=_infer_asset_id(question=question, title=event_title, category=category),
        market_title=event_title or question,
        market_question=question,
        market_slug=_coerce_string(market_payload.get("slug")) or _coerce_string(
            (event_payload or {}).get("slug")
        ),
        market_status=_derive_market_status(active_flag, closed_flag),
        market_open_ts=_parse_optional_ts(market_payload.get("startDate"))
        or _parse_optional_ts((event_payload or {}).get("startDate")),
        market_close_ts=_parse_optional_ts(market_payload.get("endDate"))
        or _parse_optional_ts((event_payload or {}).get("endDate")),
        active_flag=active_flag,
        closed_flag=closed_flag,
        archived_flag=archived_flag,
        token_yes_id=yes_token_id,
        token_no_id=no_token_id,
        condition_id=_coerce_string(market_payload.get("conditionId")),
        gamma_market_id=_coerce_string(market_payload.get("id")),
        resolution_source_text=_coerce_string(market_payload.get("resolutionSource"))
        or _coerce_string((event_payload or {}).get("resolutionSource")),
        rules_text=_coerce_string(market_payload.get("description"))
        or _coerce_string((event_payload or {}).get("description")),
        source_text=_coerce_string(market_payload.get("source"))
        or _coerce_string((event_payload or {}).get("source")),
        category=category,
        subcategory=subcategory,
    )


class PolymarketMetadataCollector:
    """Collector for raw and normalized Polymarket metadata candidate discovery."""

    def __init__(
        self,
        *,
        collector_session_id: str,
        transport: MetadataTransport | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.collector_session_id = collector_session_id.strip()
        if not self.collector_session_id:
            raise ValueError("collector_session_id must not be empty")
        self._transport = transport or _default_transport
        self._timeout = timeout

    def fetch_active_candidates(
        self,
        *,
        limit: int = 100,
        max_pages: int = 1,
        order: str = "volume_24hr",
        ascending: bool = False,
    ) -> MetadataDiscoveryBatch:
        """Fetch active/open candidate events and their nested market listings."""

        params = {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "order": order,
            "ascending": str(ascending).lower(),
        }
        return self._fetch_event_pages(endpoint="/events", params=params, max_pages=max_pages)

    def fetch_prelisted_candidates(
        self,
        *,
        limit: int = 100,
        max_pages: int = 1,
        order: str = "start_date",
        ascending: bool = True,
    ) -> MetadataDiscoveryBatch:
        """Fetch future/prelisted candidate markets when Gamma exposes them."""

        params = {
            "active": "false",
            "closed": "false",
            "limit": limit,
            "order": order,
            "ascending": str(ascending).lower(),
        }
        return self._fetch_market_pages(endpoint="/markets", params=params, max_pages=max_pages)

    def discover_candidates(
        self,
        *,
        active_limit: int = 100,
        active_pages: int = 1,
        prelisted_limit: int = 100,
        prelisted_pages: int = 1,
    ) -> MetadataDiscoveryBatch:
        """Fetch active and prelisted candidate listings into one batch."""

        active_batch = self.fetch_active_candidates(limit=active_limit, max_pages=active_pages)
        prelisted_batch = self.fetch_prelisted_candidates(
            limit=prelisted_limit,
            max_pages=prelisted_pages,
        )
        return MetadataDiscoveryBatch(
            raw_messages=active_batch.raw_messages + prelisted_batch.raw_messages,
            candidates=active_batch.candidates + prelisted_batch.candidates,
        )

    def _fetch_event_pages(
        self,
        *,
        endpoint: str,
        params: dict[str, Any],
        max_pages: int,
    ) -> MetadataDiscoveryBatch:
        raw_messages: list[RawMetadataMessage] = []
        candidates: list[MarketMetadataCandidate] = []

        for page_payload, raw_message in self._paginate(
            endpoint=endpoint,
            params=params,
            max_pages=max_pages,
        ):
            raw_messages.append(raw_message)
            for event_payload in page_payload:
                for market_payload in _coerce_list(event_payload.get("markets")):
                    if isinstance(market_payload, dict):
                        candidates.append(
                            _normalize_market_candidate(
                                market_payload=market_payload,
                                event_payload=event_payload,
                                raw_message=raw_message,
                            )
                        )

        return MetadataDiscoveryBatch(raw_messages=raw_messages, candidates=candidates)

    def _fetch_market_pages(
        self,
        *,
        endpoint: str,
        params: dict[str, Any],
        max_pages: int,
    ) -> MetadataDiscoveryBatch:
        raw_messages: list[RawMetadataMessage] = []
        candidates: list[MarketMetadataCandidate] = []

        for page_payload, raw_message in self._paginate(
            endpoint=endpoint,
            params=params,
            max_pages=max_pages,
        ):
            raw_messages.append(raw_message)
            for market_payload in page_payload:
                if isinstance(market_payload, dict):
                    candidates.append(
                        _normalize_market_candidate(
                            market_payload=market_payload,
                            event_payload=None,
                            raw_message=raw_message,
                        )
                    )

        return MetadataDiscoveryBatch(raw_messages=raw_messages, candidates=candidates)

    def _paginate(
        self,
        *,
        endpoint: str,
        params: dict[str, Any],
        max_pages: int,
    ) -> list[tuple[list[dict[str, Any]], RawMetadataMessage]]:
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        page_limit = int(params.get("limit", 100))
        if page_limit <= 0:
            raise ValueError("limit must be positive")

        pages: list[tuple[list[dict[str, Any]], RawMetadataMessage]] = []
        offset = 0

        for _ in range(max_pages):
            page_params = dict(params)
            page_params["offset"] = offset
            recv_ts = utc_now()
            status, headers, payload = self._transport(endpoint, page_params, timeout=self._timeout)
            proc_ts = utc_now()

            if not isinstance(payload, list):
                raise ValueError(
                    "Polymarket metadata endpoints are expected to return list payloads"
                )

            raw_message = RawMetadataMessage(
                raw_event_id=_hash_raw_event_id(
                    endpoint=endpoint,
                    collector_session_id=self.collector_session_id,
                    recv_ts=recv_ts,
                    payload=payload,
                ),
                venue_id=VenueCode.POLYMARKET.value,
                source_type="metadata_http",
                endpoint=endpoint,
                market_id=self._page_market_id(payload),
                recv_ts=recv_ts,
                proc_ts=proc_ts,
                raw_payload=payload,
                payload_format="json",
                collector_session_id=self.collector_session_id,
                parser_version=PARSER_VERSION,
                schema_version=SCHEMA_VERSION,
                parse_status="parsed",
                http_status=status,
                request_url=self._request_url(endpoint, page_params),
                etag=headers.get("ETag") or headers.get("Etag"),
                response_version=headers.get("X-API-Version"),
            )
            pages.append((payload, raw_message))

            if len(payload) < page_limit:
                break
            offset += page_limit

        return pages

    def _page_market_id(self, payload: list[dict[str, Any]]) -> str | None:
        for item in payload:
            if not isinstance(item, dict):
                continue
            markets = item.get("markets")
            if isinstance(markets, list) and markets:
                first_market = markets[0]
                if isinstance(first_market, dict):
                    return _coerce_string(first_market.get("conditionId")) or _coerce_string(
                        first_market.get("id")
                    )
            market_id = _coerce_string(item.get("conditionId")) or _coerce_string(item.get("id"))
            if market_id is not None:
                return market_id
        return None

    def _request_url(self, endpoint: str, params: dict[str, Any]) -> str:
        query = urlencode({key: value for key, value in params.items() if value is not None})
        return f"{DEFAULT_BASE_URL}{endpoint}?{query}" if query else f"{DEFAULT_BASE_URL}{endpoint}"


def normalize_market_payload(
    *,
    market_payload: dict[str, Any],
    recv_ts: datetime,
    proc_ts: datetime | None = None,
    raw_event_id: str = "manual",
    event_payload: dict[str, Any] | None = None,
) -> MarketMetadataCandidate:
    """Normalize a single market payload without performing network I/O."""

    normalized_recv_ts = ensure_utc(recv_ts, field_name="recv_ts")
    normalized_proc_ts = (
        ensure_utc(proc_ts, field_name="proc_ts")
        if proc_ts is not None
        else normalized_recv_ts
    )
    raw_message = RawMetadataMessage(
        raw_event_id=raw_event_id,
        venue_id=VenueCode.POLYMARKET.value,
        source_type="metadata_http",
        endpoint="/markets",
        market_id=_coerce_string(market_payload.get("conditionId")) or _coerce_string(
            market_payload.get("id")
        ),
        recv_ts=normalized_recv_ts,
        proc_ts=normalized_proc_ts,
        raw_payload=market_payload,
        payload_format="json",
        collector_session_id="manual",
        parser_version=PARSER_VERSION,
        schema_version=SCHEMA_VERSION,
        parse_status="parsed",
    )
    return _normalize_market_candidate(
        market_payload=market_payload,
        event_payload=event_payload,
        raw_message=raw_message,
    )


__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_TIMEOUT_SECONDS",
    "MetadataDiscoveryBatch",
    "MarketMetadataCandidate",
    "MetadataTransport",
    "NORMALIZER_VERSION",
    "PARSER_VERSION",
    "PolymarketMetadataCollector",
    "RawMetadataMessage",
    "SCHEMA_VERSION",
    "normalize_market_payload",
]
