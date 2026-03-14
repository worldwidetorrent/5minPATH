import json
from datetime import UTC, datetime
from pathlib import Path

from rtds.collectors.polymarket.metadata import (
    PolymarketMetadataCollector,
    normalize_market_payload,
)

FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "polymarket_metadata"
)


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_normalize_market_payload_prefers_condition_id_and_token_ids() -> None:
    candidate = normalize_market_payload(
        market_payload={
            "id": "12345",
            "conditionId": "0xabc123",
            "question": "Will BTC be up in 5 minutes?",
            "slug": "btc-up-or-down-mar-13-1210pm-utc",
            "startDate": "2026-03-13T12:05:00Z",
            "endDate": "2026-03-13T12:10:00Z",
            "active": True,
            "closed": False,
            "clobTokenIds": ["yes-token", "no-token"],
            "resolutionSource": "Chainlink BTC/USD",
            "description": "Resolves against Chainlink.",
            "category": "Crypto",
        },
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )

    assert candidate.market_id == "0xabc123"
    assert candidate.gamma_market_id == "12345"
    assert candidate.token_yes_id == "yes-token"
    assert candidate.token_no_id == "no-token"
    assert candidate.market_status == "active"
    assert candidate.asset_id == "BTC"


def test_normalize_market_payload_from_fixture_event() -> None:
    event_payload = _load_fixture("btc_5m_event.json")
    market_payload = event_payload["markets"][0]

    candidate = normalize_market_payload(
        market_payload=market_payload,
        event_payload=event_payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )

    assert candidate.event_id == "event-btc-1210"
    assert candidate.market_id == "0xbtc1210"
    assert candidate.market_slug == "btc-up-or-down-mar-13-1210pm-utc"
    assert candidate.token_yes_id == "btc1210-yes"
    assert candidate.token_no_id == "btc1210-no"


def test_fetch_active_candidates_normalizes_nested_event_markets() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_transport(
        endpoint: str,
        params: dict[str, object],
        *,
        timeout: float,
    ) -> tuple[int, dict[str, str], object]:
        calls.append((endpoint, params))
        return (
            200,
            {"ETag": "etag-1"},
            [
                {
                    "id": "event-1",
                    "title": "Bitcoin Up or Down - Mar 13, 12:10PM UTC",
                    "slug": "btc-up-or-down-mar-13-1210pm-utc",
                    "category": "Crypto",
                    "subcategory": "Bitcoin",
                    "active": True,
                    "closed": False,
                    "markets": [
                        {
                            "id": "market-row-1",
                            "conditionId": "0xabc123",
                            "question": "Will Bitcoin close higher?",
                            "slug": "btc-up-or-down-mar-13-1210pm-utc",
                            "startDate": "2026-03-13T12:05:00Z",
                            "endDate": "2026-03-13T12:10:00Z",
                            "active": True,
                            "closed": False,
                            "clobTokenIds": "[\"yes-token\",\"no-token\"]",
                            "resolutionSource": "Chainlink BTC/USD",
                        }
                    ],
                }
            ],
        )

    collector = PolymarketMetadataCollector(
        collector_session_id="test-session",
        transport=fake_transport,
    )
    batch = collector.fetch_active_candidates(limit=50, max_pages=1)

    assert len(batch.raw_messages) == 1
    assert len(batch.candidates) == 1
    assert calls[0][0] == "/events"
    assert calls[0][1]["active"] == "true"
    assert calls[0][1]["closed"] == "false"
    assert batch.raw_messages[0].source_type == "metadata_http"
    assert batch.raw_messages[0].etag == "etag-1"
    assert batch.raw_messages[0].raw_payload[0]["id"] == "event-1"

    candidate = batch.candidates[0]
    assert candidate.event_id == "event-1"
    assert candidate.market_id == "0xabc123"
    assert candidate.market_slug == "btc-up-or-down-mar-13-1210pm-utc"
    assert candidate.token_yes_id == "yes-token"
    assert candidate.token_no_id == "no-token"
    assert candidate.market_open_ts == datetime(2026, 3, 13, 12, 5, tzinfo=UTC)
    assert candidate.market_close_ts == datetime(2026, 3, 13, 12, 10, tzinfo=UTC)


def test_fetch_prelisted_candidates_uses_markets_endpoint() -> None:
    def fake_transport(
        endpoint: str,
        params: dict[str, object],
        *,
        timeout: float,
    ) -> tuple[int, dict[str, str], object]:
        return (
            200,
            {},
            [
                {
                    "id": "future-market",
                    "question": "Will BTC be above 85k at 12:30?",
                    "slug": "btc-up-or-down-mar-13-1230pm-utc",
                    "startDate": "2026-03-13T12:25:00Z",
                    "endDate": "2026-03-13T12:30:00Z",
                    "active": False,
                    "closed": False,
                    "clobTokenIds": ["future-yes", "future-no"],
                }
            ],
        )

    collector = PolymarketMetadataCollector(
        collector_session_id="test-session",
        transport=fake_transport,
    )
    batch = collector.fetch_prelisted_candidates(limit=10, max_pages=1)

    assert len(batch.raw_messages) == 1
    assert len(batch.candidates) == 1
    candidate = batch.candidates[0]
    assert candidate.market_id == "future-market"
    assert candidate.market_status == "inactive"
    assert candidate.closed_flag is False
    assert candidate.active_flag is False
