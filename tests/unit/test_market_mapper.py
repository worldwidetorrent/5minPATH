import json
from datetime import UTC, date, datetime
from pathlib import Path

from rtds.collectors.polymarket.metadata import normalize_market_payload
from rtds.mapping.market_mapper import assess_candidate, map_candidates_to_windows
from rtds.mapping.window_ids import daily_window_schedule

FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "polymarket_metadata"
)


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _candidate_from_fixture(name: str):
    event_payload = _load_fixture(name)
    market_payload = event_payload["markets"][0]
    return normalize_market_payload(
        market_payload=market_payload,
        event_payload=event_payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )


def test_market_mapper_binds_exactly_one_listing_to_one_window() -> None:
    windows = daily_window_schedule(date(2026, 3, 13))
    candidate = _candidate_from_fixture("btc_5m_event.json")

    batch = map_candidates_to_windows(windows, [candidate])
    mapped = next(record for record in batch.records if record.polymarket_market_id is not None)

    assert mapped.window_id == "btc-5m-20260313T120500Z"
    assert mapped.polymarket_market_id == "0xbtc1210"
    assert mapped.polymarket_event_id == "event-btc-1210"
    assert mapped.clob_token_id_up == "btc1210-yes"
    assert mapped.clob_token_id_down == "btc1210-no"
    assert mapped.mapping_status == "mapped"
    assert mapped.mapping_confidence == "high"


def test_market_mapper_rejects_non_btc_markets() -> None:
    windows = daily_window_schedule(date(2026, 3, 13))
    candidate = _candidate_from_fixture("non_btc_event.json")
    schedule_by_window_id = {window.window_id: window for window in windows}

    assessment = assess_candidate(candidate, schedule_by_window_id=schedule_by_window_id)

    assert assessment.accepted is False
    assert assessment.reason == "asset_mismatch"


def test_market_mapper_rejects_non_5m_markets() -> None:
    windows = daily_window_schedule(date(2026, 3, 13))
    candidate = normalize_market_payload(
        market_payload={
            "conditionId": "0xbtc1215",
            "question": "Will Bitcoin be up or down in the next 15 minutes?",
            "slug": "btc-up-or-down-mar-13-1215pm-utc",
            "startDate": "2026-03-13T12:00:00Z",
            "endDate": "2026-03-13T12:15:00Z",
            "active": True,
            "closed": False,
            "clobTokenIds": ["btc1215-yes", "btc1215-no"],
            "category": "Crypto",
        },
        recv_ts=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
    )

    batch = map_candidates_to_windows(windows, [candidate])
    relevant = next(
        record
        for record in batch.records
        if record.window_id == "btc-5m-20260313T120000Z"
    )
    assessment = batch.assessments[0]

    assert assessment.accepted is False
    assert assessment.reason == "tenor_mismatch"
    assert relevant.mapping_status == "market_missing"


def test_market_mapper_rejects_missing_token_ids() -> None:
    windows = daily_window_schedule(date(2026, 3, 13))
    candidate = normalize_market_payload(
        market_payload={
            "conditionId": "0xbtc1220",
            "question": "Will Bitcoin be up or down in the next 5 minutes?",
            "slug": "btc-up-or-down-mar-13-1220pm-utc",
            "startDate": "2026-03-13T12:15:00Z",
            "endDate": "2026-03-13T12:20:00Z",
            "active": True,
            "closed": False,
            "category": "Crypto",
        },
        recv_ts=datetime(2026, 3, 13, 12, 14, 45, tzinfo=UTC),
    )

    assessment = map_candidates_to_windows(windows, [candidate]).assessments[0]
    assert assessment.accepted is False
    assert assessment.reason == "token_ids_missing"


def test_market_mapper_rejects_duplicate_candidates_for_one_window() -> None:
    windows = daily_window_schedule(date(2026, 3, 13))
    first = _candidate_from_fixture("btc_5m_event.json")
    second = normalize_market_payload(
        market_payload={
            "conditionId": "0xbtc1210b",
            "question": "Will Bitcoin be up or down in the next 5 minutes?",
            "slug": "btc-up-or-down-mar-13-1210pm-utc-duplicate",
            "startDate": "2026-03-13T12:05:00Z",
            "endDate": "2026-03-13T12:10:00Z",
            "active": True,
            "closed": False,
            "clobTokenIds": ["btc1210b-yes", "btc1210b-no"],
            "category": "Crypto",
        },
        recv_ts=datetime(2026, 3, 13, 12, 4, 46, tzinfo=UTC),
        event_payload={
            "id": "event-btc-1210-dup",
            "title": "Bitcoin Up or Down - Mar 13, 12:10PM UTC",
        },
    )

    batch = map_candidates_to_windows(windows, [first, second])
    record = next(
        record
        for record in batch.records
        if record.window_id == "btc-5m-20260313T120500Z"
    )

    assert record.mapping_status == "market_ambiguous"
    assert record.mapping_confidence == "none"
    assert record.polymarket_market_id is None


def test_market_mapper_rejects_misaligned_start_end_times() -> None:
    windows = daily_window_schedule(date(2026, 3, 13))
    candidate = normalize_market_payload(
        market_payload={
            "conditionId": "0xbtc1210bad",
            "question": "Will Bitcoin be up or down in the next 5 minutes?",
            "slug": "btc-up-or-down-mar-13-1210pm-utc-bad",
            "startDate": "2026-03-13T12:06:00Z",
            "endDate": "2026-03-13T12:11:00Z",
            "active": True,
            "closed": False,
            "clobTokenIds": ["btc1210bad-yes", "btc1210bad-no"],
            "category": "Crypto",
        },
        recv_ts=datetime(2026, 3, 13, 12, 5, tzinfo=UTC),
    )

    assessment = map_candidates_to_windows(windows, [candidate]).assessments[0]
    assert assessment.accepted is False
    assert assessment.reason == "window_misaligned"
