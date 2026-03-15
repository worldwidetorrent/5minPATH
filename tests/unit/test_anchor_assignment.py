import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from rtds.collectors.polymarket.metadata import normalize_market_payload
from rtds.core.time import parse_utc
from rtds.mapping.anchor_assignment import (
    AMBIGUOUS_STATUS,
    ASSIGNED_STATUS,
    EXACT_BOUNDARY_METHOD,
    FIRST_AFTER_BOUNDARY_METHOD,
    LAST_BEFORE_BOUNDARY_METHOD,
    MISSING_STATUS,
    ChainlinkTick,
    assign_window_reference,
)
from rtds.mapping.market_mapper import WindowMarketMappingRecord, map_candidates_to_windows
from rtds.mapping.window_ids import daily_window_schedule

RAW_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "polymarket_metadata"
)
CHAINLINK_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "normalized_events"
    / "chainlink_ticks"
)


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _mapping_record() -> WindowMarketMappingRecord:
    event_payload = _load_json(RAW_FIXTURE_DIR / "btc_5m_event.json")
    market_payload = event_payload["markets"][0]
    candidate = normalize_market_payload(
        market_payload=market_payload,
        event_payload=event_payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )
    windows = daily_window_schedule(date(2026, 3, 13))
    batch = map_candidates_to_windows(windows, [candidate])
    return next(record for record in batch.records if record.mapping_status == "mapped")


def _load_ticks(name: str) -> list[ChainlinkTick]:
    payload = _load_json(CHAINLINK_FIXTURE_DIR / name)
    assert isinstance(payload, list)
    return [
        ChainlinkTick(
            event_id=str(item["event_id"]),
            event_ts=parse_utc(str(item["event_ts"])),
            price=Decimal(str(item["price"])),
        )
        for item in payload
    ]


def test_anchor_assignment_uses_exact_boundary_ticks_when_present() -> None:
    record = assign_window_reference(_mapping_record(), _load_ticks("clean_boundary_ticks.json"))

    assert record.chainlink_open_anchor_method == EXACT_BOUNDARY_METHOD
    assert record.chainlink_open_anchor_status == ASSIGNED_STATUS
    assert record.chainlink_open_anchor_confidence == "high"
    assert record.chainlink_open_anchor_price == Decimal("84000.10")
    assert record.chainlink_open_anchor_ts == parse_utc("2026-03-13T12:05:00Z")
    assert record.chainlink_open_anchor_source == "chainlink_snapshot_rpc"
    assert record.chainlink_open_anchor_offset_ms == 0
    assert record.chainlink_settle_method == EXACT_BOUNDARY_METHOD
    assert record.chainlink_settle_status == ASSIGNED_STATUS
    assert record.chainlink_settle_confidence == "high"
    assert record.chainlink_settle_price == Decimal("84025.50")
    assert record.chainlink_settle_ts == parse_utc("2026-03-13T12:10:00Z")
    assert record.chainlink_settle_source == "chainlink_snapshot_rpc"
    assert record.resolved_up is True
    assert record.settle_minus_open == Decimal("25.40")
    assert record.outcome_status == "resolved"
    assert record.assignment_status == "complete"
    assert record.assignment_diagnostics == ()


def test_anchor_assignment_prefers_first_after_boundary_over_last_before() -> None:
    record = assign_window_reference(_mapping_record(), _load_ticks("first_after_ticks.json"))

    assert record.chainlink_open_anchor_method == FIRST_AFTER_BOUNDARY_METHOD
    assert record.chainlink_open_anchor_ts == parse_utc("2026-03-13T12:05:00.700Z")
    assert record.chainlink_open_anchor_offset_ms == 700
    assert record.chainlink_open_anchor_confidence == "high"
    assert record.chainlink_settle_method == FIRST_AFTER_BOUNDARY_METHOD
    assert record.chainlink_settle_ts == parse_utc("2026-03-13T12:10:00.900Z")
    assert record.chainlink_settle_offset_ms == 900
    assert record.chainlink_settle_confidence == "high"


def test_anchor_assignment_falls_back_to_last_before_when_no_later_tick_exists() -> None:
    record = assign_window_reference(_mapping_record(), _load_ticks("last_before_ticks.json"))

    assert record.chainlink_open_anchor_method == LAST_BEFORE_BOUNDARY_METHOD
    assert record.chainlink_open_anchor_ts == parse_utc("2026-03-13T12:04:58.500Z")
    assert record.chainlink_open_anchor_offset_ms == -1500
    assert record.chainlink_open_anchor_confidence == "medium"
    assert record.chainlink_settle_method == LAST_BEFORE_BOUNDARY_METHOD
    assert record.chainlink_settle_ts == parse_utc("2026-03-13T12:09:57.500Z")
    assert record.chainlink_settle_offset_ms == -2500
    assert record.chainlink_settle_confidence == "medium"
    assert record.assignment_status == "complete"


def test_anchor_assignment_marks_missing_when_no_tick_is_within_tolerance() -> None:
    record = assign_window_reference(_mapping_record(), _load_ticks("missing_ticks.json"))

    assert record.chainlink_open_anchor_status == MISSING_STATUS
    assert record.chainlink_open_anchor_method == "missing"
    assert record.chainlink_open_anchor_confidence == "none"
    assert record.chainlink_open_anchor_price is None
    assert record.chainlink_settle_status == MISSING_STATUS
    assert record.chainlink_settle_method == "missing"
    assert record.chainlink_settle_confidence == "none"
    assert record.chainlink_settle_price is None
    assert record.resolved_up is None
    assert record.outcome_status == "missing_anchor"
    assert record.assignment_status == "open_and_settle_missing"
    assert "boundary_silence_gap" in record.assignment_diagnostics
    assert "no_tick_within_tolerance" in record.assignment_diagnostics


def test_anchor_assignment_marks_conflicting_ticks_as_ambiguous() -> None:
    mapping_record = _mapping_record()
    record = assign_window_reference(
        mapping_record,
        [
            ChainlinkTick(
                event_id="cl-open-a",
                event_ts=parse_utc("2026-03-13T12:05:00Z"),
                price=Decimal("84000.00"),
            ),
            ChainlinkTick(
                event_id="cl-open-b",
                event_ts=parse_utc("2026-03-13T12:05:00Z"),
                price=Decimal("84001.00"),
            ),
            ChainlinkTick(
                event_id="cl-settle",
                event_ts=parse_utc("2026-03-13T12:10:00Z"),
                price=Decimal("84010.00"),
            ),
        ],
    )

    assert record.chainlink_open_anchor_status == AMBIGUOUS_STATUS
    assert record.chainlink_open_anchor_confidence == "none"
    assert record.chainlink_open_anchor_price is None
    assert record.chainlink_open_anchor_source is None
    assert record.chainlink_settle_status == ASSIGNED_STATUS
    assert record.outcome_status == "ambiguous"
    assert record.assignment_status == "ambiguous"
    assert "conflicting_ticks_at_selected_ts" in record.assignment_diagnostics


def test_anchor_assignment_rejects_non_mapped_records() -> None:
    windows = daily_window_schedule(date(2026, 3, 13))
    batch = map_candidates_to_windows(windows, [])
    unmapped_record = next(
        record for record in batch.records if record.window_id == "btc-5m-20260313T120500Z"
    )

    with pytest.raises(ValueError, match="mapping_status='mapped'"):
        assign_window_reference(unmapped_record, _load_ticks("clean_boundary_ticks.json"))
