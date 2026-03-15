import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from rtds.collectors.polymarket.metadata import normalize_market_payload
from rtds.core.time import parse_utc, window_end
from rtds.mapping.anchor_assignment import ChainlinkTick, assign_window_reference
from rtds.mapping.market_mapper import map_candidates_to_windows
from rtds.mapping.window_ids import daily_window_schedule
from rtds.schemas.window_reference import WindowReferenceRecord
from rtds.storage.writer import WindowReferenceWriter

RAW_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages" / "polymarket_metadata"
)


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((RAW_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _mapped_record():
    event_payload = _load_fixture("btc_5m_event.json")
    candidate = normalize_market_payload(
        market_payload=event_payload["markets"][0],
        event_payload=event_payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )
    batch = map_candidates_to_windows(daily_window_schedule(date(2026, 3, 13)), [candidate])
    return next(record for record in batch.records if record.mapping_status == "mapped")


def _chainlink_ticks() -> list[ChainlinkTick]:
    return [
        ChainlinkTick(
            event_id="cl-open",
            event_ts=parse_utc("2026-03-13T12:05:00Z"),
            price=Decimal("84000.10"),
        ),
        ChainlinkTick(
            event_id="cl-settle",
            event_ts=parse_utc("2026-03-13T12:10:00Z"),
            price=Decimal("84025.50"),
        ),
    ]


def test_window_reference_writer_persists_partitioned_jsonl_rows(
    tmp_path: Path,
) -> None:
    reference_row = assign_window_reference(_mapped_record(), _chainlink_ticks())
    writer = WindowReferenceWriter(tmp_path / "reference")

    result = writer.write([reference_row])

    output_path = (
        tmp_path
        / "reference"
        / "window_reference"
        / "date=2026-03-13"
        / "part-00000.jsonl"
    )
    assert result.row_count == 1
    assert result.partition_dates == ("2026-03-13",)
    assert result.files_written == (output_path,)
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    stored = json.loads(lines[0])
    assert stored["date_utc"] == "2026-03-13"
    assert stored["window_id"] == "btc-5m-20260313T120500Z"
    assert stored["chainlink_open_anchor_price"] == "84000.10"
    assert stored["chainlink_settle_price"] == "84025.50"
    assert stored["chainlink_open_anchor_ts"] == "2026-03-13T12:05:00Z"
    assert stored["chainlink_open_anchor_source"] == "chainlink_snapshot_rpc"
    assert stored["chainlink_settle_source"] == "chainlink_snapshot_rpc"
    assert stored["assignment_diagnostics"] == []

    round_tripped = WindowReferenceRecord.from_storage_dict(stored)
    assert round_tripped == reference_row


def test_window_reference_writer_orders_rows_deterministically_within_partition(
    tmp_path: Path,
) -> None:
    base_row = assign_window_reference(_mapped_record(), _chainlink_ticks())
    earlier_row = replace(
        base_row,
        window_id="btc-5m-20260313T120000Z",
        window_start_ts=parse_utc("2026-03-13T12:00:00Z"),
        window_end_ts=window_end(parse_utc("2026-03-13T12:00:00Z")),
        polymarket_market_id="0xbtc1200",
        polymarket_event_id="event-btc-1200",
        polymarket_slug="btc-up-or-down-mar-13-1200pm-utc",
    )
    writer = WindowReferenceWriter(tmp_path / "reference")

    writer.write([base_row, earlier_row])

    output_path = (
        tmp_path
        / "reference"
        / "window_reference"
        / "date=2026-03-13"
        / "part-00000.jsonl"
    )
    stored_rows = [
        json.loads(line)
        for line in output_path.read_text(encoding="utf-8").splitlines()
    ]

    assert [row["window_id"] for row in stored_rows] == [
        "btc-5m-20260313T120000Z",
        "btc-5m-20260313T120500Z",
    ]


def test_window_reference_writer_rejects_existing_output_when_overwrite_disabled(
    tmp_path: Path,
) -> None:
    writer = WindowReferenceWriter(tmp_path / "reference")
    writer.write([assign_window_reference(_mapped_record(), _chainlink_ticks())])

    with pytest.raises(FileExistsError, match="window-reference output already exists"):
        writer.write(
            [assign_window_reference(_mapped_record(), _chainlink_ticks())],
            overwrite=False,
        )
