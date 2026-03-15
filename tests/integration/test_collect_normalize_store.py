from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from rtds.collectors.phase1_capture import (
    MetadataSelectionDiagnostics,
    Phase1CaptureConfig,
    SourceCaptureResult,
    run_phase1_capture,
)
from rtds.collectors.polymarket.metadata import RawMetadataMessage, normalize_market_payload
from rtds.mapping.anchor_assignment import ChainlinkTick
from rtds.normalizers.exchange import (
    normalize_binance_quote,
    normalize_coinbase_quote,
    normalize_kraken_quote,
)
from rtds.normalizers.polymarket import normalize_polymarket_quote

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "raw_messages"


def test_phase1_capture_writes_frozen_raw_and_normalized_layouts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metadata_payload = json.loads(
        (FIXTURE_ROOT / "polymarket_metadata" / "btc_5m_event.json").read_text(encoding="utf-8")
    )
    market_candidate = normalize_market_payload(
        market_payload=metadata_payload["markets"][0],
        event_payload=metadata_payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )
    metadata_raw = RawMetadataMessage(
        raw_event_id="rawmeta:test",
        venue_id="polymarket",
        source_type="metadata_http",
        endpoint="/events",
        market_id=market_candidate.market_id,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
        proc_ts=datetime(2026, 3, 13, 12, 4, 46, tzinfo=UTC),
        raw_payload=[metadata_payload],
        payload_format="json",
        collector_session_id="test-session",
        parser_version="0.1.0",
        schema_version="0.1.0",
        parse_status="parsed",
        http_status=200,
        request_url="https://gamma-api.polymarket.com/events",
    )
    chainlink_tick = ChainlinkTick(
        event_id="chainlink:round:1",
        event_ts=datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC),
        price=Decimal("84000.00"),
        recv_ts=datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC),
        round_id="1",
    )

    binance_quote = normalize_binance_quote(
        json.loads(
            (FIXTURE_ROOT / "exchange_quotes" / "binance_book_ticker.json").read_text(
                encoding="utf-8"
            )
        ),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 195000, tzinfo=UTC),
    )
    coinbase_quote = normalize_coinbase_quote(
        json.loads(
            (FIXTURE_ROOT / "exchange_quotes" / "coinbase_ticker.json").read_text(
                encoding="utf-8"
            )
        ),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 155000, tzinfo=UTC),
    )
    kraken_quote = normalize_kraken_quote(
        json.loads(
            (FIXTURE_ROOT / "exchange_quotes" / "kraken_book.json").read_text(encoding="utf-8")
        ),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 130000, tzinfo=UTC),
    )
    polymarket_quote = normalize_polymarket_quote(
        json.loads(
            (FIXTURE_ROOT / "polymarket_quotes" / "book_snapshot.json").read_text(
                encoding="utf-8"
            )
        ),
        recv_ts=datetime(2026, 3, 13, 12, 7, 3, 280000, tzinfo=UTC),
    )

    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_metadata",
        lambda config, logger: (
            [metadata_raw],
            [market_candidate],
            market_candidate,
            MetadataSelectionDiagnostics(
                selected_market_id=market_candidate.market_id,
                selected_market_slug=market_candidate.market_slug,
                selected_window_id="btc-5m-20260313T120500Z",
                candidate_count=1,
                admitted_count=1,
                rejected_count_by_reason={},
            ),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_ticks",
        lambda config, logger: SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=(
                {
                    "raw_event_id": "chainlink:round:1",
                    "source_type": "evm_rpc_latest_round_data",
                }
            ),
            normalized_rows=(chainlink_tick,),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_exchange_quotes",
        lambda config, logger: SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=(
                {"raw_event_id": binance_quote.raw_event_id, "venue_id": "binance"},
                {"raw_event_id": coinbase_quote.raw_event_id, "venue_id": "coinbase"},
                {"raw_event_id": kraken_quote.raw_event_id, "venue_id": "kraken"},
            ),
            normalized_rows=(binance_quote, coinbase_quote, kraken_quote),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_quote",
        lambda config, selected_market, selected_window_id, logger: SourceCaptureResult(
            source_name="polymarket_quotes",
            status="success",
            raw_rows=(
                {"raw_event_id": polymarket_quote.raw_event_id, "venue_id": "polymarket"},
            ),
            normalized_rows=(polymarket_quote,),
            details={"selected_window_id": selected_window_id},
        ),
    )

    config = Phase1CaptureConfig(
        data_root=tmp_path / "data",
        artifacts_root=tmp_path / "artifacts",
        logs_root=tmp_path / "logs",
        temp_root=tmp_path / "tmp",
        session_id="test-session",
        capture_started_at=datetime(2026, 3, 13, 12, 10, 0, tzinfo=UTC),
    )
    result = run_phase1_capture(config, logger=_logger())

    assert result.summary_path.exists()
    assert result.selected_window_id == "btc-5m-20260313T120500Z"
    assert result.session_diagnostics.termination_reason == "completed"
    assert (
        tmp_path
        / "data"
        / "raw"
        / "polymarket_metadata"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()
    assert (
        tmp_path
        / "data"
        / "raw"
        / "chainlink"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()
    assert (
        tmp_path
        / "data"
        / "raw"
        / "exchange"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()
    assert (
        tmp_path
        / "data"
        / "raw"
        / "polymarket_quotes"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()
    assert (
        tmp_path
        / "data"
        / "normalized"
        / "market_metadata_events"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()
    assert (
        tmp_path
        / "data"
        / "normalized"
        / "chainlink_ticks"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()
    assert (
        tmp_path
        / "data"
        / "normalized"
        / "exchange_quotes"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()
    assert (
        tmp_path
        / "data"
        / "normalized"
        / "polymarket_quotes"
        / "date=2026-03-13"
        / "session=test-session"
        / "part-00000.jsonl"
    ).exists()


def _logger():
    import logging

    logger = logging.getLogger("test.phase1_capture")
    logger.handlers.clear()
    logger.propagate = False
    return logger
