from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from rtds.collectors.phase1_capture import (
    MetadataSelectionDiagnostics,
    Phase1CaptureConfig,
    _build_polymarket_quote_payload,
    _collect_polymarket_metadata,
    _decode_latest_round_data,
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


def test_decode_latest_round_data_parses_chainlink_tuple() -> None:
    payload = (
        "0x"
        "00000000000000000000000000000000000000000000000100000000000c24a6"
        "0000000000000000000000000000000000000000000000000006792850a95800"
        "0000000000000000000000000000000000000000000000000000000069b5f396"
        "0000000000000000000000000000000000000000000000000000000069b5f396"
        "00000000000000000000000000000000000000000000000100000000000c24a6"
    )

    decoded = _decode_latest_round_data(payload)

    assert decoded["round_id"] == 18446744073710347430
    assert decoded["answer"] == 1822063919192064
    assert decoded["updated_at"] == 1773532054


def test_build_polymarket_quote_payload_uses_best_bid_and_best_ask() -> None:
    payload = _build_polymarket_quote_payload(
        market_id="0xbtc",
        yes_token_id="yes-token",
        no_token_id="no-token",
        yes_book={
            "timestamp": "1773532629107",
            "hash": "yes-hash",
            "bids": [{"price": "0.45", "size": "10"}, {"price": "0.48", "size": "12"}],
            "asks": [{"price": "0.52", "size": "20"}, {"price": "0.50", "size": "30"}],
        },
        no_book={
            "timestamp": "1773532629107",
            "hash": "no-hash",
            "bids": [{"price": "0.49", "size": "9"}, {"price": "0.51", "size": "8"}],
            "asks": [{"price": "0.55", "size": "11"}, {"price": "0.53", "size": "7"}],
        },
    )

    assert payload["market_id"] == "0xbtc"
    assert payload["sequence_id"] == "yes-hash:no-hash"
    assert payload["outcomes"]["up"]["bid"] == {"price": "0.48", "size": "12"}
    assert payload["outcomes"]["up"]["ask"] == {"price": "0.50", "size": "30"}
    assert payload["outcomes"]["down"]["bid"] == {"price": "0.51", "size": "8"}
    assert payload["outcomes"]["down"]["ask"] == {"price": "0.53", "size": "7"}


def test_collect_polymarket_metadata_keeps_only_admitted_target_family(monkeypatch) -> None:
    active_candidate = _metadata_candidate()
    non_family_candidate = replace(
        active_candidate,
        market_id="0x" + "2" * 64,
        market_slug="will-bitcoin-hit-150k-by-december-31-2026",
        market_question="Will Bitcoin hit $150k by December 31, 2026?",
        market_title="When will Bitcoin hit $150k?",
        market_open_ts=datetime(2025, 8, 7, 16, 29, 32, tzinfo=UTC),
        market_close_ts=datetime(2027, 1, 1, 4, 0, 0, tzinfo=UTC),
    )
    non_btc_candidate = replace(
        active_candidate,
        market_id="0x" + "3" * 64,
        asset_id="ETH",
        market_slug="eth-market",
    )

    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._fetch_polymarket_market_pages",
        lambda config: (
            [_metadata_raw(active_candidate.market_id)],
            [active_candidate, non_family_candidate, non_btc_candidate],
        ),
    )

    _, metadata_rows, selected_market, selector_diagnostics = _collect_polymarket_metadata(
        Phase1CaptureConfig(
            data_root=Path("data"),
            artifacts_root=Path("artifacts"),
            logs_root=Path("logs"),
            temp_root=Path("tmp"),
            session_id="test-session",
        ),
        logger=_logger(),
    )

    assert [row.market_id for row in metadata_rows] == [active_candidate.market_id]
    assert selected_market.market_id == active_candidate.market_id
    assert selector_diagnostics.candidate_count == 2
    assert selector_diagnostics.admitted_count == 1
    assert selector_diagnostics.selected_window_id == "btc-5m-20260313T120500Z"
    assert selector_diagnostics.rejected_count_by_reason["structure_mismatch"] == 1


def test_run_phase1_capture_repeats_samples_for_bounded_session(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_candidate = _metadata_candidate()
    metadata_raw = _metadata_raw(active_candidate.market_id)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=active_candidate.market_id,
        selected_market_slug=active_candidate.market_slug,
        selected_window_id="btc-5m-20260313T120500Z",
        candidate_count=3,
        admitted_count=1,
        rejected_count_by_reason={"structure_mismatch": 2},
    )

    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_metadata",
        lambda config, logger: (
            [metadata_raw],
            [active_candidate],
            active_candidate,
            selector_diagnostics,
        ),
    )

    sample_index = {"value": 0}

    def collect_chainlink(config, logger):
        sample_index["value"] += 1
        index = sample_index["value"]
        recv_ts = datetime(2026, 3, 15, 0, index, 0, tzinfo=UTC)
        return (
            [{"raw_event_id": f"chainlink:round:{index}", "recv_ts": recv_ts}],
            [
                ChainlinkTick(
                    event_id=f"chainlink:round:{index}",
                    event_ts=recv_ts,
                    price=Decimal("84000.00") + Decimal(index),
                    recv_ts=recv_ts,
                    round_id=str(index),
                )
            ],
        )

    def collect_exchange(config, logger):
        index = sample_index["value"]
        return (
            [
                {"raw_event_id": f"binance:{index}", "venue_id": "binance"},
                {"raw_event_id": f"coinbase:{index}", "venue_id": "coinbase"},
                {"raw_event_id": f"kraken:{index}", "venue_id": "kraken"},
            ],
            [
                _binance_quote(index),
                _coinbase_quote(index),
                _kraken_quote(index),
            ],
        )

    def collect_polymarket(config, selected_market, logger):
        index = sample_index["value"]
        return (
            [{"raw_event_id": f"polymarket:{index}", "venue_id": "polymarket"}],
            [_polymarket_quote(index, market_id=selected_market.market_id)],
        )

    monotonic_values = iter([0.0, 1.0, 61.0, 121.0])
    sleep_calls: list[float] = []
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_ticks",
        collect_chainlink,
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_exchange_quotes",
        collect_exchange,
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_quote",
        collect_polymarket,
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    result = run_phase1_capture(
        Phase1CaptureConfig(
            data_root=tmp_path / "data",
            artifacts_root=tmp_path / "artifacts",
            logs_root=tmp_path / "logs",
            temp_root=tmp_path / "tmp",
            session_id="test-session",
            capture_started_at=datetime(2026, 3, 15, 0, 0, 0, tzinfo=UTC),
            duration_seconds=120.0,
            poll_interval_seconds=60.0,
        ),
        logger=_logger(),
    )

    assert result.sample_count == 3
    assert sleep_calls == [60.0, 59.0]
    assert result.collectors[0].normalized_row_count == 1
    assert result.collectors[1].normalized_row_count == 3
    assert result.collectors[2].normalized_row_count == 9
    assert result.collectors[3].normalized_row_count == 3
    assert result.selected_market_slug == active_candidate.market_slug
    assert result.selected_window_id == "btc-5m-20260313T120500Z"
    assert result.selector_diagnostics.admitted_count == 1
    assert result.summary_path.exists()


def _metadata_candidate():
    metadata_payload = json.loads(
        (FIXTURE_ROOT / "polymarket_metadata" / "btc_5m_event.json").read_text(encoding="utf-8")
    )
    return normalize_market_payload(
        market_payload=metadata_payload["markets"][0],
        event_payload=metadata_payload,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
    )


def _metadata_raw(market_id: str) -> RawMetadataMessage:
    return RawMetadataMessage(
        raw_event_id="rawmeta:test",
        venue_id="polymarket",
        source_type="metadata_http",
        endpoint="/markets",
        market_id=market_id,
        recv_ts=datetime(2026, 3, 13, 12, 4, 45, tzinfo=UTC),
        proc_ts=datetime(2026, 3, 13, 12, 4, 46, tzinfo=UTC),
        raw_payload=[],
        payload_format="json",
        collector_session_id="test-session",
        parser_version="0.1.0",
        schema_version="0.1.0",
        parse_status="parsed",
        http_status=200,
        request_url="https://gamma-api.polymarket.com/markets",
    )


def _binance_quote(index: int):
    payload = json.loads(
        (FIXTURE_ROOT / "exchange_quotes" / "binance_book_ticker.json").read_text(encoding="utf-8")
    )
    return normalize_binance_quote(
        payload,
        recv_ts=datetime(2026, 3, 15, 0, index, 3, 195000, tzinfo=UTC),
    )


def _coinbase_quote(index: int):
    payload = json.loads(
        (FIXTURE_ROOT / "exchange_quotes" / "coinbase_ticker.json").read_text(encoding="utf-8")
    )
    return normalize_coinbase_quote(
        payload,
        recv_ts=datetime(2026, 3, 15, 0, index, 3, 155000, tzinfo=UTC),
    )


def _kraken_quote(index: int):
    payload = json.loads(
        (FIXTURE_ROOT / "exchange_quotes" / "kraken_book.json").read_text(encoding="utf-8")
    )
    return normalize_kraken_quote(
        payload,
        recv_ts=datetime(2026, 3, 15, 0, index, 3, 130000, tzinfo=UTC),
    )


def _polymarket_quote(index: int, *, market_id: str):
    payload = json.loads(
        (FIXTURE_ROOT / "polymarket_quotes" / "book_snapshot.json").read_text(encoding="utf-8")
    )
    payload["market_id"] = market_id
    return normalize_polymarket_quote(
        payload,
        recv_ts=datetime(2026, 3, 15, 0, index, 3, 280000, tzinfo=UTC),
    )


def _logger() -> logging.Logger:
    logger = logging.getLogger("test.phase1_capture")
    logger.handlers.clear()
    logger.propagate = False
    return logger
