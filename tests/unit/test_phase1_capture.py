from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from rtds.collectors.phase1_capture import (
    DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS,
    FetchResult,
    MetadataSelectionDiagnostics,
    Phase1CaptureConfig,
    SourceCaptureResult,
    _build_polymarket_quote_payload,
    _collect_chainlink_stream_tick,
    _collect_chainlink_ticks,
    _collect_polymarket_metadata,
    _decode_latest_round_data,
    _run_with_retries,
    _source_capture_interval_seconds,
    run_phase1_capture,
)
from rtds.collectors.polymarket.metadata import RawMetadataMessage, normalize_market_payload
from rtds.core.time import parse_utc
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


def test_collect_chainlink_stream_tick_uses_public_stream_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._http_json",
        lambda *args, **kwargs: FetchResult(
            status="success",
            payload={
                "data": {
                    "liveStreamReports": {
                        "nodes": [
                            {
                                "validFromTimestamp": "2026-03-15T23:50:28+00:00",
                                "price": "72656239970000000000000",
                                "bid": "72653166000000000000000",
                                "ask": "72657366352216465000000",
                            }
                        ]
                    }
                }
            },
            attempts=1,
            retries=0,
            http_status=200,
            headers={"content-type": "application/json"},
        ),
    )

    result = _collect_chainlink_stream_tick(
        Phase1CaptureConfig(
            data_root=Path("data"),
            artifacts_root=Path("artifacts"),
            logs_root=Path("logs"),
            temp_root=Path("tmp"),
            session_id="test-session",
        ),
        logger=_logger(),
    )

    assert result.status == "success"
    assert result.details["oracle_source"] == "chainlink_stream_public_delayed"
    tick = result.normalized_rows[0]
    assert isinstance(tick, ChainlinkTick)
    assert tick.event_ts == parse_utc("2026-03-15T23:50:28+00:00")
    assert tick.price == Decimal("72656.23997")
    assert tick.bid_price == Decimal("72653.166")
    assert tick.ask_price == Decimal("72657.366352216465")
    assert tick.oracle_source == "chainlink_stream_public_delayed"
    assert result.raw_rows[0]["source_type"] == "chainlink_stream_public_timescale"


def test_collect_chainlink_ticks_falls_back_to_snapshot_when_stream_degrades(monkeypatch) -> None:
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_stream_tick",
        lambda config, logger: SourceCaptureResult(
            source_name="chainlink",
            status="degraded",
            raw_rows=({"raw_event_id": "stream-failure"},),
            normalized_rows=(),
            failure_class="stream_reports_missing",
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_snapshot_tick",
        lambda config, logger: SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": "snapshot-success"},),
            normalized_rows=(
                ChainlinkTick(
                    event_id="chainlink:round:1",
                    event_ts=parse_utc("2026-03-15T23:50:28Z"),
                    price=Decimal("72656.23"),
                    oracle_source="chainlink_snapshot_rpc",
                ),
            ),
        ),
    )

    result = _collect_chainlink_ticks(
        Phase1CaptureConfig(
            data_root=Path("data"),
            artifacts_root=Path("artifacts"),
            logs_root=Path("logs"),
            temp_root=Path("tmp"),
            session_id="test-session",
        ),
        logger=_logger(),
    )

    assert result.status == "success"
    assert [row["raw_event_id"] for row in result.raw_rows] == [
        "stream-failure",
        "snapshot-success",
    ]
    assert result.details["fallback_used"] is True
    assert result.details["oracle_source"] == "chainlink_snapshot_rpc"


def test_build_polymarket_quote_payload_uses_best_bid_and_best_ask() -> None:
    payload, empty_sides = _build_polymarket_quote_payload(
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

    assert empty_sides == ()
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
        lambda config, logger=None: (
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
        return SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": f"chainlink:round:{index}", "recv_ts": recv_ts},),
            normalized_rows=(
                ChainlinkTick(
                    event_id=f"chainlink:round:{index}",
                    event_ts=recv_ts,
                    price=Decimal("84000.00") + Decimal(index),
                    recv_ts=recv_ts,
                    round_id=str(index),
                )
            ,),
        )

    def collect_exchange(config, logger):
        index = sample_index["value"]
        return SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=(
                {"raw_event_id": f"binance:{index}", "venue_id": "binance"},
                {"raw_event_id": f"coinbase:{index}", "venue_id": "coinbase"},
                {"raw_event_id": f"kraken:{index}", "venue_id": "kraken"},
            ),
            normalized_rows=(
                _binance_quote(index),
                _coinbase_quote(index),
                _kraken_quote(index),
            ),
        )

    def collect_polymarket(
        config,
        selected_market,
        selected_window_id,
        current_ts=None,
        logger=None,
    ):
        index = sample_index["value"]
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="success",
            raw_rows=(
                {"raw_event_id": f"polymarket:{index}", "venue_id": "polymarket"},
            ),
            normalized_rows=(
                _polymarket_quote(index, market_id=selected_market.market_id),
            ),
            details={"selected_window_id": selected_window_id},
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
    assert result.collectors[0].normalized_row_count == 3
    assert result.collectors[1].normalized_row_count == 3
    assert result.collectors[2].normalized_row_count == 9
    assert result.collectors[3].normalized_row_count == 3
    assert result.selected_market_slug == active_candidate.market_slug
    assert result.selected_window_id == "btc-5m-20260313T120500Z"
    assert result.selector_diagnostics.admitted_count == 1
    assert result.session_diagnostics.degraded_sample_count == 0
    assert result.session_diagnostics.termination_reason == "completed"
    assert result.summary_path.exists()


def test_run_phase1_capture_keeps_running_after_empty_book_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_candidate = _metadata_candidate()
    metadata_raw = _metadata_raw(active_candidate.market_id)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=active_candidate.market_id,
        selected_market_slug=active_candidate.market_slug,
        selected_window_id="btc-5m-20260313T120500Z",
        candidate_count=1,
        admitted_count=1,
        rejected_count_by_reason={},
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
        return SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": f"chainlink:round:{index}", "recv_ts": recv_ts},),
            normalized_rows=(
                ChainlinkTick(
                    event_id=f"chainlink:round:{index}",
                    event_ts=recv_ts,
                    price=Decimal("84000.00"),
                    recv_ts=recv_ts,
                    round_id=str(index),
                ),
            ),
        )

    def collect_exchange(config, logger):
        index = sample_index["value"]
        return SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=(
                {"raw_event_id": f"binance:{index}", "venue_id": "binance"},
                {"raw_event_id": f"coinbase:{index}", "venue_id": "coinbase"},
                {"raw_event_id": f"kraken:{index}", "venue_id": "kraken"},
            ),
            normalized_rows=(
                _binance_quote(index),
                _coinbase_quote(index),
                _kraken_quote(index),
            ),
        )

    def collect_polymarket(
        config,
        selected_market,
        selected_window_id,
        current_ts=None,
        logger=None,
    ):
        index = sample_index["value"]
        if index == 2:
            return SourceCaptureResult(
                source_name="polymarket_quotes",
                status="degraded_valid_empty_book",
                raw_rows=(
                    {
                        "raw_event_id": "polymarket:degraded:2",
                        "market_id": selected_market.market_id,
                        "selected_window_id": selected_window_id,
                        "capture_status": "valid_empty_book",
                    },
                ),
                normalized_rows=(),
                failure_class="valid_empty_book",
                details={"empty_sides": ["up_bid"], "selected_window_id": selected_window_id},
            )
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="success",
            raw_rows=(
                {"raw_event_id": f"polymarket:{index}", "venue_id": "polymarket"},
            ),
            normalized_rows=(
                _polymarket_quote(index, market_id=selected_market.market_id),
            ),
            details={"selected_window_id": selected_window_id},
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
    assert result.collectors[3].raw_row_count == 3
    assert result.collectors[3].normalized_row_count == 2
    assert result.session_diagnostics.degraded_sample_count == 1
    assert result.session_diagnostics.empty_book_count == 1
    assert result.session_diagnostics.termination_reason == "completed"
    diagnostics_rows = result.session_diagnostics.sample_diagnostics_path.read_text(
        encoding="utf-8"
    ).splitlines()
    assert len(diagnostics_rows) == 3
    degraded_row = json.loads(diagnostics_rows[1])
    assert degraded_row["sample_status"] == "degraded"
    assert degraded_row["degraded_sources"] == ["polymarket_quotes"]
    assert (
        degraded_row["source_results"]["polymarket_quotes"]["failure_class"]
        == "valid_empty_book"
    )
    assert result.session_diagnostics.max_consecutive_missing_by_source["polymarket_quotes"] == 0
    assert result.session_diagnostics.polymarket_window_coverage[0]["window_verdict"] == "degraded"


def test_run_with_retries_retries_then_succeeds(monkeypatch) -> None:
    attempts = {"value": 0}
    sleep_calls: list[float] = []

    def flaky_operation():
        attempts["value"] += 1
        if attempts["value"] < 3:
            raise OSError("temporary failure")
        return 200, {}, {"ok": True}

    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )
    monkeypatch.setattr("rtds.collectors.phase1_capture.random.uniform", lambda start, end: 0.0)

    result = _run_with_retries(
        source_name="test_source",
        operation=flaky_operation,
        max_retries=2,
        base_backoff_seconds=0.5,
        max_backoff_seconds=5.0,
        logger=_logger(),
    )

    assert result.status == "success"
    assert result.payload == {"ok": True}
    assert result.retries == 2
    assert sleep_calls == [0.5, 1.0]


def test_run_phase1_capture_does_not_hard_stop_on_valid_empty_book_streak(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metadata_raw = _metadata_raw(_metadata_candidate().market_id)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=_metadata_candidate().market_id,
        selected_market_slug=_metadata_candidate().market_slug,
        selected_window_id="btc-5m-20260313T120500Z",
        candidate_count=1,
        admitted_count=1,
        rejected_count_by_reason={},
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_metadata",
        lambda config, logger: (
            [metadata_raw],
            [_metadata_candidate()],
            _metadata_candidate(),
            selector_diagnostics,
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_ticks",
        lambda config, logger: SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": "chainlink:round:1"},),
            normalized_rows=(
                ChainlinkTick(
                    event_id="chainlink:round:1",
                    event_ts=datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC),
                    price=Decimal("84000.00"),
                    recv_ts=datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC),
                    round_id="1",
                ),
            ),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_exchange_quotes",
        lambda config, logger: SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=(
                {"raw_event_id": "binance:1", "venue_id": "binance"},
                {"raw_event_id": "coinbase:1", "venue_id": "coinbase"},
                {"raw_event_id": "kraken:1", "venue_id": "kraken"},
            ),
            normalized_rows=(_binance_quote(1), _coinbase_quote(1), _kraken_quote(1)),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_quote",
        lambda config, selected_market, selected_window_id, current_ts=None, logger=None: (
            SourceCaptureResult(
                source_name="polymarket_quotes",
                status="degraded_valid_empty_book",
                raw_rows=(
                    {
                        "raw_event_id": "polymarket:valid-empty",
                        "market_id": selected_market.market_id,
                        "selected_window_id": selected_window_id,
                        "capture_status": "valid_empty_book",
                    },
                ),
                normalized_rows=(),
                failure_class="valid_empty_book",
                details={
                    "selected_market_id": selected_market.market_id,
                    "selected_market_slug": selected_market.market_slug,
                    "selected_window_id": selected_window_id,
                    "within_rollover_grace_window": False,
                    "metadata_refresh_attempted": False,
                    "metadata_refresh_changed_binding": False,
                },
            )
        ),
    )
    monotonic_values = iter([0.0, 1.0, 61.0, 121.0, 181.0])
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("rtds.collectors.phase1_capture.time.sleep", lambda seconds: None)

    result = run_phase1_capture(
        Phase1CaptureConfig(
            data_root=tmp_path / "data",
            artifacts_root=tmp_path / "artifacts",
            logs_root=tmp_path / "logs",
            temp_root=tmp_path / "tmp",
            session_id="test-session",
            capture_started_at=datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC),
            duration_seconds=120.0,
            poll_interval_seconds=60.0,
            max_consecutive_polymarket_failures=1,
        ),
        logger=_logger(),
    )

    assert result.sample_count == 3
    assert result.session_diagnostics.termination_reason == "completed"
    assert result.session_diagnostics.max_consecutive_missing_by_source["polymarket_quotes"] == 0
    assert result.session_diagnostics.polymarket_window_coverage[0]["valid_empty_book_samples"] == 3
    assert result.session_diagnostics.polymarket_window_coverage[0]["window_verdict"] == "unusable"


def test_run_phase1_capture_writes_incremental_partial_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_candidate = _metadata_candidate()
    metadata_raw = _metadata_raw(active_candidate.market_id)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=active_candidate.market_id,
        selected_market_slug=active_candidate.market_slug,
        selected_window_id="btc-5m-20260313T120500Z",
        candidate_count=1,
        admitted_count=1,
        rejected_count_by_reason={},
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
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_ticks",
        lambda config, logger: SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": "chainlink:1"},),
            normalized_rows=(
                ChainlinkTick(
                    event_id="chainlink:round:1",
                    event_ts=datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC),
                    price=Decimal("84000.00"),
                    recv_ts=datetime(2026, 3, 13, 12, 5, 1, tzinfo=UTC),
                    round_id="1",
                ),
            ),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_exchange_quotes",
        lambda config, logger: SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=(
                {"raw_event_id": "binance:1", "venue_id": "binance"},
                {"raw_event_id": "coinbase:1", "venue_id": "coinbase"},
                {"raw_event_id": "kraken:1", "venue_id": "kraken"},
            ),
            normalized_rows=(_binance_quote(1), _coinbase_quote(1), _kraken_quote(1)),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_quote",
        lambda config, selected_market, selected_window_id, current_ts=None, logger=None: (
            SourceCaptureResult(
                source_name="polymarket_quotes",
                status="success",
                raw_rows=({"raw_event_id": "polymarket:1"},),
                normalized_rows=(_polymarket_quote(1, market_id=selected_market.market_id),),
                details={"selected_window_id": selected_window_id},
            )
        ),
    )
    monotonic_values = iter([0.0, 1.0, 61.0, 121.0, 181.0])
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("rtds.collectors.phase1_capture.time.sleep", lambda seconds: None)

    result = run_phase1_capture(
        Phase1CaptureConfig(
            data_root=tmp_path / "data",
            artifacts_root=tmp_path / "artifacts",
            logs_root=tmp_path / "logs",
            temp_root=tmp_path / "tmp",
            session_id="test-session",
            capture_started_at=datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC),
            duration_seconds=120.0,
            poll_interval_seconds=60.0,
            checkpoint_interval_seconds=60.0,
        ),
        logger=_logger(),
    )

    partial_summary_path = result.summary_path.with_name("summary.partial.json")
    partial_summary = json.loads(partial_summary_path.read_text(encoding="utf-8"))

    assert partial_summary["session_status"] == "completed"
    assert partial_summary["last_completed_sample_number"] == 3
    assert partial_summary["selected_market_id"] == active_candidate.market_id
    assert partial_summary["selected_window_id"] == "btc-5m-20260313T120500Z"
    assert partial_summary["last_healthy_timestamp_by_source"]["chainlink"] is not None
    assert partial_summary["last_healthy_timestamp_by_source"]["exchange"] is not None
    assert partial_summary["last_healthy_timestamp_by_source"]["polymarket_quotes"] is not None
    assert result.session_diagnostics.summary_partial_path == partial_summary_path


def test_run_phase1_capture_leaves_partial_artifacts_after_crash(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_candidate = _metadata_candidate()
    metadata_raw = _metadata_raw(active_candidate.market_id)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=active_candidate.market_id,
        selected_market_slug=active_candidate.market_slug,
        selected_window_id="btc-5m-20260313T120500Z",
        candidate_count=1,
        admitted_count=1,
        rejected_count_by_reason={},
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
        recv_ts = datetime(2026, 3, 13, 12, 5, index, tzinfo=UTC)
        return SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": f"chainlink:{index}"},),
            normalized_rows=(
                ChainlinkTick(
                    event_id=f"chainlink:{index}",
                    event_ts=recv_ts,
                    price=Decimal("84000.00"),
                    recv_ts=recv_ts,
                    round_id=str(index),
                ),
            ),
        )

    def collect_exchange(config, logger):
        if sample_index["value"] == 3:
            raise RuntimeError("exchange collector crashed")
        return SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=(
                {"raw_event_id": "binance:1", "venue_id": "binance"},
                {"raw_event_id": "coinbase:1", "venue_id": "coinbase"},
                {"raw_event_id": "kraken:1", "venue_id": "kraken"},
            ),
            normalized_rows=(_binance_quote(1), _coinbase_quote(1), _kraken_quote(1)),
        )

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
        lambda config, selected_market, selected_window_id, current_ts=None, logger=None: (
            SourceCaptureResult(
                source_name="polymarket_quotes",
                status="success",
                raw_rows=({"raw_event_id": "polymarket:1"},),
                normalized_rows=(_polymarket_quote(1, market_id=selected_market.market_id),),
                details={"selected_window_id": selected_window_id},
            )
        ),
    )
    monotonic_values = iter([0.0, 1.0, 61.0, 121.0])
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("rtds.collectors.phase1_capture.time.sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="exchange collector crashed"):
        run_phase1_capture(
            Phase1CaptureConfig(
                data_root=tmp_path / "data",
                artifacts_root=tmp_path / "artifacts",
                logs_root=tmp_path / "logs",
                temp_root=tmp_path / "tmp",
                session_id="test-session",
                capture_started_at=datetime(2026, 3, 13, 12, 5, 0, tzinfo=UTC),
                duration_seconds=180.0,
                poll_interval_seconds=60.0,
                checkpoint_interval_seconds=60.0,
            ),
            logger=_logger(),
        )

    partial_summary_path = (
        tmp_path
        / "artifacts"
        / "collect"
        / "date=2026-03-13"
        / "session=test-session"
        / "summary.partial.json"
    )
    diagnostics_path = partial_summary_path.with_name("sample_diagnostics.jsonl")
    partial_summary = json.loads(partial_summary_path.read_text(encoding="utf-8"))

    assert partial_summary["session_status"] == "crashed"
    assert partial_summary["termination_reason"] == "uncaught_exception"
    assert partial_summary["failure_type"] == "RuntimeError"
    assert partial_summary["last_completed_sample_number"] == 2
    assert partial_summary["sample_diagnostics_path"] == str(diagnostics_path)
    assert len(diagnostics_path.read_text(encoding="utf-8").splitlines()) == 2


def test_run_with_retries_allows_http_payload_with_empty_error_list() -> None:
    result = _run_with_retries(
        source_name="exchange",
        operation=lambda: (200, {}, {"error": [], "result": {"ok": True}}),
        max_retries=0,
        base_backoff_seconds=0.5,
        max_backoff_seconds=5.0,
        logger=_logger(),
    )

    assert result.status == "success"
    assert result.payload == {"error": [], "result": {"ok": True}}


def test_run_phase1_capture_refreshes_polymarket_binding_on_rollover_404(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_candidate = replace(
        _metadata_candidate(),
        market_slug="btc-updown-5m-1773551400",
        market_open_ts=datetime(2026, 3, 15, 5, 10, 0, tzinfo=UTC),
        market_close_ts=datetime(2026, 3, 15, 5, 15, 0, tzinfo=UTC),
    )
    next_candidate = replace(
        active_candidate,
        market_id="0x" + "4" * 64,
        market_slug="btc-updown-5m-1773551700",
        market_open_ts=datetime(2026, 3, 15, 5, 15, 0, tzinfo=UTC),
        market_close_ts=datetime(2026, 3, 15, 5, 20, 0, tzinfo=UTC),
        token_yes_id="yes-next",
        token_no_id="no-next",
    )
    metadata_raw = _metadata_raw(active_candidate.market_id)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=active_candidate.market_id,
        selected_market_slug=active_candidate.market_slug,
        selected_window_id="btc-5m-20260315T051000Z",
        candidate_count=2,
        admitted_count=2,
        rejected_count_by_reason={},
    )

    metadata_batches = iter(
        [
            (
                [metadata_raw],
                [active_candidate, next_candidate],
                active_candidate,
                selector_diagnostics,
            ),
            (
                [metadata_raw],
                [active_candidate, next_candidate],
                next_candidate,
                selector_diagnostics,
            ),
        ]
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_metadata",
        lambda config, logger: next(metadata_batches),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_ticks",
        lambda config, logger: SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": "chainlink:round:1"},),
            normalized_rows=(
                ChainlinkTick(
                    event_id="chainlink:round:1",
                    event_ts=datetime(2026, 3, 15, 5, 14, 0, tzinfo=UTC),
                    price=Decimal("84000.00"),
                    recv_ts=datetime(2026, 3, 15, 5, 14, 0, tzinfo=UTC),
                    round_id="1",
                ),
            ),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_exchange_quotes",
        lambda config, logger: SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=({"raw_event_id": "binance:1", "venue_id": "binance"},),
            normalized_rows=(_binance_quote(1),),
        ),
    )
    selected_candidates = iter([active_candidate, next_candidate])
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._select_market_for_current_time",
        lambda candidates, current_ts: next(selected_candidates),
    )

    quote_attempts: list[str] = []

    def collect_polymarket(
        config,
        selected_market,
        selected_window_id,
        current_ts=None,
        logger=None,
    ):
        quote_attempts.append(selected_market.market_id)
        if len(quote_attempts) == 1:
            return SourceCaptureResult(
                source_name="polymarket_quotes",
                status="selector_refresh_required",
                raw_rows=(
                    {"raw_event_id": "polymarket:404:1", "market_id": selected_market.market_id},
                ),
                normalized_rows=(),
                retries=0,
                failure_class="selector_refresh_required",
                failure_type="HTTPError",
                failure_message="HTTP Error 404: Not Found",
                http_status=404,
                details={
                    "selected_market_id": selected_market.market_id,
                    "selected_market_slug": selected_market.market_slug,
                    "selected_window_id": selected_window_id,
                    "seconds_remaining": 30.0,
                    "within_rollover_grace_window": True,
                    "metadata_refresh_attempted": False,
                    "metadata_refresh_changed_binding": False,
                },
            )
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="success",
            raw_rows=(
                {"raw_event_id": "polymarket:ok:2", "market_id": selected_market.market_id},
            ),
            normalized_rows=(
                _polymarket_quote(1, market_id=selected_market.market_id),
            ),
            details={
                "selected_market_id": selected_market.market_id,
                "selected_market_slug": selected_market.market_slug,
                "selected_window_id": selected_window_id,
                "seconds_remaining": 300.0,
                "within_rollover_grace_window": False,
                "metadata_refresh_attempted": False,
                "metadata_refresh_changed_binding": False,
            },
        )

    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_quote",
        collect_polymarket,
    )
    monotonic_values = iter([0.0, 1.0])
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.monotonic",
        lambda: next(monotonic_values),
    )

    result = run_phase1_capture(
        Phase1CaptureConfig(
            data_root=tmp_path / "data",
            artifacts_root=tmp_path / "artifacts",
            logs_root=tmp_path / "logs",
            temp_root=tmp_path / "tmp",
            session_id="test-session",
            capture_started_at=datetime(2026, 3, 15, 5, 14, 30, tzinfo=UTC),
            duration_seconds=0.0,
        ),
        logger=_logger(),
    )

    assert result.sample_count == 1
    assert quote_attempts == [active_candidate.market_id, next_candidate.market_id]
    assert result.session_diagnostics.polymarket_selector_refresh_count == 1
    assert result.session_diagnostics.polymarket_selector_rebind_count == 1
    diagnostics_row = json.loads(
        result.session_diagnostics.sample_diagnostics_path.read_text(encoding="utf-8").splitlines()[0]
    )
    polymarket_diag = diagnostics_row["source_results"]["polymarket_quotes"]
    assert polymarket_diag["status"] == "success"
    assert polymarket_diag["details"]["metadata_refresh_attempted"] is True
    assert polymarket_diag["details"]["metadata_refresh_changed_binding"] is True
    assert diagnostics_row["selected_market_id"] == next_candidate.market_id


def test_source_capture_interval_uses_boundary_burst_for_core_sources() -> None:
    active_candidate = replace(
        _metadata_candidate(),
        market_open_ts=datetime(2026, 3, 15, 5, 15, 0, tzinfo=UTC),
        market_close_ts=datetime(2026, 3, 15, 5, 20, 0, tzinfo=UTC),
    )

    config = Phase1CaptureConfig(
        data_root=Path("data"),
        artifacts_root=Path("artifacts"),
        logs_root=Path("logs"),
        temp_root=Path("tmp"),
        session_id="test-session",
        metadata_poll_interval_seconds=60.0,
        chainlink_poll_interval_seconds=60.0,
        exchange_poll_interval_seconds=60.0,
        polymarket_quote_poll_interval_seconds=60.0,
        boundary_burst_enabled=True,
        boundary_burst_window_seconds=15.0,
        boundary_burst_interval_seconds=DEFAULT_BOUNDARY_BURST_INTERVAL_SECONDS,
    )

    assert (
        _source_capture_interval_seconds(
            config,
            source_name="chainlink",
            current_ts=datetime(2026, 3, 15, 5, 14, 50, tzinfo=UTC),
            selected_market=active_candidate,
        )
        == 1.0
    )
    assert (
        _source_capture_interval_seconds(
            config,
            source_name="metadata",
            current_ts=datetime(2026, 3, 15, 5, 14, 50, tzinfo=UTC),
            selected_market=active_candidate,
        )
        == 60.0
    )


def test_run_phase1_capture_stops_when_polymarket_market_is_invalid_after_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_candidate = _metadata_candidate()
    metadata_raw = _metadata_raw(active_candidate.market_id)
    selector_diagnostics = MetadataSelectionDiagnostics(
        selected_market_id=active_candidate.market_id,
        selected_market_slug=active_candidate.market_slug,
        selected_window_id="btc-5m-20260313T120500Z",
        candidate_count=1,
        admitted_count=1,
        rejected_count_by_reason={},
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
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_chainlink_ticks",
        lambda config, logger: SourceCaptureResult(
            source_name="chainlink",
            status="success",
            raw_rows=({"raw_event_id": "chainlink:round:1"},),
            normalized_rows=(
                ChainlinkTick(
                    event_id="chainlink:round:1",
                    event_ts=datetime(2026, 3, 15, 1, 0, 0, tzinfo=UTC),
                    price=Decimal("84000.00"),
                    recv_ts=datetime(2026, 3, 15, 1, 0, 0, tzinfo=UTC),
                    round_id="1",
                ),
            ),
        ),
    )
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_exchange_quotes",
        lambda config, logger: SourceCaptureResult(
            source_name="exchange",
            status="success",
            raw_rows=({"raw_event_id": "binance:1", "venue_id": "binance"},),
            normalized_rows=(_binance_quote(1),),
        ),
    )

    def collect_polymarket(
        config,
        selected_market,
        selected_window_id,
        current_ts=None,
        logger=None,
    ):
        return SourceCaptureResult(
            source_name="polymarket_quotes",
            status="terminal_failure",
            raw_rows=(
                {"raw_event_id": "polymarket:404:1", "market_id": selected_market.market_id},
            ),
            normalized_rows=(),
            failure_class="market_binding_stale",
            failure_type="HTTPError",
            failure_message="HTTP Error 404: Not Found",
            http_status=404,
            details={
                "selected_market_id": selected_market.market_id,
                "selected_market_slug": selected_market.market_slug,
                "selected_window_id": selected_window_id,
                "seconds_remaining": 240.0,
                "within_rollover_grace_window": False,
                "metadata_refresh_attempted": False,
                "metadata_refresh_changed_binding": False,
            },
        )

    monkeypatch.setattr(
        "rtds.collectors.phase1_capture._collect_polymarket_quote",
        collect_polymarket,
    )
    monotonic_values = iter([0.0, 1.0])
    monkeypatch.setattr(
        "rtds.collectors.phase1_capture.time.monotonic",
        lambda: next(monotonic_values),
    )

    result = run_phase1_capture(
        Phase1CaptureConfig(
            data_root=tmp_path / "data",
            artifacts_root=tmp_path / "artifacts",
            logs_root=tmp_path / "logs",
            temp_root=tmp_path / "tmp",
            session_id="test-session",
            capture_started_at=datetime(2026, 3, 15, 1, 1, 0, tzinfo=UTC),
            duration_seconds=0.0,
        ),
        logger=_logger(),
    )

    assert result.session_diagnostics.termination_reason == "polymarket_market_invalid"
    diagnostics_row = json.loads(
        result.session_diagnostics.sample_diagnostics_path.read_text(encoding="utf-8").splitlines()[0]
    )
    assert diagnostics_row["sample_status"] == "failed"
    polymarket_diag = diagnostics_row["source_results"]["polymarket_quotes"]
    assert polymarket_diag["failure_class"] == "binding_invalid"
    assert polymarket_diag["details"]["metadata_refresh_attempted"] is True


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
