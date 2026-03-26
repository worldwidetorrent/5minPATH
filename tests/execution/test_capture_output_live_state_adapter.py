from __future__ import annotations

import json
from pathlib import Path

from rtds.execution.capture_output_live_state_adapter import (
    CaptureOutputLiveStateAdapter,
    CaptureOutputLiveStateConfig,
)
from rtds.execution.enums import PolicyMode
from rtds.execution.shadow_engine import ShadowEngine, ShadowEngineConfig
from rtds.execution.sizing import SIZE_MODE_FIXED_CONTRACTS, SizingPolicy


def test_capture_output_adapter_is_production_live_state() -> None:
    assert CaptureOutputLiveStateAdapter.descriptor.adapter_role == "live_state"
    assert CaptureOutputLiveStateAdapter.descriptor.production_safe is True


def test_capture_output_adapter_builds_executable_state_from_session_outputs(tmp_path) -> None:
    session_id = "20260326T010000000Z"
    _write_fixture_session(tmp_path, session_id=session_id)
    adapter = CaptureOutputLiveStateAdapter(
        CaptureOutputLiveStateConfig(
            session_id=session_id,
            normalized_root=tmp_path / "data/normalized",
            artifacts_root=tmp_path / "artifacts/collect",
        )
    )

    state = adapter.read_state()

    assert state is not None
    assert state.session_id == session_id
    assert state.state_source_kind == "live_state"
    assert state.polymarket_market_id == "0xmarket"
    assert state.clob_token_id_up == "up-token"
    assert state.clob_token_id_down == "down-token"
    assert state.chainlink_confidence_state == "high"
    assert state.window_quality_regime == "good"
    assert state.fair_value_base is not None
    assert state.calibrated_fair_value_base == state.fair_value_base
    assert state.up_ask_price is not None
    assert adapter.read_state() is None


def test_capture_output_adapter_wires_into_shadow_engine(tmp_path) -> None:
    session_id = "20260326T010000000Z"
    _write_fixture_session(tmp_path, session_id=session_id)
    adapter = CaptureOutputLiveStateAdapter(
        CaptureOutputLiveStateConfig(
            session_id=session_id,
            normalized_root=tmp_path / "data/normalized",
            artifacts_root=tmp_path / "artifacts/collect",
        )
    )
    engine = ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id=session_id,
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts="10",
            ),
            min_net_edge="0.01",
            max_quote_age_ms=2000,
            max_spread_abs="0.03",
            idle_sleep_seconds=0,
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )

    processed = engine.process_next_state()

    assert processed is True
    assert (tmp_path / f"artifacts/shadow/{session_id}/shadow_decisions.jsonl").exists()
    assert (tmp_path / f"artifacts/shadow/{session_id}/shadow_order_states.jsonl").exists()
    assert (tmp_path / f"artifacts/shadow/{session_id}/shadow_summary.json").exists()


def _write_fixture_session(tmp_path: Path, *, session_id: str) -> None:
    capture_dir = tmp_path / f"artifacts/collect/date=2026-03-26/session={session_id}"
    normalized_root = tmp_path / "data/normalized"
    capture_dir.mkdir(parents=True, exist_ok=True)

    _write_jsonl(
        capture_dir / "sample_diagnostics.jsonl",
        [
            {
                "sample_index": 1,
                "sample_started_at": "2026-03-26T01:00:05.000Z",
                "sample_status": "healthy",
                "degraded_sources": [],
                "selected_market_id": "0xmarket",
                "selected_market_slug": "btc-updown-5m-1770000600",
                "selected_window_id": "btc-5m-20260326T010000Z",
                "source_results": {
                    "chainlink": {"status": "success", "details": {"fallback_used": False}},
                    "polymarket_quotes": {
                        "status": "success",
                        "details": {"seconds_remaining": 295},
                    },
                },
            }
        ],
    )

    _write_jsonl(
        normalized_root
        / f"chainlink_ticks/date=2026-03-26/session={session_id}/part-00000.jsonl",
        [
            {
                "event_id": "chain-1",
                "event_ts": "2026-03-26T01:00:04Z",
                "price": "70000",
                "recv_ts": "2026-03-26T01:00:05Z",
                "oracle_source": "chainlink_stream_public_delayed",
                "oracle_feed_id": "chainlink:stream:BTC-USD",
                "round_id": None,
                "bid_price": "69999",
                "ask_price": "70001",
            }
        ],
    )

    _write_jsonl(
        normalized_root
        / f"exchange_quotes/date=2026-03-26/session={session_id}/part-00000.jsonl",
        [
            _exchange_row("binance", "binance:spot:BTCUSDT", "70500.0"),
            _exchange_row("coinbase", "coinbase:spot:BTC-USD", "70510.0"),
            _exchange_row("kraken", "kraken:spot:BTC-USD", "70520.0"),
        ],
    )

    _write_jsonl(
        normalized_root
        / f"polymarket_quotes/date=2026-03-26/session={session_id}/part-00000.jsonl",
        [
            {
                "venue_id": "polymarket",
                "market_id": "0xmarket",
                "asset_id": "BTC",
                "event_ts": "2026-03-26T01:00:05Z",
                "recv_ts": "2026-03-26T01:00:05Z",
                "proc_ts": "2026-03-26T01:00:05Z",
                "up_bid": "0.58",
                "up_ask": "0.60",
                "down_bid": "0.40",
                "down_ask": "0.42",
                "up_bid_size_contracts": "50",
                "up_ask_size_contracts": "40",
                "down_bid_size_contracts": "50",
                "down_ask_size_contracts": "40",
                "raw_event_id": "rawpoly:1",
                "normalizer_version": "0.1.0",
                "schema_version": "0.1.0",
                "created_ts": "2026-03-26T01:00:05Z",
                "token_yes_id": "up-token",
                "token_no_id": "down-token",
                "market_quote_type": "orderbook_top",
                "quote_sequence_id": "seq-1",
                "market_mid_up": "0.59",
                "market_mid_down": "0.41",
                "market_spread_up_abs": "0.02",
                "market_spread_down_abs": "0.02",
                "last_trade_price": None,
                "last_trade_size_contracts": None,
                "last_trade_side": None,
                "last_trade_outcome": None,
                "source_event_missing_ts_flag": False,
                "crossed_market_flag": False,
                "locked_market_flag": False,
                "quote_completeness_flag": True,
                "normalization_status": "normalized",
            }
        ],
    )

    _write_jsonl(
        normalized_root
        / f"market_metadata_events/date=2026-03-26/session={session_id}/part-00000.jsonl",
        [
            {
                "market_id": "0xmarket",
                "token_yes_id": "up-token",
                "token_no_id": "down-token",
            }
        ],
    )


def _exchange_row(venue_id: str, instrument_id: str, mid_price: str) -> dict[str, object]:
    mid = float(mid_price)
    best_bid = mid - 5
    best_ask = mid + 5
    return {
        "venue_id": venue_id,
        "instrument_id": instrument_id,
        "asset_id": "BTC",
        "event_ts": "2026-03-26T01:00:05Z",
        "recv_ts": "2026-03-26T01:00:05Z",
        "proc_ts": "2026-03-26T01:00:05Z",
        "best_bid": f"{best_bid:.2f}",
        "best_ask": f"{best_ask:.2f}",
        "mid_price": f"{mid:.2f}",
        "bid_size": "1.0",
        "ask_size": "1.0",
        "raw_event_id": f"raw-{venue_id}",
        "normalizer_version": "0.1.0",
        "schema_version": "0.1.0",
        "created_ts": "2026-03-26T01:00:05Z",
        "quote_type": "book",
        "quote_depth_level": 1,
        "sequence_id": f"{venue_id}-1",
        "source_event_missing_ts_flag": False,
        "crossed_market_flag": False,
        "locked_market_flag": False,
        "normalization_status": "normalized",
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
