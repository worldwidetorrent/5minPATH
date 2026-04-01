from __future__ import annotations

from decimal import Decimal

from rtds.core.time import parse_utc
from rtds.execution.enums import NoTradeReason
from rtds.execution.state_assembler import (
    CaptureOutputStateAssembler,
    _build_exchange_venue_diagnostics,
)


def test_state_assembler_builds_deterministic_executable_state() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_chainlink_row(
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
    )
    for venue_id, instrument_id, mid_price in (
        ("binance", "binance:spot:BTCUSDT", "70500.0"),
        ("coinbase", "coinbase:spot:BTC-USD", "70510.0"),
        ("kraken", "kraken:spot:BTC-USD", "70520.0"),
    ):
        assembler.ingest_exchange_row(
            {
                "venue_id": venue_id,
                "instrument_id": instrument_id,
                "asset_id": "BTC",
                "event_ts": "2026-03-26T01:00:05Z",
                "recv_ts": "2026-03-26T01:00:05Z",
                "proc_ts": "2026-03-26T01:00:05Z",
                "best_bid": f"{float(mid_price) - 5:.2f}",
                "best_ask": f"{float(mid_price) + 5:.2f}",
                "mid_price": f"{float(mid_price):.2f}",
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
        )
    assembler.ingest_polymarket_row(
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
    )

    sample_row = {
        "sample_index": 1,
        "sample_started_at": "2026-03-26T01:00:05.000Z",
        "sample_status": "healthy",
        "degraded_sources": [],
        "selected_market_id": "0xmarket",
        "selected_market_slug": "btc-updown-5m-1770000600",
        "selected_window_id": "btc-5m-20260326T010000Z",
        "source_results": {
            "chainlink": {"status": "success", "details": {"fallback_used": False}},
            "polymarket_quotes": {"status": "success", "details": {"seconds_remaining": 295}},
        },
    }

    first_state = assembler.build_state(sample_row)
    second_state = assembler.build_state(sample_row)

    assert first_state is not None
    assert second_state is not None
    assert first_state.snapshot_ts == parse_utc("2026-03-26T01:00:05.000Z")
    assert first_state.window_id == "btc-5m-20260326T010000Z"
    assert first_state.window_start_ts == parse_utc("2026-03-26T01:00:00Z")
    assert first_state.window_end_ts == parse_utc("2026-03-26T01:05:00Z")
    assert first_state.seconds_remaining == 295
    assert first_state.polymarket_market_id == "0xmarket"
    assert first_state.polymarket_slug == "btc-updown-5m-1770000600"
    assert first_state.quote_source == "polymarket"
    assert first_state.chainlink_event_ts == parse_utc("2026-03-26T01:00:04Z")
    assert first_state.exchange_event_ts == parse_utc("2026-03-26T01:00:05Z")
    assert first_state.quote_event_ts == parse_utc("2026-03-26T01:00:05Z")
    assert first_state.quote_recv_ts == parse_utc("2026-03-26T01:00:05Z")
    assert first_state.quote_age_ms == 0
    assert first_state.up_bid_price == Decimal("0.58")
    assert first_state.up_ask_price == Decimal("0.60")
    assert first_state.down_bid_price == Decimal("0.40")
    assert first_state.down_ask_price == Decimal("0.42")
    assert first_state.up_bid_size_contracts == Decimal("50")
    assert first_state.up_ask_size_contracts == Decimal("40")
    assert first_state.down_bid_size_contracts == Decimal("50")
    assert first_state.down_ask_size_contracts == Decimal("40")
    assert first_state.up_spread_abs == Decimal("0.02")
    assert first_state.down_spread_abs == Decimal("0.02")
    assert first_state.market_actionable_flag is True
    assert first_state.open_anchor_present is True
    assert first_state.composite_nowcast_present is True
    assert first_state.exchange_trusted_venue_count == 3
    assert first_state.exchange_rejected_venue_count == 0
    assert dict(first_state.exchange_present_by_venue) == {
        "binance": True,
        "coinbase": True,
        "kraken": True,
    }
    assert dict(first_state.exchange_quote_valid_for_composite_by_venue) == {
        "binance": True,
        "coinbase": True,
        "kraken": True,
    }
    assert dict(first_state.exchange_eligible_by_venue) == {
        "binance": True,
        "coinbase": True,
        "kraken": True,
    }
    assert dict(first_state.exchange_quote_invalid_reason_by_venue) == {
        "binance": None,
        "coinbase": None,
        "kraken": None,
    }
    assert dict(first_state.exchange_ineligible_reason_by_venue) == {
        "binance": None,
        "coinbase": None,
        "kraken": None,
    }
    assert dict(first_state.exchange_normalization_status_by_venue) == {
        "binance": "normalized",
        "coinbase": "normalized",
        "kraken": "normalized",
    }
    assert dict(first_state.exchange_event_age_ms_by_venue) == {
        "binance": 0,
        "coinbase": 0,
        "kraken": 0,
    }
    assert dict(first_state.exchange_recv_age_ms_by_venue) == {
        "binance": 0,
        "coinbase": 0,
        "kraken": 0,
    }
    assert first_state.nowcast_history_length == 1
    assert first_state.volatility_sigma_eff is not None
    assert first_state.state_invalid_reason is None
    assert first_state.state_fingerprint == second_state.state_fingerprint


def test_state_assembler_allows_missing_event_ts_quote_for_shadow_live_composite() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_chainlink_row(
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
    )
    for venue_id, instrument_id, mid_price, normalization_status in (
        ("binance", "binance:spot:BTCUSDT", "70500.0", "normalized_with_missing_event_ts"),
        ("coinbase", "coinbase:spot:BTC-USD", "70510.0", "normalized"),
        ("kraken", "kraken:spot:BTC-USD", "70520.0", "normalized"),
    ):
        assembler.ingest_exchange_row(
            {
                "venue_id": venue_id,
                "instrument_id": instrument_id,
                "asset_id": "BTC",
                "event_ts": "2026-03-26T01:00:05Z",
                "recv_ts": "2026-03-26T01:00:05Z",
                "proc_ts": "2026-03-26T01:00:05Z",
                "best_bid": f"{float(mid_price) - 5:.2f}",
                "best_ask": f"{float(mid_price) + 5:.2f}",
                "mid_price": f"{float(mid_price):.2f}",
                "bid_size": "1.0",
                "ask_size": "1.0",
                "raw_event_id": f"raw-{venue_id}",
                "normalizer_version": "0.1.0",
                "schema_version": "0.1.0",
                "created_ts": "2026-03-26T01:00:05Z",
                "quote_type": "book",
                "quote_depth_level": 1,
                "sequence_id": f"{venue_id}-1",
                "source_event_missing_ts_flag": normalization_status != "normalized",
                "crossed_market_flag": False,
                "locked_market_flag": False,
                "normalization_status": normalization_status,
            }
        )
    assembler.ingest_polymarket_row(
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
    )

    state = assembler.build_state(
        {
            "sample_started_at": "2026-03-26T01:00:05.000Z",
            "sample_status": "healthy",
            "degraded_sources": [],
            "selected_market_id": "0xmarket",
            "selected_market_slug": "btc-updown-5m-1770000600",
            "selected_window_id": "btc-5m-20260326T010000Z",
            "source_results": {
                "chainlink": {"status": "success", "details": {"fallback_used": False}},
                "polymarket_quotes": {"status": "success", "details": {"seconds_remaining": 295}},
            },
        }
    )

    assert state is not None
    assert state.exchange_quote_valid_for_composite_by_venue["binance"] is True
    assert state.exchange_quote_invalid_reason_by_venue["binance"] is None
    assert state.exchange_eligible_by_venue["binance"] is True
    assert state.exchange_ineligible_reason_by_venue["binance"] is None
    assert (
        state.exchange_normalization_status_by_venue["binance"]
        == "normalized_with_missing_event_ts"
    )
    assert state.exchange_trusted_venue_count == 3
    assert state.state_invalid_reason is None


def test_state_assembler_uses_recv_age_as_primary_live_shadow_freshness_gate() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_chainlink_row(
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
    )
    exchange_rows = (
        {
            "venue_id": "binance",
            "instrument_id": "binance:spot:BTCUSDT",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:05Z",
            "recv_ts": "2026-03-26T01:00:05Z",
            "proc_ts": "2026-03-26T01:00:05Z",
            "best_bid": "70495.00",
            "best_ask": "70505.00",
            "mid_price": "70500.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-binance",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:05Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "binance-1",
            "source_event_missing_ts_flag": True,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized_with_missing_event_ts",
        },
        {
            "venue_id": "coinbase",
            "instrument_id": "coinbase:spot:BTC-USD",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:03Z",
            "recv_ts": "2026-03-26T01:00:05Z",
            "proc_ts": "2026-03-26T01:00:05Z",
            "best_bid": "70504.00",
            "best_ask": "70506.00",
            "mid_price": "70505.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-coinbase",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:05Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "coinbase-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        },
        {
            "venue_id": "kraken",
            "instrument_id": "kraken:spot:BTC-USD",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:05Z",
            "recv_ts": "2026-03-26T01:00:05Z",
            "proc_ts": "2026-03-26T01:00:05Z",
            "best_bid": "70509.00",
            "best_ask": "70511.00",
            "mid_price": "70510.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-kraken",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:05Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "kraken-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        },
    )
    for row in exchange_rows:
        assembler.ingest_exchange_row(row)
    assembler.ingest_polymarket_row(
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
    )
    state = assembler.build_state(
        {
            "sample_started_at": "2026-03-26T01:00:05.000Z",
            "sample_status": "healthy",
            "degraded_sources": [],
            "selected_market_id": "0xmarket",
            "selected_market_slug": "btc-updown-5m-1770000600",
            "selected_window_id": "btc-5m-20260326T010000Z",
            "source_results": {
                "chainlink": {"status": "success", "details": {"fallback_used": False}},
                "polymarket_quotes": {"status": "success", "details": {"seconds_remaining": 295}},
            },
        }
    )

    assert state is not None
    assert state.exchange_event_age_ms_by_venue["coinbase"] == 2000
    assert state.exchange_recv_age_ms_by_venue["coinbase"] == 0
    assert state.exchange_quote_valid_for_composite_by_venue["coinbase"] is True
    assert state.exchange_eligible_by_venue["coinbase"] is True
    assert state.exchange_ineligible_reason_by_venue["coinbase"] is None
    assert state.exchange_trusted_venue_count == 3
    assert state.state_invalid_reason is None


def test_state_assembler_keeps_event_age_as_hard_cap_for_live_shadow_freshness() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_exchange_row(
        {
            "venue_id": "coinbase",
            "instrument_id": "coinbase:spot:BTC-USD",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:00Z",
            "recv_ts": "2026-03-26T01:00:11Z",
            "proc_ts": "2026-03-26T01:00:11Z",
            "best_bid": "70504.00",
            "best_ask": "70506.00",
            "mid_price": "70505.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-coinbase",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:11Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "coinbase-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        }
    )

    diagnostics = _build_exchange_venue_diagnostics(
        assembler.state_cache.latest_exchange_by_venue,
        decision_ts=parse_utc("2026-03-26T01:00:12Z"),
    )

    assert diagnostics.quote_valid_for_composite_by_venue["coinbase"] is False
    assert diagnostics.quote_invalid_reason_by_venue["coinbase"] == "event_age_hard_cap_exceeded"
    assert diagnostics.eligible_by_venue["coinbase"] is False
    assert diagnostics.ineligible_reason_by_venue["coinbase"] == "event_age_hard_cap_exceeded"


def test_state_assembler_falls_back_to_window_helper_for_seconds_remaining() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_polymarket_row(
        {
            "venue_id": "polymarket",
            "market_id": "0xmarket",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:01:00Z",
            "recv_ts": "2026-03-26T01:01:01Z",
            "proc_ts": "2026-03-26T01:01:01Z",
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
            "created_ts": "2026-03-26T01:01:01Z",
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
    )

    state = assembler.build_state(
        {
            "sample_started_at": "2026-03-26T01:01:40.000Z",
            "sample_status": "healthy",
            "degraded_sources": [],
            "selected_market_id": "0xmarket",
            "selected_market_slug": "btc-updown-5m-1770000600",
            "selected_window_id": "btc-5m-20260326T010000Z",
            "source_results": {
                "chainlink": {"status": "success", "details": {"fallback_used": False}},
                "polymarket_quotes": {"status": "success", "details": {}},
            },
        }
    )

    assert state is not None
    assert state.seconds_remaining == 200


def test_state_assembler_marks_future_recv_visibility_leak_explicitly() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_chainlink_row(
        {
            "event_id": "chain-1",
            "event_ts": "2026-03-26T01:00:08Z",
            "price": "70000",
            "recv_ts": "2026-03-26T01:00:08Z",
            "oracle_source": "chainlink_stream_public_delayed",
            "oracle_feed_id": "chainlink:stream:BTC-USD",
            "round_id": None,
            "bid_price": "69999",
            "ask_price": "70001",
        }
    )
    assembler.ingest_exchange_row(
        {
            "venue_id": "binance",
            "instrument_id": "binance:spot:BTCUSDT",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:08Z",
            "recv_ts": "2026-03-26T01:00:08Z",
            "proc_ts": "2026-03-26T01:00:08Z",
            "best_bid": "70495.00",
            "best_ask": "70505.00",
            "mid_price": "70500.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-binance",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:08Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "binance-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        }
    )
    assembler.ingest_polymarket_row(
        {
            "venue_id": "polymarket",
            "market_id": "0xmarket",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:08Z",
            "recv_ts": "2026-03-26T01:00:08Z",
            "proc_ts": "2026-03-26T01:00:08Z",
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
            "created_ts": "2026-03-26T01:00:08Z",
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
    )

    state = assembler.build_state(
        {
            "sample_started_at": "2026-03-26T01:00:05.000Z",
            "sample_status": "healthy",
            "degraded_sources": [],
            "selected_market_id": "0xmarket",
            "selected_market_slug": "btc-updown-5m-1770000600",
            "selected_window_id": "btc-5m-20260326T010000Z",
            "source_results": {
                "chainlink": {"status": "success", "details": {"fallback_used": False}},
                "polymarket_quotes": {"status": "success", "details": {"seconds_remaining": 295}},
            },
        }
    )

    assert state is not None
    assert state.state_invalid_reason == NoTradeReason.FUTURE_RECV_VISIBILITY_LEAK
    assert NoTradeReason.FUTURE_RECV_VISIBILITY_LEAK.value in state.state_diagnostics
    assert (
        "future_recv_visibility_leak:quote_recv_ts:1000ms+"
        in state.state_diagnostics
    )


def test_state_assembler_marks_future_event_clock_skew_explicitly() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_chainlink_row(
        {
            "event_id": "chain-1",
            "event_ts": "2026-03-26T01:00:05.250Z",
            "price": "70000",
            "recv_ts": "2026-03-26T01:00:05.000Z",
            "oracle_source": "chainlink_stream_public_delayed",
            "oracle_feed_id": "chainlink:stream:BTC-USD",
            "round_id": None,
            "bid_price": "69999",
            "ask_price": "70001",
        }
    )
    assembler.ingest_exchange_row(
        {
            "venue_id": "binance",
            "instrument_id": "binance:spot:BTCUSDT",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:05.300Z",
            "recv_ts": "2026-03-26T01:00:05.000Z",
            "proc_ts": "2026-03-26T01:00:05.000Z",
            "best_bid": "70495.00",
            "best_ask": "70505.00",
            "mid_price": "70500.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-binance",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:05.000Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "binance-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        }
    )
    assembler.ingest_exchange_row(
        {
            "venue_id": "coinbase",
            "instrument_id": "coinbase:spot:BTC-USD",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:05.300Z",
            "recv_ts": "2026-03-26T01:00:05.000Z",
            "proc_ts": "2026-03-26T01:00:05.000Z",
            "best_bid": "70505.00",
            "best_ask": "70515.00",
            "mid_price": "70510.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-coinbase",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:05.000Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "coinbase-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        }
    )
    assembler.ingest_exchange_row(
        {
            "venue_id": "kraken",
            "instrument_id": "kraken:spot:BTC-USD",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:05.300Z",
            "recv_ts": "2026-03-26T01:00:05.000Z",
            "proc_ts": "2026-03-26T01:00:05.000Z",
            "best_bid": "70515.00",
            "best_ask": "70525.00",
            "mid_price": "70520.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-kraken",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:05.000Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "kraken-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        }
    )
    assembler.ingest_polymarket_row(
        {
            "venue_id": "polymarket",
            "market_id": "0xmarket",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:05.250Z",
            "recv_ts": "2026-03-26T01:00:05.000Z",
            "proc_ts": "2026-03-26T01:00:05.000Z",
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
            "created_ts": "2026-03-26T01:00:05.000Z",
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
    )

    state = assembler.build_state(
        {
            "sample_started_at": "2026-03-26T01:00:05.000Z",
            "sample_status": "healthy",
            "degraded_sources": [],
            "selected_market_id": "0xmarket",
            "selected_market_slug": "btc-updown-5m-1770000600",
            "selected_window_id": "btc-5m-20260326T010000Z",
            "source_results": {
                "chainlink": {"status": "success", "details": {"fallback_used": False}},
                "polymarket_quotes": {"status": "success", "details": {"seconds_remaining": 295}},
            },
        }
    )

    assert state is not None
    assert state.state_invalid_reason == NoTradeReason.FUTURE_EVENT_CLOCK_SKEW
    assert NoTradeReason.FUTURE_EVENT_CLOCK_SKEW.value in state.state_diagnostics
    assert "future_event_clock_skew:quote_event_ts:0-250ms" in state.state_diagnostics


def test_state_assembler_records_per_venue_ineligible_reasons() -> None:
    assembler = CaptureOutputStateAssembler(session_id="20260326T010000000Z")
    assembler.ingest_exchange_row(
        {
            "venue_id": "binance",
            "instrument_id": "binance:spot:BTCUSDT",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T01:00:05Z",
            "recv_ts": "2026-03-26T01:00:05Z",
            "proc_ts": "2026-03-26T01:00:05Z",
            "best_bid": "70495.00",
            "best_ask": "70505.00",
            "mid_price": "70500.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-binance",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T01:00:05Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "binance-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        }
    )
    assembler.ingest_exchange_row(
        {
            "venue_id": "coinbase",
            "instrument_id": "coinbase:spot:BTC-USD",
            "asset_id": "BTC",
            "event_ts": "2026-03-26T00:59:59Z",
            "recv_ts": "2026-03-26T00:59:59Z",
            "proc_ts": "2026-03-26T00:59:59Z",
            "best_bid": "70505.00",
            "best_ask": "70515.00",
            "mid_price": "70510.00",
            "bid_size": "1.0",
            "ask_size": "1.0",
            "raw_event_id": "raw-coinbase",
            "normalizer_version": "0.1.0",
            "schema_version": "0.1.0",
            "created_ts": "2026-03-26T00:59:59Z",
            "quote_type": "book",
            "quote_depth_level": 1,
            "sequence_id": "coinbase-1",
            "source_event_missing_ts_flag": False,
            "crossed_market_flag": False,
            "locked_market_flag": False,
            "normalization_status": "normalized",
        }
    )
    assembler.ingest_polymarket_row(
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
    )

    state = assembler.build_state(
        {
            "sample_started_at": "2026-03-26T01:00:05.000Z",
            "sample_status": "healthy",
            "degraded_sources": [],
            "selected_market_id": "0xmarket",
            "selected_market_slug": "btc-updown-5m-1770000600",
            "selected_window_id": "btc-5m-20260326T010000Z",
            "source_results": {
                "chainlink": {"status": "success", "details": {"fallback_used": False}},
                "polymarket_quotes": {"status": "success", "details": {"seconds_remaining": 295}},
            },
        }
    )

    assert state is not None
    assert dict(state.exchange_present_by_venue) == {
        "binance": True,
        "coinbase": True,
        "kraken": False,
    }
    assert dict(state.exchange_eligible_by_venue) == {
        "binance": True,
        "coinbase": False,
        "kraken": False,
    }
    assert dict(state.exchange_ineligible_reason_by_venue) == {
        "binance": None,
        "coinbase": "stale_source",
        "kraken": "missing_from_cache",
    }
