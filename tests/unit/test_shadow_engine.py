from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from rtds.execution.adapters import ADAPTER_ROLE_LIVE_STATE, AdapterDescriptor
from rtds.execution.enums import PolicyMode
from rtds.execution.models import ExecutableStateView
from rtds.execution.shadow_engine import ShadowEngine, ShadowEngineConfig
from rtds.execution.sizing import SIZE_MODE_FIXED_CONTRACTS, SizingPolicy


def _build_state_view() -> ExecutableStateView:
    return ExecutableStateView(
        session_id="20260326T000000000Z",
        state_source_kind="live_state",
        snapshot_ts=datetime(2026, 3, 26, 0, 0, tzinfo=UTC),
        window_id="btc-5m-20260326T000000Z",
        window_start_ts=datetime(2026, 3, 26, 0, 0, tzinfo=UTC),
        window_end_ts=datetime(2026, 3, 26, 0, 5, tzinfo=UTC),
        seconds_remaining=240,
        polymarket_market_id="0xengine",
        polymarket_slug="btc-updown-5m-1770000600",
        clob_token_id_up="up-token",
        clob_token_id_down="down-token",
        window_quality_regime="good",
        chainlink_confidence_state="high",
        volatility_regime="mid_vol",
        fair_value_base=Decimal("0.58"),
        calibrated_fair_value_base=Decimal("0.61"),
        calibration_bucket="far_up",
        calibration_support_flag="sufficient",
        quote_source="polymarket",
        quote_event_ts=datetime(2026, 3, 26, 0, 0, tzinfo=UTC),
        quote_recv_ts=datetime(2026, 3, 26, 0, 0, 0, 1000, tzinfo=UTC),
        quote_age_ms=12,
        up_bid_price=Decimal("0.54"),
        up_ask_price=Decimal("0.56"),
        down_bid_price=Decimal("0.44"),
        down_ask_price=Decimal("0.46"),
        up_bid_size_contracts=Decimal("40"),
        up_ask_size_contracts=Decimal("25"),
        down_bid_size_contracts=Decimal("40"),
        down_ask_size_contracts=Decimal("25"),
        up_spread_abs=Decimal("0.02"),
        down_spread_abs=Decimal("0.02"),
    )


class FakeLiveAdapter:
    descriptor = AdapterDescriptor(
        adapter_name="fake-live",
        adapter_role=ADAPTER_ROLE_LIVE_STATE,
        production_safe=True,
    )

    def __init__(self, states: list[ExecutableStateView | Exception]) -> None:
        self._states = deque(states)
        self.closed = False

    def read_state(self) -> ExecutableStateView | None:
        if not self._states:
            return None
        value = self._states.popleft()
        if isinstance(value, Exception):
            raise value
        return value

    def close(self) -> None:
        self.closed = True


def test_shadow_engine_processes_state_and_writes_shadow_artifacts(tmp_path) -> None:
    adapter = FakeLiveAdapter([_build_state_view()])
    engine = ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id="20260326T000000000Z",
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts=Decimal("10"),
            ),
            min_net_edge=Decimal("0.03"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )

    processed = engine.process_next_state()

    assert processed is True
    decisions_path = Path(tmp_path / "artifacts/shadow/20260326T000000000Z/shadow_decisions.jsonl")
    summary_path = Path(tmp_path / "artifacts/shadow/20260326T000000000Z/shadow_summary.json")
    assert decisions_path.exists()
    assert summary_path.exists()
    decision_payload = json.loads(decisions_path.read_text(encoding="utf-8").splitlines()[0])
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert decision_payload["decision_id"].startswith("shadowdec:")
    assert summary_payload["decision_count"] == 1
    assert summary_payload["actionable_decision_count"] == 1


def test_shadow_engine_isolates_processing_exceptions_and_keeps_running(tmp_path) -> None:
    bad_state = replace(
        _build_state_view(),
        fair_value_base=None,
        calibrated_fair_value_base=None,
        state_fingerprint=None,
    )
    good_state = replace(
        _build_state_view(),
        snapshot_ts=datetime(2026, 3, 26, 0, 0, 1, tzinfo=UTC),
        state_fingerprint=None,
    )
    adapter = FakeLiveAdapter([RuntimeError("adapter read failed"), bad_state, good_state])
    engine = ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id="20260326T000000000Z",
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts=Decimal("10"),
            ),
            min_net_edge=Decimal("0.03"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )

    assert engine.process_next_state() is False
    assert engine.process_next_state() is True
    assert engine.process_next_state() is True
    assert engine.stats.error_count == 1
    assert engine.stats.decision_count == 2


def test_shadow_engine_run_closes_adapter_and_logs_heartbeat(tmp_path, caplog) -> None:
    adapter = FakeLiveAdapter([_build_state_view()])
    engine = ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id="20260326T000000000Z",
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts=Decimal("10"),
            ),
            min_net_edge=Decimal("0.03"),
            max_quote_age_ms=100,
            max_spread_abs=Decimal("0.03"),
            heartbeat_interval_seconds=1,
            idle_sleep_seconds=0,
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )
    engine._last_heartbeat_monotonic = 0

    with caplog.at_level(logging.INFO, logger="rtds.execution.shadow_engine"):
        engine.run(max_iterations=2)

    assert adapter.closed is True
    assert "shadow heartbeat" in caplog.text
