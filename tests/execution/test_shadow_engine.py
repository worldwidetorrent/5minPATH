from __future__ import annotations

import json
import logging
from pathlib import Path

from rtds.execution.enums import NoTradeReason, PolicyMode
from rtds.execution.shadow_engine import ShadowEngine, ShadowEngineConfig
from rtds.execution.sizing import SIZE_MODE_FIXED_CONTRACTS, SizingPolicy
from tests.execution.support import FakeLiveAdapter, build_state_view


def _build_engine(tmp_path, adapter: FakeLiveAdapter) -> ShadowEngine:
    return ShadowEngine(
        adapter=adapter,
        config=ShadowEngineConfig(
            session_id="20260326T000000000Z",
            policy_name="good_only_baseline",
            policy_role="baseline",
            policy_mode=PolicyMode.BASELINE,
            sizing_policy=SizingPolicy(
                size_mode=SIZE_MODE_FIXED_CONTRACTS,
                fixed_size_contracts="10",
            ),
            min_net_edge="0.03",
            max_quote_age_ms=100,
            max_spread_abs="0.03",
            heartbeat_interval_seconds=1,
            idle_sleep_seconds=0,
            shadow_root_dir=str(tmp_path / "artifacts/shadow"),
        ),
    )


def test_shadow_engine_processes_one_live_row_into_one_decision(tmp_path) -> None:
    engine = _build_engine(tmp_path, FakeLiveAdapter([build_state_view()]))

    processed = engine.process_next_state()

    assert processed is True
    decisions_path = Path(
        tmp_path / "artifacts/shadow/20260326T000000000Z/shadow_decisions.jsonl"
    )
    order_states_path = Path(
        tmp_path / "artifacts/shadow/20260326T000000000Z/shadow_order_states.jsonl"
    )
    summary_path = Path(tmp_path / "artifacts/shadow/20260326T000000000Z/shadow_summary.json")
    assert decisions_path.exists()
    assert order_states_path.exists()
    assert summary_path.exists()
    decision_payload = json.loads(decisions_path.read_text(encoding="utf-8").splitlines()[0])
    order_state_payloads = [
        json.loads(line) for line in order_states_path.read_text(encoding="utf-8").splitlines()
    ]
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert decision_payload["decision_id"].startswith("shadowdec:")
    assert [row["transition_name"] for row in order_state_payloads] == [
        "decision_seen",
        "decision_written",
    ]
    assert summary_payload["decision_count"] == 1
    assert summary_payload["actionable_decision_count"] == 1
    assert summary_payload["written_decision_count"] == 1
    assert engine.stats.decision_count == 1


def test_shadow_engine_propagates_no_trade_reason_into_summary(tmp_path) -> None:
    stale_state = build_state_view(quote_age_ms=500)
    engine = _build_engine(tmp_path, FakeLiveAdapter([stale_state]))

    processed = engine.process_next_state()

    assert processed is True
    summary_path = Path(tmp_path / "artifacts/shadow/20260326T000000000Z/shadow_summary.json")
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["decision_count"] == 1
    assert summary_payload["no_trade_count"] == 1
    assert summary_payload["no_trade_reason_counts"] == {
        NoTradeReason.QUOTE_STALE.value: 1
    }
    assert summary_payload["freshness_pass_rate"] == "0"
    assert engine.stats.last_no_trade_reason == NoTradeReason.QUOTE_STALE.value


def test_shadow_engine_run_closes_adapter_and_logs_heartbeat(tmp_path, caplog) -> None:
    engine = _build_engine(tmp_path, FakeLiveAdapter([build_state_view()]))
    engine._last_heartbeat_monotonic = 0

    with caplog.at_level(logging.INFO, logger="rtds.execution.shadow_engine"):
        engine.run(max_iterations=2)

    assert engine.adapter.closed is True
    assert "shadow heartbeat" in caplog.text
