from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.ledger import ShadowLedger
from rtds.execution.models import (
    BOOK_SIDE_ASK,
    ExecutableStateView,
    ShadowDecision,
    TradabilityCheck,
)
from rtds.execution.writer import (
    SHADOW_DECISIONS_FILENAME,
    SHADOW_SUMMARY_FILENAME,
    ShadowArtifactWriter,
    shadow_artifact_paths,
)


def _build_state_view() -> ExecutableStateView:
    return ExecutableStateView(
        session_id="20260325T230000000Z",
        state_source_kind="live_state",
        snapshot_ts=datetime(2026, 3, 25, 23, 0, tzinfo=UTC),
        window_id="btc-5m-20260325T230000Z",
        window_start_ts=datetime(2026, 3, 25, 23, 0, tzinfo=UTC),
        window_end_ts=datetime(2026, 3, 25, 23, 5, tzinfo=UTC),
        seconds_remaining=210,
        polymarket_market_id="0xshadow",
        polymarket_slug="btc-updown-5m-1770000300",
        clob_token_id_up="up-token",
        clob_token_id_down="down-token",
        window_quality_regime="good",
        chainlink_confidence_state="high",
        volatility_regime="mid_vol",
        fair_value_base=Decimal("0.59"),
        calibrated_fair_value_base=Decimal("0.61"),
        calibration_bucket="far_up",
        calibration_support_flag="sufficient",
        quote_source="polymarket",
        quote_event_ts=datetime(2026, 3, 25, 23, 0, tzinfo=UTC),
        quote_recv_ts=datetime(2026, 3, 25, 23, 0, 0, 1000, tzinfo=UTC),
        quote_age_ms=15,
        up_bid_price=Decimal("0.55"),
        up_ask_price=Decimal("0.57"),
        down_bid_price=Decimal("0.43"),
        down_ask_price=Decimal("0.45"),
        up_bid_size_contracts=Decimal("20"),
        up_ask_size_contracts=Decimal("20"),
        down_bid_size_contracts=Decimal("20"),
        down_ask_size_contracts=Decimal("20"),
        up_spread_abs=Decimal("0.02"),
        down_spread_abs=Decimal("0.02"),
    )


def _build_decision(*, actionable: bool, decision_side: Side = Side.UP) -> ShadowDecision:
    state_view = _build_state_view()
    if not actionable:
        state_view = replace(
            state_view,
            snapshot_ts=datetime(2026, 3, 25, 23, 0, 1, tzinfo=UTC),
            state_fingerprint=None,
        )
    tradability_check = TradabilityCheck(
        policy_mode=PolicyMode.BASELINE,
        intended_side=decision_side,
        intended_book_side=BOOK_SIDE_ASK,
        intended_entry_price=Decimal("0.57") if actionable else None,
        displayed_entry_size_contracts=Decimal("20") if actionable else None,
        target_size_contracts=Decimal("10"),
        selected_net_edge=Decimal("0.04") if actionable else None,
        selected_spread_abs=Decimal("0.02") if actionable else None,
        quote_age_ms=15,
        is_actionable=actionable,
        no_trade_reason=None if actionable else NoTradeReason.EDGE_BELOW_THRESHOLD,
    )
    return ShadowDecision(
        executable_state=state_view,
        policy_mode=PolicyMode.BASELINE,
        tradability_check=tradability_check,
        decision_ts=state_view.snapshot_ts,
        intended_side=decision_side,
    )


def test_shadow_artifact_paths_follow_frozen_tree(tmp_path) -> None:
    paths = shadow_artifact_paths("session123", root_dir=tmp_path / "artifacts/shadow")

    assert paths.session_dir == tmp_path / "artifacts/shadow" / "session123"
    assert paths.shadow_decisions_path.name == SHADOW_DECISIONS_FILENAME
    assert paths.shadow_summary_path.name == SHADOW_SUMMARY_FILENAME


def test_writer_appends_decisions_and_writes_atomic_summary(tmp_path) -> None:
    writer = ShadowArtifactWriter(
        session_id="20260325T230000000Z",
        root_dir=tmp_path / "artifacts/shadow",
    )
    ledger = ShadowLedger(
        session_id="20260325T230000000Z",
        policy_mode=PolicyMode.BASELINE,
    )
    actionable = _build_decision(actionable=True)
    blocked = _build_decision(actionable=False, decision_side=Side.DOWN)

    ledger.record_decision_seen(actionable)
    writer.append_shadow_decision(actionable)
    ledger.record_decision_written(actionable)

    ledger.record_decision_seen(blocked)
    writer.append_shadow_decision(blocked)
    ledger.record_decision_written(blocked)

    summary = ledger.build_summary()
    writer.write_shadow_summary(summary)

    decisions_lines = writer.paths.shadow_decisions_path.read_text(encoding="utf-8").splitlines()
    assert len(decisions_lines) == 2
    assert json.loads(decisions_lines[0])["decision_id"] == actionable.decision_id
    assert json.loads(decisions_lines[1])["decision_id"] == blocked.decision_id

    summary_payload = json.loads(writer.paths.shadow_summary_path.read_text(encoding="utf-8"))
    assert summary_payload["decision_count"] == 2
    assert summary_payload["actionable_decision_count"] == 1
    assert summary_payload["no_trade_count"] == 1
    assert summary_payload["no_trade_reason_counts"] == {
        NoTradeReason.EDGE_BELOW_THRESHOLD.value: 1
    }


def test_writer_requires_schema_types(tmp_path) -> None:
    writer = ShadowArtifactWriter(
        session_id="20260325T230000000Z",
        root_dir=tmp_path / "artifacts/shadow",
    )

    with pytest.raises(TypeError, match="ShadowDecision"):
        writer.append_shadow_decision({"bad": "row"})  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="ShadowSummary"):
        writer.write_shadow_summary({"bad": "summary"})  # type: ignore[arg-type]


def test_ledger_requires_seen_before_written(tmp_path) -> None:
    _ = tmp_path
    ledger = ShadowLedger(
        session_id="20260325T230000000Z",
        policy_mode=PolicyMode.BASELINE,
    )
    decision = _build_decision(actionable=True)

    with pytest.raises(ValueError, match="seen before written"):
        ledger.record_decision_written(decision)
