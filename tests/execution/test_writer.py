from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.ledger import ShadowLedger
from rtds.execution.reconciler import (
    ReplayExpectation,
    WindowResolution,
    reconcile_shadow_decisions,
)
from rtds.execution.writer import (
    SHADOW_DECISIONS_FILENAME,
    SHADOW_ORDER_STATES_FILENAME,
    SHADOW_OUTCOMES_FILENAME,
    SHADOW_SUMMARY_FILENAME,
    SHADOW_VS_REPLAY_FILENAME,
    ShadowArtifactWriter,
    shadow_artifact_paths,
)
from tests.execution.support import build_shadow_decision


def test_shadow_artifact_paths_follow_frozen_tree(tmp_path) -> None:
    paths = shadow_artifact_paths("session123", root_dir=tmp_path / "artifacts/shadow")

    assert paths.session_dir == tmp_path / "artifacts/shadow" / "session123"
    assert paths.shadow_decisions_path.name == SHADOW_DECISIONS_FILENAME
    assert paths.shadow_order_states_path.name == SHADOW_ORDER_STATES_FILENAME
    assert paths.shadow_outcomes_path.name == SHADOW_OUTCOMES_FILENAME
    assert paths.shadow_summary_path.name == SHADOW_SUMMARY_FILENAME
    assert paths.shadow_vs_replay_path.name == SHADOW_VS_REPLAY_FILENAME


def test_writer_appends_decisions_and_writes_atomic_summary(tmp_path) -> None:
    writer = ShadowArtifactWriter(
        session_id="20260326T000000000Z",
        root_dir=tmp_path / "artifacts/shadow",
    )
    ledger = ShadowLedger(
        session_id="20260326T000000000Z",
        policy_mode=PolicyMode.BASELINE,
    )
    actionable = build_shadow_decision(actionable=True)
    blocked = build_shadow_decision(
        actionable=False,
        state=replace(
            actionable.executable_state,
            snapshot_ts=datetime(2026, 3, 26, 0, 0, 1, tzinfo=UTC),
            state_fingerprint=None,
        ),
        intended_side=Side.DOWN,
        reason=NoTradeReason.EDGE_BELOW_THRESHOLD,
    )

    actionable_seen = ledger.record_decision_seen(actionable)
    writer.append_shadow_order_state(actionable_seen)
    writer.append_shadow_decision(actionable)
    actionable_written = ledger.record_decision_written(actionable)
    writer.append_shadow_order_state(actionable_written)

    blocked_seen = ledger.record_decision_seen(blocked)
    writer.append_shadow_order_state(blocked_seen)
    writer.append_shadow_decision(blocked)
    blocked_written = ledger.record_decision_written(blocked)
    writer.append_shadow_order_state(blocked_written)

    writer.write_shadow_summary(ledger.build_summary())

    decisions_lines = writer.paths.shadow_decisions_path.read_text(encoding="utf-8").splitlines()
    assert len(decisions_lines) == 2
    assert json.loads(decisions_lines[0])["decision_id"] == actionable.decision_id
    assert json.loads(decisions_lines[1])["decision_id"] == blocked.decision_id

    summary_payload = json.loads(writer.paths.shadow_summary_path.read_text(encoding="utf-8"))
    assert summary_payload["decision_count"] == 2
    assert summary_payload["actionable_decision_count"] == 1
    assert summary_payload["no_trade_count"] == 1
    assert summary_payload["written_decision_count"] == 2
    assert summary_payload["no_trade_reason_counts"] == {
        NoTradeReason.EDGE_BELOW_THRESHOLD.value: 1
    }
    assert summary_payload["tradability_pass_rate"] == "0.5"
    assert summary_payload["size_coverage_pass_rate"] == "1"

    order_state_lines = writer.paths.shadow_order_states_path.read_text(
        encoding="utf-8"
    ).splitlines()
    assert len(order_state_lines) == 4
    assert json.loads(order_state_lines[0])["transition_name"] == "decision_seen"
    assert json.loads(order_state_lines[1])["transition_name"] == "decision_written"


def test_writer_requires_schema_types(tmp_path) -> None:
    writer = ShadowArtifactWriter(
        session_id="20260326T000000000Z",
        root_dir=tmp_path / "artifacts/shadow",
    )

    with pytest.raises(TypeError, match="ShadowDecision"):
        writer.append_shadow_decision({"bad": "row"})  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="ShadowSummary"):
        writer.write_shadow_summary({"bad": "summary"})  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="ShadowOrderState"):
        writer.append_shadow_order_state({"bad": "row"})  # type: ignore[arg-type]


def test_ledger_requires_seen_before_written() -> None:
    ledger = ShadowLedger(
        session_id="20260326T000000000Z",
        policy_mode=PolicyMode.BASELINE,
    )
    decision = build_shadow_decision(actionable=True)

    with pytest.raises(ValueError, match="seen before written"):
        ledger.record_decision_written(decision)


def test_writer_appends_outcomes_and_writes_shadow_vs_replay_summary(tmp_path) -> None:
    writer = ShadowArtifactWriter(
        session_id="20260326T000000000Z",
        root_dir=tmp_path / "artifacts/shadow",
    )
    ledger = ShadowLedger(
        session_id="20260326T000000000Z",
        policy_mode=PolicyMode.BASELINE,
    )
    actionable = build_shadow_decision(actionable=True)

    writer.append_shadow_order_state(ledger.record_decision_seen(actionable))
    writer.append_shadow_decision(actionable)
    writer.append_shadow_order_state(ledger.record_decision_written(actionable))

    reconciliation = reconcile_shadow_decisions(
        ledger=ledger,
        replay_expectations=[
            ReplayExpectation(
                decision_id=actionable.decision_id,
                replay_expected_pnl="0.4",
                replay_expected_roi="0.08",
            )
        ],
        window_resolutions=[
            WindowResolution(
                window_id=actionable.executable_state.window_id,
                outcome_ts=actionable.decision_ts,
                outcome_status="resolved",
                resolved_up=True,
            )
        ],
    )
    outcome = reconciliation.outcomes[0]
    writer.append_shadow_outcome(outcome)
    writer.write_shadow_vs_replay(reconciliation.comparison_summary)

    outcome_payload = json.loads(
        writer.paths.shadow_outcomes_path.read_text(encoding="utf-8").splitlines()[0]
    )
    comparison_payload = json.loads(writer.paths.shadow_vs_replay_path.read_text(encoding="utf-8"))

    assert outcome_payload["decision"]["decision_id"] == actionable.decision_id
    assert comparison_payload["decision_count"] == 1
    assert comparison_payload["reconciled_decision_count"] == 1
    assert comparison_payload["replay_expected_pnl"] == "0.4"
