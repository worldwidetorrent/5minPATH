from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from rtds.execution.enums import NoTradeReason, PolicyMode, Side
from rtds.execution.ledger import ShadowLedger
from rtds.execution.writer import (
    SHADOW_DECISIONS_FILENAME,
    SHADOW_SUMMARY_FILENAME,
    ShadowArtifactWriter,
    shadow_artifact_paths,
)
from tests.execution.support import build_shadow_decision


def test_shadow_artifact_paths_follow_frozen_tree(tmp_path) -> None:
    paths = shadow_artifact_paths("session123", root_dir=tmp_path / "artifacts/shadow")

    assert paths.session_dir == tmp_path / "artifacts/shadow" / "session123"
    assert paths.shadow_decisions_path.name == SHADOW_DECISIONS_FILENAME
    assert paths.shadow_summary_path.name == SHADOW_SUMMARY_FILENAME


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

    ledger.record_decision_seen(actionable)
    writer.append_shadow_decision(actionable)
    ledger.record_decision_written(actionable)

    ledger.record_decision_seen(blocked)
    writer.append_shadow_decision(blocked)
    ledger.record_decision_written(blocked)

    writer.write_shadow_summary(ledger.build_summary())

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
        session_id="20260326T000000000Z",
        root_dir=tmp_path / "artifacts/shadow",
    )

    with pytest.raises(TypeError, match="ShadowDecision"):
        writer.append_shadow_decision({"bad": "row"})  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="ShadowSummary"):
        writer.write_shadow_summary({"bad": "summary"})  # type: ignore[arg-type]


def test_ledger_requires_seen_before_written() -> None:
    ledger = ShadowLedger(
        session_id="20260326T000000000Z",
        policy_mode=PolicyMode.BASELINE,
    )
    decision = build_shadow_decision(actionable=True)

    with pytest.raises(ValueError, match="seen before written"):
        ledger.record_decision_written(decision)
