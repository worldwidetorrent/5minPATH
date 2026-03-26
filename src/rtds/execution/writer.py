"""Append-only shadow artifact writing for execution v0."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

from rtds.execution.models import (
    ShadowDecision,
    ShadowOrderState,
    ShadowOutcome,
    ShadowSummary,
    ShadowVsReplaySummary,
)
from rtds.storage.writer import json_dumps_stable, serialize_value

SHADOW_ROOT_DIRNAME = "artifacts/shadow"
SHADOW_DECISIONS_FILENAME = "shadow_decisions.jsonl"
SHADOW_ORDER_STATES_FILENAME = "shadow_order_states.jsonl"
SHADOW_OUTCOMES_FILENAME = "shadow_outcomes.jsonl"
SHADOW_SUMMARY_FILENAME = "shadow_summary.json"
SHADOW_VS_REPLAY_FILENAME = "shadow_vs_replay.json"


@dataclass(slots=True, frozen=True)
class ShadowArtifactPaths:
    """Frozen execution-v0 shadow artifact tree for one session."""

    session_id: str
    root_dir: Path
    session_dir: Path
    shadow_decisions_path: Path
    shadow_order_states_path: Path
    shadow_outcomes_path: Path
    shadow_summary_path: Path
    shadow_vs_replay_path: Path


def shadow_artifact_paths(
    session_id: str,
    *,
    root_dir: str | Path = SHADOW_ROOT_DIRNAME,
) -> ShadowArtifactPaths:
    """Resolve the frozen v0 shadow artifact tree for one session."""

    normalized_session_id = str(session_id).strip()
    if not normalized_session_id:
        raise ValueError("session_id must be non-empty")
    base_root = Path(root_dir)
    session_dir = base_root / normalized_session_id
    return ShadowArtifactPaths(
        session_id=normalized_session_id,
        root_dir=base_root,
        session_dir=session_dir,
        shadow_decisions_path=session_dir / SHADOW_DECISIONS_FILENAME,
        shadow_order_states_path=session_dir / SHADOW_ORDER_STATES_FILENAME,
        shadow_outcomes_path=session_dir / SHADOW_OUTCOMES_FILENAME,
        shadow_summary_path=session_dir / SHADOW_SUMMARY_FILENAME,
        shadow_vs_replay_path=session_dir / SHADOW_VS_REPLAY_FILENAME,
    )


class ShadowArtifactWriter:
    """Execution-v0 writer for append-only shadow artifacts."""

    def __init__(
        self,
        *,
        session_id: str,
        root_dir: str | Path = SHADOW_ROOT_DIRNAME,
    ) -> None:
        self._paths = shadow_artifact_paths(session_id, root_dir=root_dir)
        self._paths.session_dir.mkdir(parents=True, exist_ok=True)

    @property
    def paths(self) -> ShadowArtifactPaths:
        return self._paths

    def append_shadow_decision(self, decision: ShadowDecision) -> Path:
        """Append one validated shadow decision row."""

        if not isinstance(decision, ShadowDecision):
            raise TypeError("append_shadow_decision requires ShadowDecision")
        return _append_jsonl_dataclass(
            self._paths.shadow_decisions_path,
            decision,
        )

    def write_shadow_summary(self, summary: ShadowSummary) -> Path:
        """Atomically write the current shadow summary."""

        if not isinstance(summary, ShadowSummary):
            raise TypeError("write_shadow_summary requires ShadowSummary")
        return _atomic_write_json_dataclass(
            self._paths.shadow_summary_path,
            summary,
        )

    def append_shadow_order_state(self, order_state: ShadowOrderState) -> Path:
        """Append one validated shadow order-state transition row."""

        if not isinstance(order_state, ShadowOrderState):
            raise TypeError("append_shadow_order_state requires ShadowOrderState")
        return _append_jsonl_dataclass(
            self._paths.shadow_order_states_path,
            order_state,
        )

    def append_shadow_outcome(self, outcome: ShadowOutcome) -> Path:
        """Append one validated shadow outcome row."""

        if not isinstance(outcome, ShadowOutcome):
            raise TypeError("append_shadow_outcome requires ShadowOutcome")
        return _append_jsonl_dataclass(
            self._paths.shadow_outcomes_path,
            outcome,
        )

    def write_shadow_vs_replay(self, summary: ShadowVsReplaySummary) -> Path:
        """Atomically write the current shadow-vs-replay comparison summary."""

        if not isinstance(summary, ShadowVsReplaySummary):
            raise TypeError("write_shadow_vs_replay requires ShadowVsReplaySummary")
        return _atomic_write_json_dataclass(
            self._paths.shadow_vs_replay_path,
            summary,
        )


def _append_jsonl_dataclass(path: Path, row: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = serialize_value(row)
    encoded = f"{json_dumps_stable(payload)}\n"
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return path


def _atomic_write_json_dataclass(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = serialize_value(payload)
    encoded = f"{json_dumps_stable(serialized)}\n"
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        delete=False,
    ) as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = Path(handle.name)
    os.replace(temp_path, path)
    return path


__all__ = [
    "SHADOW_DECISIONS_FILENAME",
    "SHADOW_ORDER_STATES_FILENAME",
    "SHADOW_OUTCOMES_FILENAME",
    "SHADOW_ROOT_DIRNAME",
    "SHADOW_SUMMARY_FILENAME",
    "SHADOW_VS_REPLAY_FILENAME",
    "ShadowArtifactPaths",
    "ShadowArtifactWriter",
    "shadow_artifact_paths",
]
