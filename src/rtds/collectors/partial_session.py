"""Partial-session evaluation from crash-safe capture checkpoints."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from rtds.collectors.admission_summary import build_capture_admission_summary
from rtds.collectors.phase1_capture import (
    CollectorArtifactSet,
    MetadataSelectionDiagnostics,
    Phase1CaptureResult,
    SessionDiagnostics,
)
from rtds.storage.writer import write_json_file

PARTIAL_SESSION_VERDICT_USABLE = "partial_but_usable"
PARTIAL_SESSION_VERDICT_UNUSABLE = "partial_unusable"
PARTIAL_ARTIFACT_COMPLETENESS_REPLAY_READY = "replay_ready"
PARTIAL_ARTIFACT_COMPLETENESS_CHECKPOINT_ONLY = "checkpoint_only"
PARTIAL_USABLE_MIN_CONTIGUOUS_INTERVAL_SECONDS = 1800.0
PARTIAL_USABLE_MIN_WINDOW_COUNT = 12


@dataclass(slots=True, frozen=True)
class PartialSessionEvaluation:
    """Materialized partial-session outputs."""

    partial_summary_path: Path
    summary: dict[str, object]
    partial_admission_path: Path | None
    partial_admission_summary: dict[str, object] | None


def evaluate_partial_capture_session(partial_summary_path: str | Path) -> PartialSessionEvaluation:
    """Build partial summary and partial admission artifacts from a checkpoint file."""

    partial_path = Path(partial_summary_path)
    payload = json.loads(partial_path.read_text(encoding="utf-8"))
    sample_diagnostics_path = Path(str(payload["sample_diagnostics_path"]))
    sample_rows = (
        _read_jsonl_rows(sample_diagnostics_path)
        if sample_diagnostics_path.exists()
        else []
    )
    last_good_interval = _last_good_interval(sample_rows)
    observed_window_ids = sorted(
        {
            str(row["selected_window_id"])
            for row in sample_rows
            if row.get("selected_window_id") is not None
        }
    )

    collector_outputs = dict(payload.get("collector_outputs", {}))
    artifact_completeness = _artifact_completeness(collector_outputs)
    partial_result = _partial_result_from_payload(payload, partial_path)
    partial_admission_summary: dict[str, object] | None = None
    partial_admission_path: Path | None = None
    if artifact_completeness == PARTIAL_ARTIFACT_COMPLETENESS_REPLAY_READY:
        partial_admission_summary = build_capture_admission_summary(partial_result)
        partial_admission_summary["partial_session"] = True
        partial_admission_path = partial_path.with_name("partial_admission_summary.json")
        write_json_file(partial_admission_path, partial_admission_summary)

    verdict, reasons = _partial_session_verdict(
        payload=payload,
        sample_rows=sample_rows,
        observed_window_ids=observed_window_ids,
        last_good_interval=last_good_interval,
        artifact_completeness=artifact_completeness,
        partial_admission_summary=partial_admission_summary,
    )
    summary = {
        "session_id": str(payload["session_id"]),
        "capture_date": str(payload["capture_date"]),
        "summary_partial_path": str(partial_path),
        "sample_diagnostics_path": str(sample_diagnostics_path),
        "lifecycle_state": str(payload.get("lifecycle_state", "failed_cleanly")),
        "termination_reason": payload.get("termination_reason"),
        "artifact_completeness_level": artifact_completeness,
        "sample_count": int(payload.get("sample_count", 0)),
        "last_completed_sample_number": int(payload.get("last_completed_sample_number", 0)),
        "observed_window_count": len(observed_window_ids),
        "observed_window_ids": observed_window_ids,
        "last_good_interval": last_good_interval,
        "collector_outputs": collector_outputs,
        "partial_admission_summary_path": (
            str(partial_admission_path) if partial_admission_path is not None else None
        ),
        "verdict": verdict,
        "verdict_reasons": reasons,
    }
    summary_path = partial_path.with_name("partial_session_summary.json")
    write_json_file(summary_path, summary)
    return PartialSessionEvaluation(
        partial_summary_path=summary_path,
        summary=summary,
        partial_admission_path=partial_admission_path,
        partial_admission_summary=partial_admission_summary,
    )


def _partial_result_from_payload(
    payload: Mapping[str, Any],
    partial_path: Path,
) -> Phase1CaptureResult:
    selector_payload = dict(payload["selector_diagnostics"])
    collector_outputs = dict(payload.get("collector_outputs", {}))
    capture_date = date.fromisoformat(str(payload["capture_date"]))
    collectors = tuple(
        CollectorArtifactSet(
            collector_name=str(name),
            raw_path=Path(str(item["raw_path"])),
            normalized_path=Path(str(item["normalized_path"])),
            raw_row_count=int(item.get("raw_row_count", 0)),
            normalized_row_count=int(item.get("normalized_row_count", 0)),
        )
        for name, item in sorted(collector_outputs.items())
    )
    return Phase1CaptureResult(
        session_id=str(payload["session_id"]),
        capture_date=capture_date,
        selected_market_id=str(payload["selected_market_id"]),
        selected_market_slug=_optional_str(payload.get("selected_market_slug")),
        selected_market_question=None,
        selected_window_id=str(selector_payload["selected_window_id"]),
        selector_diagnostics=MetadataSelectionDiagnostics(
            selected_market_id=str(selector_payload["selected_market_id"]),
            selected_market_slug=_optional_str(selector_payload.get("selected_market_slug")),
            selected_window_id=str(selector_payload["selected_window_id"]),
            candidate_count=int(selector_payload["candidate_count"]),
            admitted_count=int(selector_payload["admitted_count"]),
            rejected_count_by_reason={
                str(key): int(value)
                for key, value in dict(selector_payload["rejected_count_by_reason"]).items()
            },
        ),
        duration_seconds=0.0,
        poll_interval_seconds=0.0,
        sample_count=int(payload.get("sample_count", 0)),
        session_diagnostics=SessionDiagnostics(
            degraded_sample_count=int(payload.get("degraded_sample_count", 0)),
            failed_sample_count=int(payload.get("failed_sample_count", 0)),
            empty_book_count=int(payload.get("empty_book_count", 0)),
            retry_count_by_source=_int_map(payload.get("retry_count_by_source")),
            retry_exhaustion_count_by_source=_int_map(
                payload.get("retry_exhaustion_count_by_source")
            ),
            source_failure_count_by_source=_int_map(payload.get("source_failure_count_by_source")),
            max_consecutive_missing_by_source=_int_map(
                payload.get("max_consecutive_missing_by_source")
            ),
            polymarket_failure_count_by_class=_int_map(
                payload.get("polymarket_failure_count_by_class")
            ),
            polymarket_selector_refresh_count=int(
                payload.get("polymarket_selector_refresh_count", 0)
            ),
            polymarket_selector_rebind_count=int(
                payload.get("polymarket_selector_rebind_count", 0)
            ),
            polymarket_rollover_grace_sample_count=int(
                payload.get("polymarket_rollover_grace_sample_count", 0)
            ),
            termination_reason=str(payload.get("termination_reason", "failed_cleanly")),
            sample_diagnostics_path=Path(str(payload["sample_diagnostics_path"])),
            summary_partial_path=partial_path,
            lifecycle_state=str(payload.get("lifecycle_state", "failed_cleanly")),
            lifecycle_history=tuple(
                dict(item) for item in payload.get("lifecycle_history", [])
            ),
            polymarket_window_coverage=(),
            max_consecutive_unusable_polymarket_windows=1,
            polymarket_unusable_window_min_quote_coverage_ratio=0.5,
        ),
        summary_path=Path(str(payload["summary_path"])),
        collectors=collectors,
    )


def _artifact_completeness(collector_outputs: Mapping[str, Any]) -> str:
    required = ("polymarket_metadata", "chainlink", "exchange", "polymarket_quotes")
    for collector_name in required:
        item = collector_outputs.get(collector_name)
        if not isinstance(item, Mapping):
            return PARTIAL_ARTIFACT_COMPLETENESS_CHECKPOINT_ONLY
        raw_path = Path(str(item.get("raw_path", "")))
        normalized_path = Path(str(item.get("normalized_path", "")))
        if not raw_path.exists() or not normalized_path.exists():
            return PARTIAL_ARTIFACT_COMPLETENESS_CHECKPOINT_ONLY
        if int(item.get("raw_row_count", 0)) <= 0:
            return PARTIAL_ARTIFACT_COMPLETENESS_CHECKPOINT_ONLY
    return PARTIAL_ARTIFACT_COMPLETENESS_REPLAY_READY


def _last_good_interval(sample_rows: list[dict[str, Any]]) -> dict[str, object] | None:
    current_rows: list[dict[str, Any]] = []
    last_good_rows: list[dict[str, Any]] = []
    for row in sample_rows:
        if (
            str(row.get("sample_status", "")) in {"healthy", "degraded"}
            and str(row.get("family_validation_status", "")) != "selection_failed"
        ):
            current_rows.append(row)
            last_good_rows = list(current_rows)
        else:
            current_rows = []
    if not last_good_rows:
        return None
    start = _parse_optional_utc(last_good_rows[0].get("sample_started_at"))
    end = _parse_optional_utc(last_good_rows[-1].get("sample_started_at"))
    if start is None or end is None:
        return None
    return {
        "start_ts": last_good_rows[0]["sample_started_at"],
        "end_ts": last_good_rows[-1]["sample_started_at"],
        "sample_count": len(last_good_rows),
        "duration_seconds": max(0.0, (end - start).total_seconds()),
    }


def _partial_session_verdict(
    *,
    payload: Mapping[str, Any],
    sample_rows: list[dict[str, Any]],
    observed_window_ids: list[str],
    last_good_interval: dict[str, object] | None,
    artifact_completeness: str,
    partial_admission_summary: Mapping[str, Any] | None,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if artifact_completeness != PARTIAL_ARTIFACT_COMPLETENESS_REPLAY_READY:
        reasons.append("collector artifacts are incomplete")
        return PARTIAL_SESSION_VERDICT_UNUSABLE, reasons
    if len(observed_window_ids) < PARTIAL_USABLE_MIN_WINDOW_COUNT:
        reasons.append("too few mapped windows for exploratory replay")
        return PARTIAL_SESSION_VERDICT_UNUSABLE, reasons
    if last_good_interval is None or float(last_good_interval["duration_seconds"]) < float(
        PARTIAL_USABLE_MIN_CONTIGUOUS_INTERVAL_SECONDS
    ):
        reasons.append("contiguous structurally valid interval is too short")
        return PARTIAL_SESSION_VERDICT_UNUSABLE, reasons
    if partial_admission_summary is None:
        reasons.append("partial admission summary could not be built")
        return PARTIAL_SESSION_VERDICT_UNUSABLE, reasons

    family_validation = _mapping(partial_admission_summary.get("family_validation"))
    mapping_and_anchor = _mapping(partial_admission_summary.get("mapping_and_anchor"))
    snapshot_eligibility = _mapping(partial_admission_summary.get("snapshot_eligibility"))
    if int(family_validation.get("off_family_switch_count", 0)) != 0:
        reasons.append("off-family drift detected")
    if int(mapping_and_anchor.get("selected_binding_unresolved_window_count", 0)) != 0:
        reasons.append("unresolved selected bindings detected")
    if float(snapshot_eligibility.get("snapshot_eligible_sample_ratio", 0.0)) < 0.60:
        reasons.append("snapshot eligibility is below exploratory floor")
    if reasons:
        return PARTIAL_SESSION_VERDICT_UNUSABLE, reasons
    return PARTIAL_SESSION_VERDICT_USABLE, [
        "structural continuity held and partial dataset cleared exploratory replay floor"
    ]


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(dict(json.loads(line)))
    return rows


def _int_map(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): int(item) for key, item in value.items()}


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _parse_optional_utc(value: object):
    from rtds.core.time import parse_utc

    if value is None:
        return None
    return parse_utc(str(value))


__all__ = [
    "PARTIAL_SESSION_VERDICT_UNUSABLE",
    "PARTIAL_SESSION_VERDICT_USABLE",
    "evaluate_partial_capture_session",
]
