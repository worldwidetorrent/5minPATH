"""Helpers for pinned capture-session baselines."""

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


@dataclass(slots=True, frozen=True)
class CaptureSessionBaseline:
    """Pinned expectations for one local capture-session baseline."""

    baseline_id: str
    session_id: str
    capture_date: date
    capture_mode: str
    oracle_source: str
    current_admission_verdict: str
    failure_reason: str
    known_caveat: str
    expected: Mapping[str, Any]


def load_capture_session_baseline(path: str | Path) -> CaptureSessionBaseline:
    """Load one baseline manifest from disk."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return CaptureSessionBaseline(
        baseline_id=str(payload["baseline_id"]),
        session_id=str(payload["session_id"]),
        capture_date=date.fromisoformat(str(payload["capture_date"])),
        capture_mode=str(payload["capture_mode"]),
        oracle_source=str(payload["oracle_source"]),
        current_admission_verdict=str(payload["current_admission_verdict"]),
        failure_reason=str(payload["failure_reason"]),
        known_caveat=str(payload["known_caveat"]),
        expected=dict(payload["expected"]),
    )


def load_capture_result_from_summary(summary_path: str | Path) -> Phase1CaptureResult:
    """Reconstruct a persisted capture result from one summary artifact."""

    path = Path(summary_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    selector_payload = dict(payload["selector_diagnostics"])
    session_payload = dict(payload["session_diagnostics"])
    collectors_payload = list(payload["collectors"])

    return Phase1CaptureResult(
        session_id=str(payload["session_id"]),
        capture_date=date.fromisoformat(str(payload["capture_date"])),
        selected_market_id=str(payload["selected_market_id"]),
        selected_market_slug=_optional_str(payload.get("selected_market_slug")),
        selected_market_question=_optional_str(payload.get("selected_market_question")),
        selected_window_id=str(payload["selected_window_id"]),
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
        duration_seconds=float(payload["duration_seconds"]),
        poll_interval_seconds=float(payload["poll_interval_seconds"]),
        sample_count=int(payload["sample_count"]),
        session_diagnostics=SessionDiagnostics(
            degraded_sample_count=int(session_payload["degraded_sample_count"]),
            failed_sample_count=int(session_payload["failed_sample_count"]),
            empty_book_count=int(session_payload["empty_book_count"]),
            retry_count_by_source=_int_map(session_payload.get("retry_count_by_source")),
            retry_exhaustion_count_by_source=_int_map(
                session_payload.get("retry_exhaustion_count_by_source")
            ),
            source_failure_count_by_source=_int_map(
                session_payload.get("source_failure_count_by_source")
            ),
            max_consecutive_missing_by_source=_int_map(
                session_payload.get("max_consecutive_missing_by_source")
            ),
            polymarket_failure_count_by_class=_int_map(
                session_payload.get("polymarket_failure_count_by_class")
            ),
            polymarket_selector_refresh_count=int(
                session_payload.get("polymarket_selector_refresh_count", 0)
            ),
            polymarket_selector_rebind_count=int(
                session_payload.get("polymarket_selector_rebind_count", 0)
            ),
            polymarket_rollover_grace_sample_count=int(
                session_payload.get("polymarket_rollover_grace_sample_count", 0)
            ),
            termination_reason=str(session_payload["termination_reason"]),
            sample_diagnostics_path=Path(str(session_payload["sample_diagnostics_path"])),
            summary_partial_path=(
                Path(str(session_payload["summary_partial_path"]))
                if session_payload.get("summary_partial_path") is not None
                else None
            ),
            lifecycle_state=str(
                session_payload.get("lifecycle_state", "completed")
            ),
            lifecycle_history=tuple(
                dict(item) for item in session_payload.get("lifecycle_history", [])
            ),
            polymarket_window_coverage=tuple(
                dict(item) for item in session_payload.get("polymarket_window_coverage", [])
            ),
            max_consecutive_unusable_polymarket_windows=int(
                session_payload.get("max_consecutive_unusable_polymarket_windows", 1)
            ),
            polymarket_unusable_window_min_quote_coverage_ratio=float(
                session_payload.get("polymarket_unusable_window_min_quote_coverage_ratio", 0.5)
            ),
        ),
        summary_path=path,
        collectors=tuple(
            CollectorArtifactSet(
                collector_name=str(item["collector_name"]),
                raw_path=Path(str(item["raw_path"])),
                normalized_path=Path(str(item["normalized_path"])),
                raw_row_count=int(item["raw_row_count"]),
                normalized_row_count=int(item["normalized_row_count"]),
            )
            for item in collectors_payload
        ),
    )


def refresh_capture_admission_from_summary(summary_path: str | Path) -> Path:
    """Rebuild and overwrite one admission summary from a stored capture summary."""

    result = load_capture_result_from_summary(summary_path)
    summary = build_capture_admission_summary(result)
    output_path = result.summary_path.with_name("admission_summary.json")
    write_json_file(output_path, summary)
    return output_path


def validate_admission_summary_against_baseline(
    admission_summary: Mapping[str, Any],
    baseline: CaptureSessionBaseline,
) -> list[str]:
    """Compare one regenerated admission summary against a pinned baseline manifest."""

    issues: list[str] = []
    expected = dict(baseline.expected)

    if str(admission_summary.get("session_id")) != baseline.session_id:
        issues.append("session_id mismatch")
    if str(admission_summary.get("capture_date")) != baseline.capture_date.isoformat():
        issues.append("capture_date mismatch")
    if str(admission_summary.get("verdict")) != str(expected["verdict"]):
        issues.append("verdict mismatch")
    if str(admission_summary.get("termination_reason")) != str(expected["termination_reason"]):
        issues.append("termination_reason mismatch")

    family_validation = _mapping(admission_summary.get("family_validation"))
    mapping_and_anchor = _mapping(admission_summary.get("mapping_and_anchor"))
    snapshot_eligibility = _mapping(admission_summary.get("snapshot_eligibility"))
    chainlink_continuity = _mapping(admission_summary.get("chainlink_continuity"))
    polymarket_continuity = _mapping(admission_summary.get("polymarket_continuity"))

    if int(family_validation.get("off_family_switch_count", -1)) != int(
        expected["off_family_switch_count"]
    ):
        issues.append("off_family_switch_count mismatch")
    if int(mapping_and_anchor.get("selected_binding_unresolved_window_count", -1)) != int(
        expected["selected_binding_unresolved_window_count"]
    ):
        issues.append("selected_binding_unresolved_window_count mismatch")
    if int(mapping_and_anchor.get("mapped_window_count", 0)) < int(
        expected["min_mapped_window_count"]
    ):
        issues.append("mapped_window_count below baseline floor")
    if float(snapshot_eligibility.get("snapshot_eligible_sample_ratio", 0.0)) < float(
        expected["min_snapshot_eligible_sample_ratio"]
    ):
        issues.append("snapshot_eligible_sample_ratio below baseline floor")

    anchor_breakdown = _mapping(mapping_and_anchor.get("anchor_assignment_confidence_breakdown"))
    if (
        int(anchor_breakdown.get("high", 0)) + int(anchor_breakdown.get("medium", 0))
        < int(expected["min_high_medium_anchor_windows"])
    ):
        issues.append("high/medium anchor coverage below baseline floor")

    oracle_source_count = _mapping(chainlink_continuity.get("oracle_source_count"))
    if int(oracle_source_count.get(baseline.oracle_source, 0)) < int(
        expected["min_oracle_source_sample_count"]
    ):
        issues.append("oracle source count below baseline floor")

    window_verdict_counts = _mapping(polymarket_continuity.get("window_verdict_counts"))
    if int(window_verdict_counts.get("good", 0)) < int(expected["min_good_window_count"]):
        issues.append("good window count below baseline floor")
    if int(window_verdict_counts.get("unusable", 0)) > int(expected["max_unusable_window_count"]):
        issues.append("unusable window count above baseline ceiling")

    return issues


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


__all__ = [
    "CaptureSessionBaseline",
    "load_capture_result_from_summary",
    "load_capture_session_baseline",
    "refresh_capture_admission_from_summary",
    "validate_admission_summary_against_baseline",
]
