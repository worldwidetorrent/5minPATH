from __future__ import annotations

from pathlib import Path

from rtds.collectors.partial_session import (
    PARTIAL_SESSION_VERDICT_UNUSABLE,
    PARTIAL_SESSION_VERDICT_USABLE,
    evaluate_partial_capture_session,
)
from rtds.storage.writer import write_json_file, write_jsonl_rows


def test_evaluate_partial_capture_session_marks_checkpoint_only_runs_unusable(
    tmp_path: Path,
) -> None:
    partial_summary_path = tmp_path / "summary.partial.json"
    diagnostics_path = tmp_path / "sample_diagnostics.jsonl"
    write_jsonl_rows(
        diagnostics_path,
        [
            {
                "sample_index": 1,
                "sample_started_at": "2026-03-20T00:00:00Z",
                "sample_status": "healthy",
                "selected_market_id": "0x1",
                "selected_market_slug": "btc-updown-5m-1",
                "selected_window_id": "btc-5m-20260320T000000Z",
                "family_validation_status": "selected",
                "degraded_sources": [],
                "source_results": {},
                "termination_reason": None,
            }
        ],
    )
    write_json_file(
        partial_summary_path,
        {
            "session_id": "partial-session",
            "capture_date": "2026-03-20",
            "summary_path": str(tmp_path / "summary.json"),
            "sample_diagnostics_path": str(diagnostics_path),
            "sample_count": 1,
            "last_completed_sample_number": 1,
            "selected_market_id": "0x1",
            "selected_market_slug": "btc-updown-5m-1",
            "selected_window_id": "btc-5m-20260320T000000Z",
            "selector_diagnostics": {
                "selected_market_id": "0x1",
                "selected_market_slug": "btc-updown-5m-1",
                "selected_window_id": "btc-5m-20260320T000000Z",
                "candidate_count": 1,
                "admitted_count": 1,
                "rejected_count_by_reason": {},
            },
            "collector_outputs": {},
            "lifecycle_state": "failed_cleanly",
            "lifecycle_history": [{"state": "failed_cleanly"}],
            "termination_reason": "uncaught_exception",
        },
    )

    evaluation = evaluate_partial_capture_session(partial_summary_path)

    assert evaluation.summary["artifact_completeness_level"] == "checkpoint_only"
    assert evaluation.summary["verdict"] == PARTIAL_SESSION_VERDICT_UNUSABLE
    assert evaluation.partial_admission_summary is None


def test_evaluate_partial_capture_session_marks_replay_ready_runs_usable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    partial_summary_path = tmp_path / "summary.partial.json"
    diagnostics_path = tmp_path / "sample_diagnostics.jsonl"
    sample_rows = []
    for index in range(12):
        sample_rows.append(
            {
                "sample_index": index + 1,
                "sample_started_at": f"2026-03-20T00:{index * 3:02d}:00Z",
                "sample_status": "healthy",
                "selected_market_id": f"0x{index + 1}",
                "selected_market_slug": f"btc-updown-5m-{index + 1}",
                "selected_window_id": f"btc-5m-20260320T00{index * 3:02d}00Z",
                "family_validation_status": "selected",
                "degraded_sources": [],
                "source_results": {},
                "termination_reason": None,
            }
        )
    write_jsonl_rows(diagnostics_path, sample_rows)

    collector_outputs = {}
    for collector_name in (
        "polymarket_metadata",
        "chainlink",
        "exchange",
        "polymarket_quotes",
    ):
        raw_path = tmp_path / collector_name / "raw.jsonl"
        normalized_path = tmp_path / collector_name / "normalized.jsonl"
        write_jsonl_rows(raw_path, [{"row": 1}])
        write_jsonl_rows(normalized_path, [{"row": 1}])
        collector_outputs[collector_name] = {
            "raw_path": str(raw_path),
            "normalized_path": str(normalized_path),
            "raw_row_count": 1,
            "normalized_row_count": 1,
        }

    write_json_file(
        partial_summary_path,
        {
            "session_id": "partial-session",
            "capture_date": "2026-03-20",
            "summary_path": str(tmp_path / "summary.json"),
            "sample_diagnostics_path": str(diagnostics_path),
            "sample_count": 12,
            "degraded_sample_count": 0,
            "failed_sample_count": 0,
            "empty_book_count": 0,
            "retry_count_by_source": {},
            "retry_exhaustion_count_by_source": {},
            "source_failure_count_by_source": {},
            "max_consecutive_missing_by_source": {},
            "polymarket_failure_count_by_class": {},
            "selected_market_id": "0x1",
            "selected_market_slug": "btc-updown-5m-1",
            "selected_window_id": "btc-5m-20260320T000000Z",
            "selector_diagnostics": {
                "selected_market_id": "0x1",
                "selected_market_slug": "btc-updown-5m-1",
                "selected_window_id": "btc-5m-20260320T000000Z",
                "candidate_count": 1,
                "admitted_count": 1,
                "rejected_count_by_reason": {},
            },
            "collector_outputs": collector_outputs,
            "lifecycle_state": "failed_cleanly",
            "lifecycle_history": [{"state": "failed_cleanly"}],
            "termination_reason": "uncaught_exception",
        },
    )
    monkeypatch.setattr(
        "rtds.collectors.partial_session.build_capture_admission_summary",
        lambda result: {
            "family_validation": {"off_family_switch_count": 0},
            "mapping_and_anchor": {"selected_binding_unresolved_window_count": 0},
            "snapshot_eligibility": {"snapshot_eligible_sample_ratio": 0.8},
        },
    )

    evaluation = evaluate_partial_capture_session(partial_summary_path)

    assert evaluation.summary["artifact_completeness_level"] == "replay_ready"
    assert evaluation.summary["verdict"] == PARTIAL_SESSION_VERDICT_USABLE
    assert evaluation.partial_admission_summary is not None
    assert Path(str(evaluation.summary["partial_admission_summary_path"])).exists()
