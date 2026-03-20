from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from rtds.collectors.session_baseline import (
    CaptureSessionBaseline,
    load_capture_result_from_summary,
    validate_admission_summary_against_baseline,
)
from rtds.storage.writer import write_json_file


def test_load_capture_result_from_summary_reconstructs_core_fields(tmp_path: Path) -> None:
    summary_path = tmp_path / "artifacts" / "collect" / "summary.json"
    write_json_file(
        summary_path,
        {
            "session_id": "test-session",
            "capture_date": "2026-03-16",
            "selected_market_id": "0x" + "1" * 64,
            "selected_market_slug": "btc-updown-5m-1773655800",
            "selected_market_question": "Bitcoin Up or Down",
            "selected_window_id": "btc-5m-20260316T101000Z",
            "duration_seconds": 60.0,
            "poll_interval_seconds": 1.0,
            "sample_count": 5,
            "selector_diagnostics": {
                "selected_market_id": "0x" + "1" * 64,
                "selected_market_slug": "btc-updown-5m-1773655800",
                "selected_window_id": "btc-5m-20260316T101000Z",
                "candidate_count": 10,
                "admitted_count": 4,
                "rejected_count_by_reason": {"tenor_mismatch": 6},
            },
            "session_diagnostics": {
                "degraded_sample_count": 1,
                "failed_sample_count": 0,
                "empty_book_count": 1,
                "retry_count_by_source": {"exchange": 1},
                "retry_exhaustion_count_by_source": {},
                "source_failure_count_by_source": {"polymarket_quotes": 1},
                "max_consecutive_missing_by_source": {"chainlink": 0, "polymarket_quotes": 0},
                "polymarket_failure_count_by_class": {"valid_empty_book": 1},
                "polymarket_selector_refresh_count": 2,
                "polymarket_selector_rebind_count": 0,
                "polymarket_rollover_grace_sample_count": 3,
                "polymarket_window_coverage": [],
                "max_consecutive_unusable_polymarket_windows": 2,
                "polymarket_unusable_window_min_quote_coverage_ratio": 0.2,
                "termination_reason": "completed",
                "sample_diagnostics_path": str(
                    tmp_path / "artifacts" / "collect" / "samples.jsonl"
                ),
                "summary_partial_path": str(
                    tmp_path / "artifacts" / "collect" / "summary.partial.json"
                ),
                "lifecycle_state": "completed",
                "lifecycle_history": [
                    {"state": "running", "recorded_at": "2026-03-16T10:00:00Z"},
                    {"state": "completed", "recorded_at": "2026-03-16T10:05:00Z"},
                ],
            },
            "collectors": [
                {
                    "collector_name": "chainlink",
                    "raw_path": str(tmp_path / "data" / "raw" / "chainlink" / "part-00000.jsonl"),
                    "normalized_path": str(
                        tmp_path / "data" / "normalized" / "chainlink_ticks" / "part-00000.jsonl"
                    ),
                    "raw_row_count": 5,
                    "normalized_row_count": 5,
                }
            ],
        },
    )

    result = load_capture_result_from_summary(summary_path)

    assert result.session_id == "test-session"
    assert result.capture_date == date(2026, 3, 16)
    assert result.selector_diagnostics.candidate_count == 10
    assert result.session_diagnostics.polymarket_failure_count_by_class == {
        "valid_empty_book": 1
    }
    assert result.session_diagnostics.lifecycle_state == "completed"
    assert result.session_diagnostics.summary_partial_path == (
        tmp_path / "artifacts" / "collect" / "summary.partial.json"
    )
    assert result.collectors[0].collector_name == "chainlink"


def test_validate_admission_summary_against_baseline_accepts_pinned_expectations() -> None:
    baseline = CaptureSessionBaseline(
        baseline_id="capture-baseline-test",
        session_id="test-session",
        capture_date=date(2026, 3, 16),
        capture_mode="pilot",
        oracle_source="chainlink_stream_public_delayed",
        current_admission_verdict="not_admissible",
        failure_reason=(
            "degradation or snapshot coverage fell below conditional admission thresholds"
        ),
        known_caveat="Widespread valid_empty_book degradation",
        expected={
            "verdict": "not_admissible",
            "termination_reason": "completed",
            "off_family_switch_count": 0,
            "selected_binding_unresolved_window_count": 0,
            "min_mapped_window_count": 70,
            "min_snapshot_eligible_sample_ratio": 0.9,
            "min_high_medium_anchor_windows": 28,
            "min_oracle_source_sample_count": 12000,
            "min_good_window_count": 10,
            "max_unusable_window_count": 0,
        },
    )
    admission_summary = json.loads(
        """
        {
          "session_id": "test-session",
          "capture_date": "2026-03-16",
          "verdict": "not_admissible",
          "termination_reason": "completed",
          "family_validation": {"off_family_switch_count": 0},
          "mapping_and_anchor": {
            "selected_binding_unresolved_window_count": 0,
            "mapped_window_count": 73,
            "anchor_assignment_confidence_breakdown": {
              "high": 17,
              "medium": 11,
              "low": 44,
              "none": 1
            }
          },
          "snapshot_eligibility": {"snapshot_eligible_sample_ratio": 0.9305},
          "chainlink_continuity": {
            "oracle_source_count": {"chainlink_stream_public_delayed": 12575}
          },
          "polymarket_continuity": {
            "window_verdict_counts": {
              "good": 12,
              "degraded_light": 20,
              "degraded_medium": 21,
              "degraded_heavy": 20
            }
          }
        }
        """
    )

    issues = validate_admission_summary_against_baseline(admission_summary, baseline)

    assert issues == []
