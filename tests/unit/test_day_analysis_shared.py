from __future__ import annotations

import json
from pathlib import Path

from rtds.cli.day_analysis_shared import (
    build_day_tracker_entry,
    build_shadow_quick_stage_a,
    classify_shadow_baseline,
)


def test_classify_shadow_baseline_prefers_recv_visibility_leak() -> None:
    clean, reason = classify_shadow_baseline(
        {
            "no_trade_reason_counts": {
                "future_recv_visibility_leak": 2,
                "future_state_leak_detected": 1,
            }
        }
    )

    assert clean is False
    assert reason == "future_recv_visibility_leak"


def test_classify_shadow_baseline_supports_historical_leak_reason() -> None:
    clean, reason = classify_shadow_baseline(
        {"no_trade_reason_counts": {"future_state_leak_detected": 4}}
    )

    assert clean is False
    assert reason == "future_state_leak_detected"


def test_build_shadow_quick_stage_a_counts_rows(tmp_path: Path) -> None:
    decisions_path = tmp_path / "shadow_decisions.jsonl"
    rows = [
        {
            "executable_state": {
                "fair_value_base": "0.61",
                "calibrated_fair_value_base": "0.58",
                "exchange_trusted_venue_count": 3,
                "state_diagnostics": {
                    "future_event_clock_skew_bucket": "250-500ms",
                },
            },
            "tradability_check": {"is_actionable": True},
        },
        {
            "executable_state": {
                "fair_value_base": None,
                "calibrated_fair_value_base": None,
                "exchange_trusted_venue_count": 2,
                "state_diagnostics": [
                    "future_event_clock_skew",
                    "future_event_clock_skew:quote_event_ts:500-1000ms",
                ],
            },
            "tradability_check": {"is_actionable": False},
        },
    ]
    decisions_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    summary = build_shadow_quick_stage_a(
        {
            "processing_mode": "live_only_from_attach_ts",
            "backlog_decision_count": 0,
            "live_forward_decision_count": 2,
            "written_decision_count": 2,
            "actionable_decision_count": 1,
            "no_trade_reason_counts": {"insufficient_trusted_venues": 1},
            "max_decision_lag_ms": 100,
        },
        decisions_path=decisions_path,
    )

    assert summary["decision_count"] == 2
    assert summary["fair_value_non_null_count"] == 1
    assert summary["calibrated_fair_value_non_null_count"] == 1
    assert summary["trusted_venue_count_distribution"] == {"2": 1, "3": 1}
    assert summary["three_trusted_venue_row_count"] == 1
    assert summary["actionable_given_three_trusted_rate"] == "1"
    assert summary["event_clock_skew_bucket_counts"] == {
        "250-500ms": 1,
        "500-1000ms": 1,
    }


def test_build_day_tracker_entry_uses_new_shadow_reason() -> None:
    entry = build_day_tracker_entry(
        capture_date="2026-04-01",
        session_id="session-1",
        summary={
            "sample_count": 10,
            "session_diagnostics": {
                "termination_reason": "completed",
                "lifecycle_state": "completed",
            },
        },
        admission={
            "polymarket_continuity": {"window_verdict_counts": {"good": 3}},
            "snapshot_eligibility": {
                "snapshot_eligible_sample_count": 9,
                "snapshot_eligible_sample_ratio": 0.9,
            },
            "family_validation": {"off_family_switch_count": 0},
            "mapping_and_anchor": {"selected_binding_unresolved_window_count": 0},
        },
        policy_stack_summary={
            "stacks": [
                {
                    "stack_name": "baseline_only",
                    "trade_count": 5,
                    "hit_rate": "0.5",
                    "average_selected_net_edge": "0.1",
                    "total_pnl": "1.0",
                    "average_roi": "0.2",
                    "pnl_per_window": "0.3",
                    "pnl_per_100_trades": "4.0",
                    "pnl_per_1000_snapshots": "5.0",
                }
            ]
        },
        calibrated_summary={
            "sessions": [
                {
                    "session_id": "session-1",
                    "raw_summary": {"total_pnl": "1.0"},
                    "calibrated_summary": {"total_pnl": "2.0"},
                    "delta_total_pnl": "1.0",
                    "delta_average_roi": "0.1",
                    "delta_average_selected_net_edge": "0.01",
                    "delta_trade_count": 1,
                    "calibration_applied_row_count": 5,
                    "calibration_support_flag_counts": {"sufficient": 5},
                }
            ]
        },
        calibration_summary={
            "total_snapshot_count": 100,
            "support_flag_counts": {"sufficient": 5},
            "buckets": [
                {
                    "bucket_name": "near_mid",
                    "support_flag": "sufficient",
                    "calibration_gap_ci_low": "0.0",
                    "calibration_gap_ci_high": "0.2",
                }
            ],
        },
        cross_horizon_summary={
            "sessions": [
                {
                    "session_id": "session-1",
                    "window_verdict_counts": {"good": 3},
                }
            ]
        },
        artifact_paths={
            "summary_path": "summary.json",
            "admission_summary_path": "admission.json",
            "policy_stack_summary_path": "policy.json",
            "calibrated_summary_path": "cal.json",
            "policy_v1_calibration_summary_path": "calibration.json",
            "policy_v1_cross_horizon_summary_path": "cross.json",
            "shadow_summary_path": "shadow.json",
        },
        shadow_summary={
            "no_trade_reason_counts": {"future_recv_visibility_leak": 3},
        },
    )

    assert entry["capture_valid"] is True
    assert entry["shadow_clean_baseline"] is False
    assert entry["shadow_reason"] == "future_recv_visibility_leak"
