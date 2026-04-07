from __future__ import annotations

from decimal import Decimal

from rtds.replay.good_only_calibration import (
    CalibrationObservation,
    build_good_only_calibration_summary,
    build_good_only_calibration_summary_from_rollups,
    load_good_only_calibration_config,
)
from rtds.replay.session_rollups import (
    build_session_calibration_rollup,
    build_session_policy_rollup,
    build_session_shadow_rollup,
)


def test_session_calibration_rollups_rebuild_same_summary() -> None:
    config = load_good_only_calibration_config("configs/replay/calibration_good_only_v1.json")
    observations = [
        CalibrationObservation(
            session_label="day_a",
            session_id="a",
            capture_date="2026-04-01",
            window_id="w1",
            fair_value_base=Decimal("0.20"),
            resolved_up=False,
        ),
        CalibrationObservation(
            session_label="day_a",
            session_id="a",
            capture_date="2026-04-01",
            window_id="w1",
            fair_value_base=Decimal("0.22"),
            resolved_up=False,
        ),
        CalibrationObservation(
            session_label="day_a",
            session_id="a",
            capture_date="2026-04-01",
            window_id="w2",
            fair_value_base=Decimal("0.81"),
            resolved_up=True,
        ),
        CalibrationObservation(
            session_label="day_b",
            session_id="b",
            capture_date="2026-04-02",
            window_id="w3",
            fair_value_base=Decimal("0.18"),
            resolved_up=False,
        ),
        CalibrationObservation(
            session_label="day_b",
            session_id="b",
            capture_date="2026-04-02",
            window_id="w4",
            fair_value_base=Decimal("0.79"),
            resolved_up=True,
        ),
    ]
    direct = build_good_only_calibration_summary(
        observations,
        config=config,
        source_manifest_path="manifest.json",
        comparison_config_path="comparison.yaml",
    )
    rollups = [
        build_session_calibration_rollup(
            [row for row in observations if row.session_id == session_id],
            session_label=label,
            session_id=session_id,
            capture_date=capture_date,
            good_window_count=2,
            config=config,
        )
        for label, session_id, capture_date in [
            ("day_a", "a", "2026-04-01"),
            ("day_b", "b", "2026-04-02"),
        ]
    ]
    from_rollups = build_good_only_calibration_summary_from_rollups(
        rollups,
        config=config,
        source_manifest_path="manifest.json",
        comparison_config_path="comparison.yaml",
    )

    assert from_rollups.total_snapshot_count == direct.total_snapshot_count
    assert from_rollups.total_window_count == direct.total_window_count
    assert from_rollups.total_session_count == direct.total_session_count
    assert from_rollups.support_flag_counts == direct.support_flag_counts
    direct_buckets = {bucket.bucket_name: bucket for bucket in direct.buckets}
    rollup_buckets = {bucket.bucket_name: bucket for bucket in from_rollups.buckets}
    for bucket_name, direct_bucket in direct_buckets.items():
        rollup_bucket = rollup_buckets[bucket_name]
        assert rollup_bucket.snapshot_count == direct_bucket.snapshot_count
        assert rollup_bucket.window_count == direct_bucket.window_count
        assert rollup_bucket.session_count == direct_bucket.session_count
        assert rollup_bucket.observed_resolution_rate == direct_bucket.observed_resolution_rate
        assert rollup_bucket.average_predicted_f == direct_bucket.average_predicted_f
        assert rollup_bucket.calibration_gap == direct_bucket.calibration_gap
        assert rollup_bucket.support_flag == direct_bucket.support_flag


def test_session_policy_and_shadow_rollups_shape() -> None:
    policy = build_session_policy_rollup(
        capture_date="2026-04-01",
        session_id="abc",
        session_label="day7",
        policy_stack_summary={
            "stacks": [
                {
                    "stack_name": "baseline_only",
                    "trade_count": 1,
                    "hit_rate": "1",
                    "average_selected_net_edge": "0.1",
                    "total_pnl": "2",
                    "average_roi": "3",
                },
                {
                    "stack_name": "baseline_plus_degraded_light",
                    "trade_count": 2,
                    "hit_rate": "0.5",
                    "average_selected_net_edge": "0.05",
                    "total_pnl": "1",
                    "average_roi": "1",
                },
            ]
        },
        calibrated_session_summary={
            "raw_summary": {"total_pnl": "2"},
            "calibrated_summary": {"total_pnl": "3"},
        },
    )
    shadow = build_session_shadow_rollup(
        capture_date="2026-04-01",
        session_id="abc",
        shadow_clean_baseline=True,
        shadow_reason=None,
        quick_stage_a={
            "decision_count": 10,
            "actionable_decision_count": 4,
            "three_trusted_venue_rate": "0.3",
            "fair_value_non_null_count": 5,
            "no_trade_reason_counts": {"insufficient_trusted_venues": 6},
        },
    )

    assert policy["session_id"] == "abc"
    assert "baseline_plus_degraded_light" in policy["overlay_summaries"]
    assert shadow["classification"]["shadow_clean_baseline"] is True
    assert shadow["actionable_decision_count"] == 4
    assert shadow["fair_value_available_rate"] == "0.5"
