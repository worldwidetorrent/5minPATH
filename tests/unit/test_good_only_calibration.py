from decimal import Decimal

from rtds.replay.good_only_calibration import (
    CalibrationObservation,
    build_good_only_calibration_summary,
    classify_calibration_bucket,
    load_good_only_calibration_config,
)


def test_classify_calibration_bucket_uses_config_boundaries() -> None:
    config = load_good_only_calibration_config("configs/replay/calibration_good_only_v1.json")

    assert classify_calibration_bucket(Decimal("0.10"), config=config).bucket_name == "far_down"
    assert classify_calibration_bucket(Decimal("0.35"), config=config).bucket_name == "lean_down"
    assert classify_calibration_bucket(Decimal("0.50"), config=config).bucket_name == "near_mid"
    assert classify_calibration_bucket(Decimal("0.60"), config=config).bucket_name == "lean_up"
    assert classify_calibration_bucket(Decimal("1.00"), config=config).bucket_name == "far_up"


def test_build_good_only_calibration_summary_sets_support_flags_and_merge_targets() -> None:
    config = load_good_only_calibration_config("configs/replay/calibration_good_only_v1.json")
    observations: list[CalibrationObservation] = []

    for session_label, session_id in (("baseline_6h", "s1"), ("pilot_12h", "s2")):
        for window_index in range(4):
            window_id = f"{session_id}-window-{window_index}"
            for _ in range(250):
                observations.append(
                    CalibrationObservation(
                        session_label=session_label,
                        session_id=session_id,
                        capture_date="2026-03-16",
                        window_id=window_id,
                        fair_value_base=Decimal("0.55"),
                        resolved_up=True,
                    )
                )
        sparse_window_id = f"{session_id}-sparse"
        for _ in range(10):
            observations.append(
                CalibrationObservation(
                    session_label=session_label,
                    session_id=session_id,
                    capture_date="2026-03-16",
                    window_id=sparse_window_id,
                    fair_value_base=Decimal("0.90"),
                    resolved_up=True,
                )
            )

    summary = build_good_only_calibration_summary(
        observations,
        config=config,
        source_manifest_path="configs/baselines/analysis/policy_v1_cross_horizon.json",
        comparison_config_path="configs/replay/task7_reference_comparison.yaml",
    )

    by_name = {bucket.bucket_name: bucket for bucket in summary.buckets}
    lean_up = by_name["lean_up"]
    far_up = by_name["far_up"]

    assert lean_up.support_flag == "sufficient"
    assert lean_up.snapshot_count == 2000
    assert lean_up.window_count == 8
    assert lean_up.recommended_merge_bucket is None

    assert far_up.support_flag == "merge_required"
    assert far_up.snapshot_count == 20
    assert far_up.window_count == 2
    assert far_up.recommended_merge_bucket == "lean_up"
