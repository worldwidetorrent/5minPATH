from __future__ import annotations

from decimal import Decimal

from rtds.replay.calibration_state import (
    CALIBRATION_STATE_VERSION,
    build_cumulative_calibration_state,
    update_cumulative_calibration_state,
)
from rtds.replay.good_only_calibration import (
    CalibrationObservation,
    load_good_only_calibration_config,
)
from rtds.replay.session_rollups import build_session_calibration_rollup


def _rollup(
    *,
    session_label: str,
    session_id: str,
    capture_date: str,
    rows: list[tuple[str, str, bool]],
):
    config = load_good_only_calibration_config("configs/replay/calibration_good_only_v1.json")
    observations = [
        CalibrationObservation(
            session_label=session_label,
            session_id=session_id,
            capture_date=capture_date,
            window_id=window_id,
            fair_value_base=Decimal(fair_value),
            resolved_up=resolved_up,
        )
        for fair_value, window_id, resolved_up in rows
    ]
    return build_session_calibration_rollup(
        observations,
        session_label=session_label,
        session_id=session_id,
        capture_date=capture_date,
        good_window_count=len({row[1] for row in rows}),
        config=config,
    )


def test_build_and_update_cumulative_calibration_state() -> None:
    config = load_good_only_calibration_config("configs/replay/calibration_good_only_v1.json")
    state = build_cumulative_calibration_state(
        [
            _rollup(
                session_label="day1",
                session_id="s1",
                capture_date="2026-04-01",
                rows=[("0.20", "w1", False), ("0.80", "w2", True)],
            )
        ],
        config=config,
        calibration_config_path="config.json",
        source_manifest_path="manifest.json",
        comparison_config_path="comparison.yaml",
    )

    assert state.state_version == CALIBRATION_STATE_VERSION
    assert state.applied_session_ids == ("s1",)
    assert state.summary.total_session_count == 1
    assert state.summary.total_snapshot_count == 2

    updated = update_cumulative_calibration_state(
        state,
        incoming_rollup=_rollup(
            session_label="day2",
            session_id="s2",
            capture_date="2026-04-02",
            rows=[("0.18", "w3", False), ("0.82", "w4", True)],
        ),
        config=config,
        calibration_config_path="config.json",
        source_manifest_path="manifest.json",
        comparison_config_path="comparison.yaml",
    )

    assert updated.applied_session_ids == ("s1", "s2")
    assert updated.summary.total_session_count == 2
    assert updated.summary.total_snapshot_count == 4
