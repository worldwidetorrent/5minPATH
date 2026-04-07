"""Versioned cumulative calibration state for incremental refresh."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from rtds.core.time import format_utc, utc_now
from rtds.replay.good_only_calibration import (
    GoodOnlyCalibrationConfig,
    GoodOnlyCalibrationSummary,
    build_good_only_calibration_summary_from_rollups,
    good_only_calibration_summary_to_dict,
)
from rtds.replay.session_rollups import SessionCalibrationRollup
from rtds.storage.writer import serialize_value, write_json_file

CALIBRATION_STATE_VERSION = "good_only_calibration_state_v1"


@dataclass(slots=True, frozen=True)
class CumulativeCalibrationState:
    """Incremental cumulative calibration state."""

    state_version: str
    calibration_id: str
    calibration_config_path: str
    source_manifest_path: str
    comparison_config_path: str
    updated_at: str
    applied_session_ids: tuple[str, ...]
    session_rollups: tuple[SessionCalibrationRollup, ...]
    summary: GoodOnlyCalibrationSummary


def build_cumulative_calibration_state(
    session_rollups: Sequence[SessionCalibrationRollup],
    *,
    config: GoodOnlyCalibrationConfig,
    calibration_config_path: str,
    source_manifest_path: str,
    comparison_config_path: str,
) -> CumulativeCalibrationState:
    """Build a cumulative state snapshot from session rollups."""

    ordered_rollups = tuple(sorted(session_rollups, key=lambda item: item.session_id))
    summary = build_good_only_calibration_summary_from_rollups(
        ordered_rollups,
        config=config,
        source_manifest_path=source_manifest_path,
        comparison_config_path=comparison_config_path,
    )
    return CumulativeCalibrationState(
        state_version=CALIBRATION_STATE_VERSION,
        calibration_id=config.calibration_id,
        calibration_config_path=calibration_config_path,
        source_manifest_path=source_manifest_path,
        comparison_config_path=comparison_config_path,
        updated_at=format_utc(utc_now()),
        applied_session_ids=tuple(item.session_id for item in ordered_rollups),
        session_rollups=ordered_rollups,
        summary=summary,
    )


def update_cumulative_calibration_state(
    state: CumulativeCalibrationState | None,
    *,
    incoming_rollup: SessionCalibrationRollup,
    config: GoodOnlyCalibrationConfig,
    calibration_config_path: str,
    source_manifest_path: str,
    comparison_config_path: str,
) -> CumulativeCalibrationState:
    """Merge one session rollup into the cumulative state."""

    existing = {
        rollup.session_id: rollup for rollup in (() if state is None else state.session_rollups)
    }
    existing[incoming_rollup.session_id] = incoming_rollup
    return build_cumulative_calibration_state(
        tuple(existing.values()),
        config=config,
        calibration_config_path=calibration_config_path,
        source_manifest_path=source_manifest_path,
        comparison_config_path=comparison_config_path,
    )


def cumulative_calibration_state_to_dict(
    state: CumulativeCalibrationState,
) -> dict[str, object]:
    """Serialize one cumulative calibration state."""

    return {
        "state_version": state.state_version,
        "calibration_id": state.calibration_id,
        "calibration_config_path": state.calibration_config_path,
        "source_manifest_path": state.source_manifest_path,
        "comparison_config_path": state.comparison_config_path,
        "updated_at": state.updated_at,
        "applied_session_ids": list(state.applied_session_ids),
        "session_rollups": [serialize_value(item) for item in state.session_rollups],
        "summary": good_only_calibration_summary_to_dict(state.summary),
    }


def write_cumulative_calibration_state(
    path: str | Path,
    state: CumulativeCalibrationState,
) -> Path:
    """Write the cumulative calibration state to disk."""

    return write_json_file(path, cumulative_calibration_state_to_dict(state))
