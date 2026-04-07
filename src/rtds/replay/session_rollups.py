"""Per-session rollups for incremental policy, calibration, and shadow refresh."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from rtds.replay.good_only_calibration import (
    CalibrationBucketDefinition,
    CalibrationObservation,
    GoodOnlyCalibrationConfig,
    classify_calibration_bucket,
)
from rtds.storage.writer import serialize_value


@dataclass(slots=True, frozen=True)
class CalibrationWindowRollup:
    """One bucketed per-window aggregate for calibration refresh."""

    bucket_name: str
    session_label: str
    session_id: str
    capture_date: str
    window_id: str
    snapshot_count: int
    resolved_up_count: int
    predicted_f_sum: Decimal


@dataclass(slots=True, frozen=True)
class CalibrationBucketRollup:
    """One per-session calibration bucket aggregate."""

    bucket_name: str
    lower_bound_inclusive: Decimal
    upper_bound: Decimal
    upper_bound_inclusive: bool
    snapshot_count: int
    window_count: int
    resolved_up_count: int
    resolved_down_count: int
    predicted_f_sum: Decimal
    average_predicted_f: Decimal | None
    window_rollups: tuple[CalibrationWindowRollup, ...]


@dataclass(slots=True, frozen=True)
class SessionCalibrationRollup:
    """One persisted session-level calibration rollup."""

    session_label: str
    session_id: str
    capture_date: str
    good_window_count: int
    calibration_eligible_snapshot_count: int
    calibration_eligible_window_count: int
    calibration_eligible_window_ids: tuple[str, ...]
    bucket_rollups: tuple[CalibrationBucketRollup, ...]


def build_session_calibration_rollup(
    observations: Sequence[CalibrationObservation],
    *,
    session_label: str,
    session_id: str,
    capture_date: str,
    good_window_count: int,
    config: GoodOnlyCalibrationConfig,
) -> SessionCalibrationRollup:
    """Build one per-session calibration rollup from good-only observations."""

    bucket_rows: dict[str, list[CalibrationObservation]] = {
        bucket.bucket_name: [] for bucket in config.bucket_definitions
    }
    for observation in observations:
        bucket = classify_calibration_bucket(observation.fair_value_base, config=config)
        if bucket is None:
            continue
        bucket_rows[bucket.bucket_name].append(observation)

    bucket_rollups = tuple(
        _build_bucket_rollup(bucket, bucket_rows[bucket.bucket_name])
        for bucket in config.bucket_definitions
    )
    eligible_window_ids = sorted({observation.window_id for observation in observations})
    return SessionCalibrationRollup(
        session_label=session_label,
        session_id=session_id,
        capture_date=capture_date,
        good_window_count=good_window_count,
        calibration_eligible_snapshot_count=len(observations),
        calibration_eligible_window_count=len(eligible_window_ids),
        calibration_eligible_window_ids=tuple(eligible_window_ids),
        bucket_rollups=bucket_rollups,
    )


def build_session_policy_rollup(
    *,
    capture_date: str,
    session_id: str,
    session_label: str,
    policy_stack_summary: Mapping[str, Any],
    calibrated_session_summary: Mapping[str, Any],
) -> dict[str, object]:
    """Build one per-session policy rollup."""

    stack_metrics = {stack["stack_name"]: dict(stack) for stack in policy_stack_summary["stacks"]}
    return serialize_value(
        {
            "capture_date": capture_date,
            "session_id": session_id,
            "session_label": session_label,
            "raw_baseline_only_summary": stack_metrics["baseline_only"],
            "calibrated_baseline_only_summary": calibrated_session_summary,
            "overlay_summaries": {
                name: payload
                for name, payload in stack_metrics.items()
                if name != "baseline_only"
            },
        }
    )


def build_session_shadow_rollup(
    *,
    capture_date: str,
    session_id: str,
    shadow_clean_baseline: bool | None,
    shadow_reason: str | None,
    quick_stage_a: Mapping[str, Any] | None,
) -> dict[str, object]:
    """Build one per-session shadow rollup."""

    quick = dict(quick_stage_a or {})
    return serialize_value(
        {
            "capture_date": capture_date,
            "session_id": session_id,
            "classification": {
                "shadow_clean_baseline": shadow_clean_baseline,
                "shadow_reason": shadow_reason,
            },
            "actionable_decision_count": quick.get("actionable_decision_count"),
            "three_trusted_venue_rate": quick.get("three_trusted_venue_rate"),
            "fair_value_available_rate": _safe_rate(
                quick.get("fair_value_non_null_count"),
                quick.get("decision_count"),
            ),
            "fair_value_non_null_count": quick.get("fair_value_non_null_count"),
            "calibrated_fair_value_non_null_count": quick.get(
                "calibrated_fair_value_non_null_count"
            ),
            "no_trade_reason_counts": quick.get("no_trade_reason_counts"),
            "stage_a_summary": quick,
        }
    )


def _build_bucket_rollup(
    bucket: CalibrationBucketDefinition,
    observations: Sequence[CalibrationObservation],
) -> CalibrationBucketRollup:
    per_window: dict[str, dict[str, Any]] = {}
    resolved_up_count = 0
    predicted_f_sum = Decimal("0")
    for observation in observations:
        resolved_up_count += int(observation.resolved_up)
        predicted_f_sum += observation.fair_value_base
        record = per_window.setdefault(
            observation.window_id,
            {
                "snapshot_count": 0,
                "resolved_up_count": 0,
                "predicted_f_sum": Decimal("0"),
            },
        )
        record["snapshot_count"] += 1
        record["resolved_up_count"] += int(observation.resolved_up)
        record["predicted_f_sum"] += observation.fair_value_base
    snapshot_count = len(observations)
    window_rollups = tuple(
        CalibrationWindowRollup(
            bucket_name=bucket.bucket_name,
            session_label=observations[0].session_label if observations else "",
            session_id=observations[0].session_id if observations else "",
            capture_date=observations[0].capture_date if observations else "",
            window_id=window_id,
            snapshot_count=int(payload["snapshot_count"]),
            resolved_up_count=int(payload["resolved_up_count"]),
            predicted_f_sum=payload["predicted_f_sum"],
        )
        for window_id, payload in sorted(per_window.items())
    )
    average_predicted_f = (
        None if snapshot_count == 0 else predicted_f_sum / Decimal(snapshot_count)
    )
    return CalibrationBucketRollup(
        bucket_name=bucket.bucket_name,
        lower_bound_inclusive=bucket.lower_bound_inclusive,
        upper_bound=bucket.upper_bound,
        upper_bound_inclusive=bucket.upper_bound_inclusive,
        snapshot_count=snapshot_count,
        window_count=len(per_window),
        resolved_up_count=resolved_up_count,
        resolved_down_count=snapshot_count - resolved_up_count,
        predicted_f_sum=predicted_f_sum,
        average_predicted_f=average_predicted_f,
        window_rollups=window_rollups,
    )


def _safe_rate(numerator: Any, denominator: Any) -> str | None:
    if numerator is None or denominator in (None, 0):
        return None
    return str(Decimal(int(numerator)) / Decimal(int(denominator)))


def load_session_calibration_rollup(path: str | Path) -> SessionCalibrationRollup:
    """Load one persisted session calibration rollup."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return session_calibration_rollup_from_payload(payload)


def session_calibration_rollup_from_payload(
    payload: Mapping[str, Any],
) -> SessionCalibrationRollup:
    """Load one session calibration rollup from an in-memory payload."""

    return SessionCalibrationRollup(
        session_label=str(payload["session_label"]),
        session_id=str(payload["session_id"]),
        capture_date=str(payload["capture_date"]),
        good_window_count=int(payload["good_window_count"]),
        calibration_eligible_snapshot_count=int(payload["calibration_eligible_snapshot_count"]),
        calibration_eligible_window_count=int(payload["calibration_eligible_window_count"]),
        calibration_eligible_window_ids=tuple(
            str(item) for item in payload["calibration_eligible_window_ids"]
        ),
        bucket_rollups=tuple(
            CalibrationBucketRollup(
                bucket_name=str(bucket["bucket_name"]),
                lower_bound_inclusive=Decimal(str(bucket["lower_bound_inclusive"])),
                upper_bound=Decimal(str(bucket["upper_bound"])),
                upper_bound_inclusive=bool(bucket["upper_bound_inclusive"]),
                snapshot_count=int(bucket["snapshot_count"]),
                window_count=int(bucket["window_count"]),
                resolved_up_count=int(bucket["resolved_up_count"]),
                resolved_down_count=int(bucket["resolved_down_count"]),
                predicted_f_sum=Decimal(str(bucket["predicted_f_sum"])),
                average_predicted_f=(
                    None
                    if bucket["average_predicted_f"] is None
                    else Decimal(str(bucket["average_predicted_f"]))
                ),
                window_rollups=tuple(
                    CalibrationWindowRollup(
                        bucket_name=str(window["bucket_name"]),
                        session_label=str(window["session_label"]),
                        session_id=str(window["session_id"]),
                        capture_date=str(window["capture_date"]),
                        window_id=str(window["window_id"]),
                        snapshot_count=int(window["snapshot_count"]),
                        resolved_up_count=int(window["resolved_up_count"]),
                        predicted_f_sum=Decimal(str(window["predicted_f_sum"])),
                    )
                    for window in bucket["window_rollups"]
                ),
            )
            for bucket in payload["bucket_rollups"]
        ),
    )
