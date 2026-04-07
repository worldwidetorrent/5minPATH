"""Stage-1 good-only calibration with coarse uncertainty-aware buckets."""

from __future__ import annotations

import json
import random
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

from rtds.storage.writer import serialize_value


@dataclass(slots=True, frozen=True)
class CalibrationBucketDefinition:
    """One coarse fair-value bucket used for stage-1 calibration."""

    bucket_name: str
    lower_bound_inclusive: Decimal
    upper_bound: Decimal
    upper_bound_inclusive: bool

    def contains(self, value: Decimal) -> bool:
        if value < self.lower_bound_inclusive:
            return False
        if self.upper_bound_inclusive:
            return value <= self.upper_bound
        return value < self.upper_bound


@dataclass(slots=True, frozen=True)
class CalibrationSupportThreshold:
    """Minimum support needed for one qualitative support flag."""

    min_window_count: int
    min_snapshot_count: int


@dataclass(slots=True, frozen=True)
class GoodOnlyCalibrationConfig:
    """Effective config for the coarse good-only calibration pass."""

    calibration_id: str
    policy_universe: str
    bucket_definitions: tuple[CalibrationBucketDefinition, ...]
    bootstrap_replicates: int
    bootstrap_seed: int
    sufficient_threshold: CalibrationSupportThreshold
    thin_threshold: CalibrationSupportThreshold


@dataclass(slots=True, frozen=True)
class CalibrationObservation:
    """One labeled good-window snapshot used in calibration."""

    session_label: str
    session_id: str
    capture_date: str
    window_id: str
    fair_value_base: Decimal
    resolved_up: bool


@dataclass(slots=True, frozen=True)
class CalibrationBucketResult:
    """One coarse reliability bucket with uncertainty and support flags."""

    bucket_name: str
    lower_bound_inclusive: Decimal
    upper_bound: Decimal
    upper_bound_inclusive: bool
    snapshot_count: int
    window_count: int
    session_count: int
    observed_resolution_rate: Decimal | None
    average_predicted_f: Decimal | None
    calibration_gap: Decimal | None
    observed_resolution_rate_ci_low: Decimal | None
    observed_resolution_rate_ci_high: Decimal | None
    calibration_gap_ci_low: Decimal | None
    calibration_gap_ci_high: Decimal | None
    support_flag: str
    recommended_merge_bucket: str | None
    recommended_action: str
    provisional_calibrated_f: Decimal | None
    session_snapshot_counts: dict[str, int]
    session_window_counts: dict[str, int]


@dataclass(slots=True, frozen=True)
class GoodOnlyCalibrationSummary:
    """Full stage-1 calibration summary across pinned good-window baselines."""

    calibration_id: str
    policy_universe: str
    source_manifest_path: str
    comparison_config_path: str
    total_snapshot_count: int
    total_window_count: int
    total_session_count: int
    support_flag_counts: dict[str, int]
    buckets: tuple[CalibrationBucketResult, ...]


def load_good_only_calibration_config(path: str | Path) -> GoodOnlyCalibrationConfig:
    """Load the versioned good-only calibration config from JSON."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    bucket_definitions = tuple(
        CalibrationBucketDefinition(
            bucket_name=str(item["bucket_name"]),
            lower_bound_inclusive=Decimal(str(item["lower_bound_inclusive"])),
            upper_bound=Decimal(
                str(item.get("upper_bound_exclusive", item.get("upper_bound_inclusive")))
            ),
            upper_bound_inclusive="upper_bound_inclusive" in item,
        )
        for item in payload["bucket_definitions"]
    )
    sufficient_payload = payload["support_thresholds"]["sufficient"]
    thin_payload = payload["support_thresholds"]["thin"]
    return GoodOnlyCalibrationConfig(
        calibration_id=str(payload["calibration_id"]),
        policy_universe=str(payload["policy_universe"]),
        bucket_definitions=bucket_definitions,
        bootstrap_replicates=int(payload["bootstrap_replicates"]),
        bootstrap_seed=int(payload["bootstrap_seed"]),
        sufficient_threshold=CalibrationSupportThreshold(
            min_window_count=int(sufficient_payload["min_window_count"]),
            min_snapshot_count=int(sufficient_payload["min_snapshot_count"]),
        ),
        thin_threshold=CalibrationSupportThreshold(
            min_window_count=int(thin_payload["min_window_count"]),
            min_snapshot_count=int(thin_payload["min_snapshot_count"]),
        ),
    )


def classify_calibration_bucket(
    fair_value_base: Decimal,
    *,
    config: GoodOnlyCalibrationConfig,
) -> CalibrationBucketDefinition | None:
    """Resolve one fair value into the configured coarse calibration bucket."""

    for bucket in config.bucket_definitions:
        if bucket.contains(fair_value_base):
            return bucket
    return None


def build_good_only_calibration_summary(
    observations: Sequence[CalibrationObservation],
    *,
    config: GoodOnlyCalibrationConfig,
    source_manifest_path: str,
    comparison_config_path: str,
) -> GoodOnlyCalibrationSummary:
    """Aggregate the pinned good-window observations into coarse reliability buckets."""

    observation_rows_by_bucket: dict[str, list[CalibrationObservation]] = {
        bucket.bucket_name: [] for bucket in config.bucket_definitions
    }
    for observation in observations:
        bucket = classify_calibration_bucket(observation.fair_value_base, config=config)
        if bucket is None:
            continue
        observation_rows_by_bucket[bucket.bucket_name].append(observation)

    bucket_results: list[CalibrationBucketResult] = []
    for index, bucket in enumerate(config.bucket_definitions):
        bucket_rows = observation_rows_by_bucket[bucket.bucket_name]
        bucket_results.append(
            _build_bucket_result(
                bucket,
                bucket_rows,
                config=config,
                bucket_index=index,
            )
        )

    bucket_results = _apply_merge_recommendations(bucket_results, config=config)
    support_flag_counts = defaultdict(int)
    for result in bucket_results:
        support_flag_counts[result.support_flag] += 1

    distinct_windows = {
        (observation.session_id, observation.window_id) for observation in observations
    }
    distinct_sessions = {observation.session_id for observation in observations}
    return GoodOnlyCalibrationSummary(
        calibration_id=config.calibration_id,
        policy_universe=config.policy_universe,
        source_manifest_path=source_manifest_path,
        comparison_config_path=comparison_config_path,
        total_snapshot_count=len(observations),
        total_window_count=len(distinct_windows),
        total_session_count=len(distinct_sessions),
        support_flag_counts=dict(sorted(support_flag_counts.items())),
        buckets=tuple(bucket_results),
    )


def build_good_only_calibration_summary_from_rollups(
    session_rollups: Sequence[object],
    *,
    config: GoodOnlyCalibrationConfig,
    source_manifest_path: str,
    comparison_config_path: str,
) -> GoodOnlyCalibrationSummary:
    """Aggregate coarse reliability buckets from per-session rollups."""

    window_rollups_by_bucket: dict[str, list[Mapping[str, object]]] = {
        bucket.bucket_name: [] for bucket in config.bucket_definitions
    }
    total_snapshot_count = 0
    distinct_windows: set[tuple[str, str]] = set()
    distinct_sessions: set[str] = set()
    for rollup in session_rollups:
        total_snapshot_count += int(rollup.calibration_eligible_snapshot_count)
        distinct_sessions.add(str(rollup.session_id))
        for window_id in rollup.calibration_eligible_window_ids:
            distinct_windows.add((str(rollup.session_id), str(window_id)))
        for bucket_rollup in rollup.bucket_rollups:
            for window_rollup in bucket_rollup.window_rollups:
                window_rollups_by_bucket[bucket_rollup.bucket_name].append(
                    {
                        "session_label": window_rollup.session_label,
                        "session_id": window_rollup.session_id,
                        "window_id": window_rollup.window_id,
                        "snapshot_count": window_rollup.snapshot_count,
                        "resolved_up_count": window_rollup.resolved_up_count,
                        "pred_sum": float(window_rollup.predicted_f_sum),
                    }
                )

    bucket_results: list[CalibrationBucketResult] = []
    for index, bucket in enumerate(config.bucket_definitions):
        bucket_results.append(
            _build_bucket_result_from_window_rollups(
                bucket,
                window_rollups_by_bucket[bucket.bucket_name],
                config=config,
                bucket_index=index,
            )
        )
    bucket_results = _apply_merge_recommendations(bucket_results, config=config)
    support_flag_counts = defaultdict(int)
    for result in bucket_results:
        support_flag_counts[result.support_flag] += 1
    return GoodOnlyCalibrationSummary(
        calibration_id=config.calibration_id,
        policy_universe=config.policy_universe,
        source_manifest_path=source_manifest_path,
        comparison_config_path=comparison_config_path,
        total_snapshot_count=total_snapshot_count,
        total_window_count=len(distinct_windows),
        total_session_count=len(distinct_sessions),
        support_flag_counts=dict(sorted(support_flag_counts.items())),
        buckets=tuple(bucket_results),
    )


def good_only_calibration_summary_to_dict(
    summary: GoodOnlyCalibrationSummary,
) -> dict[str, object]:
    """Serialize the calibration summary to stable JSON."""

    return {
        "calibration_id": summary.calibration_id,
        "policy_universe": summary.policy_universe,
        "source_manifest_path": summary.source_manifest_path,
        "comparison_config_path": summary.comparison_config_path,
        "total_snapshot_count": summary.total_snapshot_count,
        "total_window_count": summary.total_window_count,
        "total_session_count": summary.total_session_count,
        "support_flag_counts": summary.support_flag_counts,
        "buckets": [serialize_value(bucket) for bucket in summary.buckets],
    }


def _build_bucket_result(
    bucket: CalibrationBucketDefinition,
    observations: Sequence[CalibrationObservation],
    *,
    config: GoodOnlyCalibrationConfig,
    bucket_index: int,
) -> CalibrationBucketResult:
    per_window = defaultdict(lambda: {"snapshot_count": 0, "resolved_up_count": 0, "pred_sum": 0.0})
    session_snapshot_counts = defaultdict(int)
    session_window_ids = defaultdict(set)

    for observation in observations:
        key = (observation.session_id, observation.window_id)
        per_window[key]["snapshot_count"] += 1
        per_window[key]["resolved_up_count"] += int(observation.resolved_up)
        per_window[key]["pred_sum"] += float(observation.fair_value_base)
        session_snapshot_counts[observation.session_label] += 1
        session_window_ids[observation.session_label].add(observation.window_id)

    snapshot_count = len(observations)
    window_count = len(per_window)
    session_count = len(session_snapshot_counts)
    observed_resolution_rate = (
        None
        if snapshot_count == 0
        else Decimal(
            str(
                sum(int(observation.resolved_up) for observation in observations) / snapshot_count
            )
        )
    )
    average_predicted_f = (
        None
        if snapshot_count == 0
        else Decimal(
            str(
                sum(float(observation.fair_value_base) for observation in observations)
                / snapshot_count
            )
        )
    )
    calibration_gap = (
        None
        if observed_resolution_rate is None or average_predicted_f is None
        else observed_resolution_rate - average_predicted_f
    )
    observed_ci_low: Decimal | None = None
    observed_ci_high: Decimal | None = None
    gap_ci_low: Decimal | None = None
    gap_ci_high: Decimal | None = None
    if per_window:
        bootstrap = _bootstrap_bucket(
            list(per_window.values()),
            replicates=config.bootstrap_replicates,
            seed=config.bootstrap_seed + bucket_index,
        )
        observed_ci_low = Decimal(str(bootstrap["observed_ci_low"]))
        observed_ci_high = Decimal(str(bootstrap["observed_ci_high"]))
        gap_ci_low = Decimal(str(bootstrap["gap_ci_low"]))
        gap_ci_high = Decimal(str(bootstrap["gap_ci_high"]))

    support_flag = _support_flag(
        snapshot_count=snapshot_count,
        window_count=window_count,
        config=config,
    )
    provisional_calibrated_f = None
    recommended_action = "merge_or_leave_uncorrected"
    if support_flag in {"sufficient", "thin"} and observed_resolution_rate is not None:
        provisional_calibrated_f = observed_resolution_rate
        recommended_action = "apply_bucket_mean"
        if support_flag == "thin":
            recommended_action = "apply_with_caution"

    return CalibrationBucketResult(
        bucket_name=bucket.bucket_name,
        lower_bound_inclusive=bucket.lower_bound_inclusive,
        upper_bound=bucket.upper_bound,
        upper_bound_inclusive=bucket.upper_bound_inclusive,
        snapshot_count=snapshot_count,
        window_count=window_count,
        session_count=session_count,
        observed_resolution_rate=observed_resolution_rate,
        average_predicted_f=average_predicted_f,
        calibration_gap=calibration_gap,
        observed_resolution_rate_ci_low=observed_ci_low,
        observed_resolution_rate_ci_high=observed_ci_high,
        calibration_gap_ci_low=gap_ci_low,
        calibration_gap_ci_high=gap_ci_high,
        support_flag=support_flag,
        recommended_merge_bucket=None,
        recommended_action=recommended_action,
        provisional_calibrated_f=provisional_calibrated_f,
        session_snapshot_counts=dict(sorted(session_snapshot_counts.items())),
        session_window_counts={
            key: len(value) for key, value in sorted(session_window_ids.items())
        },
    )


def _build_bucket_result_from_window_rollups(
    bucket: CalibrationBucketDefinition,
    window_rollups: Sequence[Mapping[str, object]],
    *,
    config: GoodOnlyCalibrationConfig,
    bucket_index: int,
) -> CalibrationBucketResult:
    session_snapshot_counts = defaultdict(int)
    session_window_ids = defaultdict(set)
    snapshot_count = 0
    resolved_up_count = 0
    predicted_f_sum = 0.0
    for row in window_rollups:
        snapshot_count += int(row["snapshot_count"])
        resolved_up_count += int(row["resolved_up_count"])
        predicted_f_sum += float(row["pred_sum"])
        session_snapshot_counts[str(row["session_label"])] += int(row["snapshot_count"])
        session_window_ids[str(row["session_label"])].add(str(row["window_id"]))
    window_count = len(window_rollups)
    session_count = len(session_snapshot_counts)
    observed_resolution_rate = (
        None if snapshot_count == 0 else Decimal(str(resolved_up_count / snapshot_count))
    )
    average_predicted_f = (
        None if snapshot_count == 0 else Decimal(str(predicted_f_sum / snapshot_count))
    )
    calibration_gap = (
        None
        if observed_resolution_rate is None or average_predicted_f is None
        else observed_resolution_rate - average_predicted_f
    )
    observed_ci_low: Decimal | None = None
    observed_ci_high: Decimal | None = None
    gap_ci_low: Decimal | None = None
    gap_ci_high: Decimal | None = None
    if window_rollups:
        bootstrap = _bootstrap_bucket(
            [
                {
                    "snapshot_count": int(row["snapshot_count"]),
                    "resolved_up_count": int(row["resolved_up_count"]),
                    "pred_sum": float(row["pred_sum"]),
                }
                for row in window_rollups
            ],
            replicates=config.bootstrap_replicates,
            seed=config.bootstrap_seed + bucket_index,
        )
        observed_ci_low = Decimal(str(bootstrap["observed_ci_low"]))
        observed_ci_high = Decimal(str(bootstrap["observed_ci_high"]))
        gap_ci_low = Decimal(str(bootstrap["gap_ci_low"]))
        gap_ci_high = Decimal(str(bootstrap["gap_ci_high"]))
    support_flag = _support_flag(
        snapshot_count=snapshot_count,
        window_count=window_count,
        config=config,
    )
    provisional_calibrated_f = None
    recommended_action = "merge_or_leave_uncorrected"
    if support_flag in {"sufficient", "thin"} and observed_resolution_rate is not None:
        provisional_calibrated_f = observed_resolution_rate
        recommended_action = "apply_bucket_mean"
        if support_flag == "thin":
            recommended_action = "apply_with_caution"
    return CalibrationBucketResult(
        bucket_name=bucket.bucket_name,
        lower_bound_inclusive=bucket.lower_bound_inclusive,
        upper_bound=bucket.upper_bound,
        upper_bound_inclusive=bucket.upper_bound_inclusive,
        snapshot_count=snapshot_count,
        window_count=window_count,
        session_count=session_count,
        observed_resolution_rate=observed_resolution_rate,
        average_predicted_f=average_predicted_f,
        calibration_gap=calibration_gap,
        observed_resolution_rate_ci_low=observed_ci_low,
        observed_resolution_rate_ci_high=observed_ci_high,
        calibration_gap_ci_low=gap_ci_low,
        calibration_gap_ci_high=gap_ci_high,
        support_flag=support_flag,
        recommended_merge_bucket=None,
        recommended_action=recommended_action,
        provisional_calibrated_f=provisional_calibrated_f,
        session_snapshot_counts=dict(sorted(session_snapshot_counts.items())),
        session_window_counts={
            key: len(value) for key, value in sorted(session_window_ids.items())
        },
    )


def _bootstrap_bucket(
    per_window_rows: Sequence[Mapping[str, float | int]],
    *,
    replicates: int,
    seed: int,
) -> dict[str, float]:
    rng = random.Random(seed)
    observed_samples: list[float] = []
    gap_samples: list[float] = []
    for _ in range(replicates):
        total_snapshot_count = 0
        total_resolved_up = 0
        total_pred_sum = 0.0
        for _index in range(len(per_window_rows)):
            sampled = per_window_rows[rng.randrange(len(per_window_rows))]
            total_snapshot_count += int(sampled["snapshot_count"])
            total_resolved_up += int(sampled["resolved_up_count"])
            total_pred_sum += float(sampled["pred_sum"])
        if total_snapshot_count == 0:
            continue
        observed_rate = total_resolved_up / total_snapshot_count
        average_predicted = total_pred_sum / total_snapshot_count
        observed_samples.append(observed_rate)
        gap_samples.append(observed_rate - average_predicted)

    if not observed_samples:
        return {
            "observed_ci_low": 0.0,
            "observed_ci_high": 0.0,
            "gap_ci_low": 0.0,
            "gap_ci_high": 0.0,
        }
    return {
        "observed_ci_low": _percentile(observed_samples, 0.025),
        "observed_ci_high": _percentile(observed_samples, 0.975),
        "gap_ci_low": _percentile(gap_samples, 0.025),
        "gap_ci_high": _percentile(gap_samples, 0.975),
    }


def _percentile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = probability * (len(ordered) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    if lower_index == upper_index:
        return lower_value
    weight = position - lower_index
    return lower_value + (upper_value - lower_value) * weight


def _support_flag(
    *,
    snapshot_count: int,
    window_count: int,
    config: GoodOnlyCalibrationConfig,
) -> str:
    if (
        window_count >= config.sufficient_threshold.min_window_count
        and snapshot_count >= config.sufficient_threshold.min_snapshot_count
    ):
        return "sufficient"
    if (
        window_count >= config.thin_threshold.min_window_count
        and snapshot_count >= config.thin_threshold.min_snapshot_count
    ):
        return "thin"
    return "merge_required"


def _apply_merge_recommendations(
    bucket_results: Sequence[CalibrationBucketResult],
    *,
    config: GoodOnlyCalibrationConfig,
) -> list[CalibrationBucketResult]:
    ordered_nonempty = [
        bucket.bucket_name for bucket in bucket_results if bucket.snapshot_count > 0
    ]
    updated: list[CalibrationBucketResult] = []
    for index, bucket in enumerate(bucket_results):
        merge_target = None
        if bucket.support_flag == "merge_required" and ordered_nonempty:
            merge_target = _nearest_supported_bucket_name(
                bucket_results,
                current_index=index,
            )
        updated.append(
            CalibrationBucketResult(
                bucket_name=bucket.bucket_name,
                lower_bound_inclusive=bucket.lower_bound_inclusive,
                upper_bound=bucket.upper_bound,
                upper_bound_inclusive=bucket.upper_bound_inclusive,
                snapshot_count=bucket.snapshot_count,
                window_count=bucket.window_count,
                session_count=bucket.session_count,
                observed_resolution_rate=bucket.observed_resolution_rate,
                average_predicted_f=bucket.average_predicted_f,
                calibration_gap=bucket.calibration_gap,
                observed_resolution_rate_ci_low=bucket.observed_resolution_rate_ci_low,
                observed_resolution_rate_ci_high=bucket.observed_resolution_rate_ci_high,
                calibration_gap_ci_low=bucket.calibration_gap_ci_low,
                calibration_gap_ci_high=bucket.calibration_gap_ci_high,
                support_flag=bucket.support_flag,
                recommended_merge_bucket=merge_target,
                recommended_action=bucket.recommended_action,
                provisional_calibrated_f=bucket.provisional_calibrated_f,
                session_snapshot_counts=bucket.session_snapshot_counts,
                session_window_counts=bucket.session_window_counts,
            )
        )
    return updated


def _nearest_supported_bucket_name(
    bucket_results: Sequence[CalibrationBucketResult],
    *,
    current_index: int,
) -> str | None:
    current = bucket_results[current_index]
    current_midpoint = _bucket_midpoint(current)
    candidates: list[tuple[Decimal, str]] = []
    for index, bucket in enumerate(bucket_results):
        if index == current_index or bucket.snapshot_count == 0:
            continue
        candidates.append((abs(_bucket_midpoint(bucket) - current_midpoint), bucket.bucket_name))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1]


def _bucket_midpoint(bucket: CalibrationBucketResult) -> Decimal:
    return (bucket.lower_bound_inclusive + bucket.upper_bound) / Decimal("2")


__all__ = [
    "CalibrationBucketDefinition",
    "CalibrationBucketResult",
    "CalibrationObservation",
    "CalibrationSupportThreshold",
    "GoodOnlyCalibrationConfig",
    "GoodOnlyCalibrationSummary",
    "build_good_only_calibration_summary",
    "build_good_only_calibration_summary_from_rollups",
    "classify_calibration_bucket",
    "good_only_calibration_summary_to_dict",
    "load_good_only_calibration_config",
]
