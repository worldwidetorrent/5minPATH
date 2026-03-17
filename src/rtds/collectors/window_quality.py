"""Versioned Polymarket window-quality classifier policy."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

DEFAULT_WINDOW_QUALITY_POLICY_PATH = Path("configs/replay/window_quality_classifier_v1.json")


@dataclass(slots=True, frozen=True)
class WindowQualityClassifierPolicy:
    """Stable classifier thresholds for per-window Polymarket quality."""

    classifier_version: str
    config_path: str
    label_order: tuple[str, ...]
    verdict_rule_order: tuple[str, ...]
    degraded_light_max_outside_grace_degraded_samples: int
    degraded_medium_max_outside_grace_degraded_samples: int
    degraded_light_max_consecutive_valid_empty_book: int
    degraded_medium_max_consecutive_valid_empty_book: int
    degraded_light_min_quote_coverage_ratio: float
    degraded_medium_min_quote_coverage_ratio: float
    degraded_light_min_snapshot_eligible_ratio: float
    degraded_medium_min_snapshot_eligible_ratio: float


def load_window_quality_classifier_policy(
    path: str | Path = DEFAULT_WINDOW_QUALITY_POLICY_PATH,
) -> WindowQualityClassifierPolicy:
    """Load the explicit versioned classifier contract from disk."""

    resolved_path = Path(path)
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    thresholds = dict(payload["thresholds"])
    return WindowQualityClassifierPolicy(
        classifier_version=str(payload["classifier_version"]),
        config_path=str(resolved_path),
        label_order=tuple(str(value) for value in payload["label_order"]),
        verdict_rule_order=tuple(str(value) for value in payload["verdict_rule_order"]),
        degraded_light_max_outside_grace_degraded_samples=int(
            thresholds["degraded_light_max_outside_grace_degraded_samples"]
        ),
        degraded_medium_max_outside_grace_degraded_samples=int(
            thresholds["degraded_medium_max_outside_grace_degraded_samples"]
        ),
        degraded_light_max_consecutive_valid_empty_book=int(
            thresholds["degraded_light_max_consecutive_valid_empty_book"]
        ),
        degraded_medium_max_consecutive_valid_empty_book=int(
            thresholds["degraded_medium_max_consecutive_valid_empty_book"]
        ),
        degraded_light_min_quote_coverage_ratio=float(
            thresholds["degraded_light_min_quote_coverage_ratio"]
        ),
        degraded_medium_min_quote_coverage_ratio=float(
            thresholds["degraded_medium_min_quote_coverage_ratio"]
        ),
        degraded_light_min_snapshot_eligible_ratio=float(
            thresholds["degraded_light_min_snapshot_eligible_ratio"]
        ),
        degraded_medium_min_snapshot_eligible_ratio=float(
            thresholds["degraded_medium_min_snapshot_eligible_ratio"]
        ),
    )


def window_quality_classifier_policy_to_dict(
    policy: WindowQualityClassifierPolicy,
    *,
    unusable_min_quote_coverage_ratio: float,
    pilot_runtime_max_consecutive_unusable_windows: int,
) -> dict[str, object]:
    """Render one classifier policy for capture admission artifacts."""

    return {
        "classifier_version": policy.classifier_version,
        "config_path": policy.config_path,
        "label_order": list(policy.label_order),
        "verdict_rule_order": list(policy.verdict_rule_order),
        "pilot_runtime_max_consecutive_unusable_windows": (
            pilot_runtime_max_consecutive_unusable_windows
        ),
        "unusable_min_quote_coverage_ratio": unusable_min_quote_coverage_ratio,
        "degraded_light_max_outside_grace_degraded_samples": (
            policy.degraded_light_max_outside_grace_degraded_samples
        ),
        "degraded_medium_max_outside_grace_degraded_samples": (
            policy.degraded_medium_max_outside_grace_degraded_samples
        ),
        "degraded_light_max_consecutive_valid_empty_book": (
            policy.degraded_light_max_consecutive_valid_empty_book
        ),
        "degraded_medium_max_consecutive_valid_empty_book": (
            policy.degraded_medium_max_consecutive_valid_empty_book
        ),
        "degraded_light_min_quote_coverage_ratio": (
            policy.degraded_light_min_quote_coverage_ratio
        ),
        "degraded_medium_min_quote_coverage_ratio": (
            policy.degraded_medium_min_quote_coverage_ratio
        ),
        "degraded_light_min_snapshot_eligible_ratio": (
            policy.degraded_light_min_snapshot_eligible_ratio
        ),
        "degraded_medium_min_snapshot_eligible_ratio": (
            policy.degraded_medium_min_snapshot_eligible_ratio
        ),
    }


__all__ = [
    "DEFAULT_WINDOW_QUALITY_POLICY_PATH",
    "WindowQualityClassifierPolicy",
    "load_window_quality_classifier_policy",
    "window_quality_classifier_policy_to_dict",
]
