"""Shared helpers for daily fast-lane and checkpoint analysis workflows."""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

from rtds.storage.writer import serialize_value


def load_json(path: str | Path) -> dict[str, Any]:
    """Load one JSON artifact."""

    return json.loads(Path(path).read_text(encoding="utf-8"))


def latest_run_dir(root: str | Path) -> Path | None:
    """Return the latest run_* directory under one root."""

    run_root = Path(root)
    runs = sorted(path for path in run_root.glob("run_*") if path.is_dir())
    if not runs:
        return None
    return runs[-1]


def classify_shadow_baseline(
    shadow_summary: dict[str, Any] | None,
) -> tuple[bool | None, str | None]:
    """Classify whether one shadow session is a clean baseline day."""

    if shadow_summary is None:
        return None, None
    no_trade_counts = shadow_summary.get("no_trade_reason_counts", {})
    if int(no_trade_counts.get("future_recv_visibility_leak", 0)) > 0:
        return False, "future_recv_visibility_leak"
    if int(no_trade_counts.get("future_state_leak_detected", 0)) > 0:
        return False, "future_state_leak_detected"
    return True, None


def build_day_tracker_entry(
    *,
    capture_date: str,
    session_id: str,
    summary: dict[str, Any],
    admission: dict[str, Any],
    policy_stack_summary: dict[str, Any],
    calibrated_summary: dict[str, Any],
    calibration_summary: dict[str, Any],
    cross_horizon_summary: dict[str, Any],
    artifact_paths: dict[str, str],
    shadow_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one tracker entry from persisted artifacts."""

    session_calibrated = next(
        session for session in calibrated_summary["sessions"] if session["session_id"] == session_id
    )
    cross_horizon_session = next(
        session
        for session in cross_horizon_summary["sessions"]
        if session["session_id"] == session_id
    )
    bucket_ci_widths = {
        bucket["bucket_name"]: (
            float(bucket["calibration_gap_ci_high"]) - float(bucket["calibration_gap_ci_low"])
        )
        for bucket in calibration_summary["buckets"]
    }
    near_mid_bucket = next(
        (
            bucket
            for bucket in calibration_summary["buckets"]
            if bucket["bucket_name"] == "near_mid"
        ),
        None,
    )
    shadow_clean_baseline, shadow_reason = classify_shadow_baseline(shadow_summary)
    entry = {
        "capture_date": capture_date,
        "session_id": session_id,
        "termination_reason": summary["session_diagnostics"]["termination_reason"],
        "lifecycle_state": summary["session_diagnostics"]["lifecycle_state"],
        "sample_count": summary["sample_count"],
        "good_window_count": admission["polymarket_continuity"]["window_verdict_counts"]["good"],
        "good_snapshot_count": calibration_summary["total_snapshot_count"],
        "session_good_window_count": cross_horizon_session["window_verdict_counts"]["good"],
        "snapshot_eligible_sample_count": admission["snapshot_eligibility"][
            "snapshot_eligible_sample_count"
        ],
        "snapshot_eligible_sample_ratio": admission["snapshot_eligibility"][
            "snapshot_eligible_sample_ratio"
        ],
        "bucket_support_flags": {
            bucket["bucket_name"]: bucket["support_flag"]
            for bucket in calibration_summary["buckets"]
        },
        "bucket_ci_widths": bucket_ci_widths,
        "support_flag_counts": calibration_summary["support_flag_counts"],
        "near_mid_support_flag": near_mid_bucket["support_flag"] if near_mid_bucket else None,
        "near_mid_ci_width": bucket_ci_widths.get("near_mid"),
        "policy_stack_metrics": {
            stack["stack_name"]: {
                "trade_count": stack["trade_count"],
                "hit_rate": stack["hit_rate"],
                "average_selected_net_edge": stack["average_selected_net_edge"],
                "total_pnl": stack["total_pnl"],
                "average_roi": stack["average_roi"],
                "pnl_per_window": stack["pnl_per_window"],
                "pnl_per_100_trades": stack["pnl_per_100_trades"],
                "pnl_per_1000_snapshots": stack["pnl_per_1000_snapshots"],
            }
            for stack in policy_stack_summary["stacks"]
        },
        "calibrated_baseline_metrics": {
            "raw_summary": session_calibrated["raw_summary"],
            "calibrated_summary": session_calibrated["calibrated_summary"],
            "delta_total_pnl": session_calibrated["delta_total_pnl"],
            "delta_average_roi": session_calibrated["delta_average_roi"],
            "delta_average_selected_net_edge": session_calibrated[
                "delta_average_selected_net_edge"
            ],
            "delta_trade_count": session_calibrated["delta_trade_count"],
            "calibration_applied_row_count": session_calibrated["calibration_applied_row_count"],
            "calibration_support_flag_counts": session_calibrated[
                "calibration_support_flag_counts"
            ],
        },
        "notable_anomalies": [
            "session remains quote-noisy overall despite clean structural continuity",
            "use good-only baseline metrics as the primary calibration signal",
        ],
        "artifact_paths": artifact_paths,
        "capture_valid": (
            summary["session_diagnostics"]["termination_reason"] == "completed"
            and admission["family_validation"]["off_family_switch_count"] == 0
            and admission["mapping_and_anchor"]["selected_binding_unresolved_window_count"] == 0
        ),
        "shadow_clean_baseline": shadow_clean_baseline,
        "shadow_reason": shadow_reason,
    }
    if shadow_summary is not None:
        entry["artifact_paths"]["shadow_summary_path"] = artifact_paths["shadow_summary_path"]
    return serialize_value(entry)


def build_shadow_quick_stage_a(
    shadow_summary: dict[str, Any],
    *,
    decisions_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build a cheap execution-side quick read for one shadow session."""

    summary: dict[str, Any] = {
        "processing_mode": shadow_summary.get("processing_mode"),
        "backlog_decision_count": int(shadow_summary.get("backlog_decision_count", 0)),
        "live_forward_decision_count": int(shadow_summary.get("live_forward_decision_count", 0)),
        "decision_count": int(
            shadow_summary.get(
                "written_decision_count",
                shadow_summary.get("decision_count", 0),
            )
        ),
        "actionable_decision_count": int(shadow_summary.get("actionable_decision_count", 0)),
        "no_trade_reason_counts": shadow_summary.get("no_trade_reason_counts", {}),
        "max_decision_lag_ms": shadow_summary.get("max_decision_lag_ms"),
    }
    if decisions_path is None or not Path(decisions_path).exists():
        return summary

    fair_value_non_null_count = 0
    calibrated_fair_value_non_null_count = 0
    trusted_counter: Counter[int] = Counter()
    three_trusted_rows = 0
    actionable_with_three_trusted = 0
    event_clock_skew_bucket_counts: Counter[str] = Counter()

    with Path(decisions_path).open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            state = row.get("executable_state", {})
            tradability = row.get("tradability_check", {})
            fair_value = state.get("fair_value_base")
            calibrated_fair_value = state.get("calibrated_fair_value_base")
            if fair_value is not None:
                fair_value_non_null_count += 1
            if calibrated_fair_value is not None:
                calibrated_fair_value_non_null_count += 1
            trusted_count = int(state.get("exchange_trusted_venue_count", 0) or 0)
            trusted_counter[trusted_count] += 1
            if trusted_count == 3:
                three_trusted_rows += 1
                if bool(tradability.get("is_actionable")):
                    actionable_with_three_trusted += 1
            diagnostics = state.get("state_diagnostics", {})
            skew_bucket = _extract_skew_bucket(diagnostics)
            if skew_bucket is not None:
                event_clock_skew_bucket_counts[skew_bucket] += 1

    summary["fair_value_non_null_count"] = fair_value_non_null_count
    summary["calibrated_fair_value_non_null_count"] = calibrated_fair_value_non_null_count
    summary["trusted_venue_count_distribution"] = {
        str(key): trusted_counter[key] for key in sorted(trusted_counter)
    }
    summary["three_trusted_venue_row_count"] = three_trusted_rows
    summary["three_trusted_venue_rate"] = _safe_decimal_ratio(
        three_trusted_rows,
        summary["decision_count"],
    )
    summary["actionable_given_three_trusted_rate"] = _safe_decimal_ratio(
        actionable_with_three_trusted,
        three_trusted_rows,
    )
    summary["event_clock_skew_bucket_counts"] = dict(event_clock_skew_bucket_counts)
    return serialize_value(summary)


def _safe_decimal_ratio(numerator: int, denominator: int) -> str | None:
    if denominator <= 0:
        return None
    return str(Decimal(numerator) / Decimal(denominator))


def _extract_skew_bucket(diagnostics: Any) -> str | None:
    if isinstance(diagnostics, dict):
        skew_bucket = diagnostics.get("future_event_clock_skew_bucket")
        return None if skew_bucket is None else str(skew_bucket)
    if isinstance(diagnostics, list):
        prefix = "future_event_clock_skew:quote_event_ts:"
        for item in diagnostics:
            if isinstance(item, str) and item.startswith(prefix):
                return item.removeprefix(prefix)
    return None
