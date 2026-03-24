from __future__ import annotations

import json
from pathlib import Path

from rtds.replay.calibration_profile_compare import (
    build_calibration_profile_comparison,
    calibration_profile_comparison_to_dict,
    load_calibration_profile,
    render_calibration_profile_comparison,
)


def test_compare_calibration_profiles_builds_side_by_side_rows(tmp_path: Path) -> None:
    left_path = tmp_path / "left.json"
    right_path = tmp_path / "right.json"
    left_path.write_text(json.dumps(_profile_payload("s12", "-5", "far_up", "-10", "5")))
    right_path.write_text(json.dumps(_profile_payload("s24", "10", "far_up", "20", "15")))

    comparison = build_calibration_profile_comparison(
        left=load_calibration_profile(left_path),
        right=load_calibration_profile(right_path),
        buckets=("far_up",),
        dimensions=("volatility_regime", "spread_bucket"),
    )

    assert comparison.left.session_id == "s12"
    assert comparison.right.session_id == "s24"
    first = comparison.comparisons[0]
    assert first.bucket == "far_up"
    assert first.dimension == "volatility_regime"
    assert [row.value for row in first.rows] == ["low_vol", "mid_vol"]
    low_vol = first.rows[0]
    assert str(low_vol.left_delta_pnl) == "-10"
    assert str(low_vol.right_delta_pnl) == "20"


def test_compare_calibration_profiles_serializes_and_renders(tmp_path: Path) -> None:
    left_path = tmp_path / "left.json"
    right_path = tmp_path / "right.json"
    left_path.write_text(json.dumps(_profile_payload("s12", "-5", "far_up", "-10", "5")))
    right_path.write_text(json.dumps(_profile_payload("s24", "10", "far_up", "20", "15")))

    comparison = build_calibration_profile_comparison(
        left=load_calibration_profile(left_path),
        right=load_calibration_profile(right_path),
        buckets=("far_up",),
        dimensions=("volatility_regime",),
    )

    payload = calibration_profile_comparison_to_dict(comparison)
    report = render_calibration_profile_comparison(comparison)

    assert payload["left"]["session_id"] == "s12"
    assert payload["right"]["session_id"] == "s24"
    assert "12h vs 24h Far-Up / Lean-Up Diagnosis" in report
    assert "`far_up` does not show the same sign pattern" in report
    assert "| low_vol |" in report


def _profile_payload(
    session_id: str,
    delta_total_pnl: str,
    bucket: str,
    low_vol_delta: str,
    tight_spread_delta: str,
) -> dict[str, object]:
    return {
        "analysis": "calibration_profile",
        "session_id": session_id,
        "capture_date": "2026-03-01",
        "session_label": session_id,
        "frozen_manifest_path": "manifest.json",
        "skipped_missing_calibrated_rows": 0,
        "session_summary": {
            "raw_total_pnl": "1",
            "calibrated_total_pnl": str(1 + int(delta_total_pnl)),
            "delta_total_pnl": delta_total_pnl,
            "raw_trade_count": 10,
            "calibrated_trade_count": 11,
            "delta_trade_count": 1,
        },
        "global_dimensions": {},
        "bucket_profiles": {
            bucket: {
                "volatility_regime": [
                    {
                        "volatility_regime": "low_vol",
                        "rows": 1,
                        "raw_trade_count": 2,
                        "calibrated_trade_count": 3,
                        "delta_trade_count": 1,
                        "raw_pnl": "1",
                        "calibrated_pnl": str(1 + int(low_vol_delta)),
                        "delta_pnl": low_vol_delta,
                        "raw_hit_rate": "0.5",
                        "calibrated_hit_rate": "0.6",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.2",
                    },
                    {
                        "volatility_regime": "mid_vol",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    },
                ],
                "spread_bucket": [
                    {
                        "spread_bucket": "tight_spread",
                        "rows": 1,
                        "raw_trade_count": 2,
                        "calibrated_trade_count": 2,
                        "delta_trade_count": 0,
                        "raw_pnl": "1",
                        "calibrated_pnl": str(1 + int(tight_spread_delta)),
                        "delta_pnl": tight_spread_delta,
                        "raw_hit_rate": "0.5",
                        "calibrated_hit_rate": "0.5",
                        "raw_avg_edge": "0.2",
                        "calibrated_avg_edge": "0.3",
                    }
                ],
                "seconds_remaining_bucket": [
                    {
                        "seconds_remaining_bucket": "early_window",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    },
                    {
                        "seconds_remaining_bucket": "mid_window",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    },
                ],
                "hour_utc": [
                    {
                        "hour_utc": "10",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    }
                ],
            },
            "lean_up": {
                "volatility_regime": [
                    {
                        "volatility_regime": "low_vol",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    }
                ],
                "spread_bucket": [
                    {
                        "spread_bucket": "tight_spread",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    }
                ],
                "seconds_remaining_bucket": [
                    {
                        "seconds_remaining_bucket": "early_window",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    },
                    {
                        "seconds_remaining_bucket": "mid_window",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    },
                ],
                "hour_utc": [
                    {
                        "hour_utc": "10",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "0",
                        "delta_pnl": "0",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "0",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.1",
                    }
                ],
            },
            "far_down": {
                "volatility_regime": [
                    {
                        "volatility_regime": "low_vol",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "1",
                        "delta_pnl": "1",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "1",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.2",
                    }
                ],
                "spread_bucket": [
                    {
                        "spread_bucket": "tight_spread",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "1",
                        "delta_pnl": "1",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "1",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.2",
                    }
                ],
                "seconds_remaining_bucket": [
                    {
                        "seconds_remaining_bucket": "early_window",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "1",
                        "delta_pnl": "1",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "1",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.2",
                    },
                    {
                        "seconds_remaining_bucket": "mid_window",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "1",
                        "delta_pnl": "1",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "1",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.2",
                    },
                ],
                "hour_utc": [
                    {
                        "hour_utc": "10",
                        "rows": 1,
                        "raw_trade_count": 1,
                        "calibrated_trade_count": 1,
                        "delta_trade_count": 0,
                        "raw_pnl": "0",
                        "calibrated_pnl": "1",
                        "delta_pnl": "1",
                        "raw_hit_rate": "0",
                        "calibrated_hit_rate": "1",
                        "raw_avg_edge": "0.1",
                        "calibrated_avg_edge": "0.2",
                    }
                ],
            },
        },
    }
