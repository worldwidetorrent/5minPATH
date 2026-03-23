from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from rtds.features.executable_edge import EdgeCostPolicy
from rtds.replay.calibrated_baseline import (
    apply_frozen_stage1_calibration,
    build_calibrated_baseline_session_comparison,
    load_frozen_calibration_runtime,
)
from rtds.replay.simulate import EntryRulePolicy, FeeCurvePolicy
from rtds.storage.writer import write_json_file


def test_apply_frozen_stage1_calibration_only_uses_sufficient_buckets(tmp_path: Path) -> None:
    summary_path = tmp_path / "calibration-summary.json"
    write_json_file(
        summary_path,
        {
            "calibration_id": "good-only-coarse-v1",
            "buckets": [
                {
                    "bucket_name": "far_down",
                    "lower_bound_inclusive": "0.00",
                    "upper_bound": "0.35",
                    "upper_bound_inclusive": False,
                    "support_flag": "sufficient",
                    "provisional_calibrated_f": "0.30",
                },
                {
                    "bucket_name": "lean_down",
                    "lower_bound_inclusive": "0.35",
                    "upper_bound": "0.47",
                    "upper_bound_inclusive": False,
                    "support_flag": "thin",
                    "provisional_calibrated_f": "0.50",
                },
                {
                    "bucket_name": "near_mid",
                    "lower_bound_inclusive": "0.47",
                    "upper_bound": "0.53",
                    "upper_bound_inclusive": False,
                    "support_flag": "thin",
                    "provisional_calibrated_f": "0.56",
                },
                {
                    "bucket_name": "lean_up",
                    "lower_bound_inclusive": "0.53",
                    "upper_bound": "0.65",
                    "upper_bound_inclusive": False,
                    "support_flag": "sufficient",
                    "provisional_calibrated_f": "0.49",
                },
                {
                    "bucket_name": "far_up",
                    "lower_bound_inclusive": "0.65",
                    "upper_bound": "1.00",
                    "upper_bound_inclusive": True,
                    "support_flag": "sufficient",
                    "provisional_calibrated_f": "0.60",
                },
            ],
        },
    )
    runtime = load_frozen_calibration_runtime(
        config_path="configs/replay/calibration_good_only_v1.json",
        summary_path=summary_path,
    )

    far_down = apply_frozen_stage1_calibration(Decimal("0.20"), runtime=runtime)
    near_mid = apply_frozen_stage1_calibration(Decimal("0.50"), runtime=runtime)

    assert far_down.bucket_name == "far_down"
    assert far_down.support_flag == "sufficient"
    assert far_down.calibration_applied is True
    assert far_down.calibrated_f == Decimal("0.30")

    assert near_mid.bucket_name == "near_mid"
    assert near_mid.support_flag == "thin"
    assert near_mid.calibration_applied is False
    assert near_mid.calibrated_f == Decimal("0.50")


def test_build_calibrated_baseline_session_comparison_tracks_deltas(tmp_path: Path) -> None:
    summary_path = tmp_path / "calibration-summary.json"
    write_json_file(
        summary_path,
        {
            "calibration_id": "good-only-coarse-v1",
            "buckets": [
                {
                    "bucket_name": "far_down",
                    "lower_bound_inclusive": "0.00",
                    "upper_bound": "0.35",
                    "upper_bound_inclusive": False,
                    "support_flag": "sufficient",
                    "provisional_calibrated_f": "0.60",
                },
                {
                    "bucket_name": "lean_down",
                    "lower_bound_inclusive": "0.35",
                    "upper_bound": "0.47",
                    "upper_bound_inclusive": False,
                    "support_flag": "sufficient",
                    "provisional_calibrated_f": "0.55",
                },
                {
                    "bucket_name": "near_mid",
                    "lower_bound_inclusive": "0.47",
                    "upper_bound": "0.53",
                    "upper_bound_inclusive": False,
                    "support_flag": "thin",
                    "provisional_calibrated_f": "0.56",
                },
                {
                    "bucket_name": "lean_up",
                    "lower_bound_inclusive": "0.53",
                    "upper_bound": "0.65",
                    "upper_bound_inclusive": False,
                    "support_flag": "sufficient",
                    "provisional_calibrated_f": "0.49",
                },
                {
                    "bucket_name": "far_up",
                    "lower_bound_inclusive": "0.65",
                    "upper_bound": "1.00",
                    "upper_bound_inclusive": True,
                    "support_flag": "sufficient",
                    "provisional_calibrated_f": "0.60",
                },
            ],
        },
    )
    runtime = load_frozen_calibration_runtime(
        config_path="configs/replay/calibration_good_only_v1.json",
        summary_path=summary_path,
    )

    comparison, rows = build_calibrated_baseline_session_comparison(
        (
            _evaluation_row(window_id="good-1", fair_value=Decimal("0.20")),
            _evaluation_row(window_id="good-2", fair_value=Decimal("0.50")),
        ),
        session_label="baseline_6h",
        session_id="session-a",
        capture_date="2026-03-16",
        replay_config=SimpleNamespace(
            edge_cost_policy=EdgeCostPolicy.from_bps(
                taker_fee_bps=0,
                slippage_up_bps=10,
                slippage_down_bps=10,
                model_uncertainty_bps=0,
            ),
            fee_curve=FeeCurvePolicy(taker_fee_rate=Decimal("0")),
            entry_rules=EntryRulePolicy(
                min_net_edge=Decimal("0"),
                target_trade_size_contracts=Decimal("1"),
            ),
        ),
        runtime=runtime,
    )

    assert comparison.row_count == 2
    assert comparison.calibration_bucket_counts == {"far_down": 1, "near_mid": 1}
    assert comparison.calibration_support_flag_counts == {"sufficient": 1, "thin": 1}
    assert comparison.calibration_applied_row_count == 1
    assert rows[0].raw_f == Decimal("0.20")
    assert rows[0].calibrated_f == Decimal("0.60")
    assert rows[1].raw_f == Decimal("0.50")
    assert rows[1].calibrated_f == Decimal("0.50")
    assert comparison.calibrated_summary.trade_count >= comparison.raw_summary.trade_count


def _evaluation_row(*, window_id: str, fair_value: Decimal):
    snapshot = SimpleNamespace(
        snapshot_id=f"snapshot-{window_id}",
        snapshot_ts=datetime(2026, 3, 17, 12, 0, 0, tzinfo=UTC),
        window_id=window_id,
        asset_id="BTC",
        polymarket_market_id="0x" + "1" * 64,
        created_ts=datetime(2026, 3, 17, 12, 0, 0, tzinfo=UTC),
        polymarket_quote_event_ts=datetime(2026, 3, 17, 12, 0, 0, tzinfo=UTC),
        polymarket_quote_recv_ts=datetime(2026, 3, 17, 12, 0, 0, tzinfo=UTC),
        up_bid=Decimal("0.48"),
        up_ask=Decimal("0.50"),
        down_bid=Decimal("0.49"),
        down_ask=Decimal("0.51"),
        up_bid_size_contracts=Decimal("10"),
        up_ask_size_contracts=Decimal("10"),
        down_bid_size_contracts=Decimal("10"),
        down_ask_size_contracts=Decimal("10"),
        market_mid_up=Decimal("0.49"),
        market_mid_down=Decimal("0.50"),
        market_spread_up_abs=Decimal("0.02"),
        market_spread_down_abs=Decimal("0.02"),
        last_trade_price=None,
        last_trade_size_contracts=None,
    )
    return SimpleNamespace(
        snapshot=snapshot,
        labeled_snapshot=SimpleNamespace(
            snapshot=snapshot,
            label=SimpleNamespace(label_status="attached", resolved_up=True),
        ),
        fair_value=SimpleNamespace(fair_value_base=fair_value),
    )
