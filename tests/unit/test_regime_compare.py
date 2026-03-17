from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from rtds.replay.regime_compare import (
    REGIME_ALL_WINDOWS,
    REGIME_DEGRADED_HEAVY_ONLY,
    REGIME_DEGRADED_LIGHT_ONLY,
    REGIME_DEGRADED_LIGHT_PLUS_MEDIUM,
    REGIME_DEGRADED_MEDIUM_ONLY,
    REGIME_DEGRADED_ONLY,
    REGIME_GOOD_ONLY,
    REGIME_GOOD_PLUS_ALL_DEGRADED,
    REGIME_GOOD_PLUS_DEGRADED_LIGHT,
    REGIME_GOOD_PLUS_DEGRADED_LIGHT_MEDIUM,
    build_regime_result,
    load_window_quality_rows,
    load_window_verdicts,
)
from rtds.replay.simulate import SimulatedTrade


def test_load_window_verdicts_reads_capture_admission_rows(tmp_path: Path) -> None:
    admission_summary_path = tmp_path / "admission_summary.json"
    admission_summary_path.write_text(
        json.dumps(
            {
                "polymarket_continuity": {
                    "window_quote_coverage": [
                        {"window_id": "w1", "window_verdict": "good"},
                        {"window_id": "w2", "window_verdict": "degraded_light"},
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    verdicts = load_window_verdicts(admission_summary_path)

    assert verdicts == {"w1": "good", "w2": "degraded_light"}


def test_load_window_quality_rows_reads_capture_admission_metrics(tmp_path: Path) -> None:
    admission_summary_path = tmp_path / "admission_summary.json"
    admission_summary_path.write_text(
        json.dumps(
            {
                "polymarket_continuity": {
                    "window_quote_coverage": [
                        {
                            "window_id": "w1",
                            "window_verdict": "degraded_light",
                            "quote_coverage_ratio": 0.94,
                            "degraded_samples_outside_rollover_grace_window": 1,
                            "max_consecutive_valid_empty_book": 2,
                            "snapshot_eligible_ratio": 0.9,
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    rows = load_window_quality_rows(admission_summary_path)

    assert rows["w1"].window_verdict == "degraded_light"
    assert rows["w1"].quote_coverage_ratio == 0.94
    assert rows["w1"].degraded_samples_outside_rollover_grace_window == 1
    assert rows["w1"].max_consecutive_valid_empty_book == 2
    assert rows["w1"].snapshot_eligible_ratio == 0.9


def test_build_regime_result_filters_window_verdicts_and_preserves_metrics() -> None:
    rows = [
        _evaluation_row(
            window_id="w1",
            preferred_side="up",
            raw_edge=Decimal("0.05"),
            net_edge=Decimal("0.03"),
            pnl=Decimal("0.08"),
            roi=Decimal("0.16"),
            outcome="win",
            seconds_remaining=240,
            sigma_eff=Decimal("0.00004"),
            quality="green",
        ),
        _evaluation_row(
            window_id="w2",
            preferred_side="down",
            raw_edge=Decimal("0.03"),
            net_edge=Decimal("0.01"),
            pnl=Decimal("-0.02"),
            roi=Decimal("-0.04"),
            outcome="loss",
            seconds_remaining=120,
            sigma_eff=Decimal("0.00008"),
            quality="yellow",
        ),
        _evaluation_row(
            window_id="w3",
            preferred_side="up",
            raw_edge=Decimal("0.02"),
            net_edge=Decimal("0.005"),
            pnl=Decimal("0.01"),
            roi=Decimal("0.02"),
            outcome="win",
            seconds_remaining=60,
            sigma_eff=Decimal("0.00010"),
            quality="yellow",
        ),
        _evaluation_row(
            window_id="w4",
            preferred_side=None,
            raw_edge=None,
            net_edge=None,
            pnl=Decimal("0"),
            roi=Decimal("0"),
            outcome="no_trade",
            seconds_remaining=30,
            sigma_eff=Decimal("0.00012"),
            quality="red",
        ),
    ]
    verdicts = {
        "w1": "good",
        "w2": "degraded_light",
        "w3": "degraded_medium",
        "w4": "unusable",
    }
    window_quality = {
        "w1": SimpleNamespace(quote_coverage_ratio=0.99),
        "w2": SimpleNamespace(quote_coverage_ratio=0.94),
        "w3": SimpleNamespace(quote_coverage_ratio=0.84),
        "w4": SimpleNamespace(quote_coverage_ratio=0.10),
    }

    good_only = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_GOOD_ONLY,
    )
    degraded_only = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_DEGRADED_ONLY,
    )
    light_only = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_DEGRADED_LIGHT_ONLY,
    )
    medium_only = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_DEGRADED_MEDIUM_ONLY,
    )
    heavy_only = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_DEGRADED_HEAVY_ONLY,
    )
    light_plus_medium = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_DEGRADED_LIGHT_PLUS_MEDIUM,
    )
    good_plus_light = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_GOOD_PLUS_DEGRADED_LIGHT,
    )
    good_plus_light_medium = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_GOOD_PLUS_DEGRADED_LIGHT_MEDIUM,
    )
    good_plus_all_degraded = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_GOOD_PLUS_ALL_DEGRADED,
    )
    all_windows = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_ALL_WINDOWS,
    )
    strict_light = build_regime_result(
        rows,
        window_verdict_by_window=verdicts,
        regime_name=REGIME_GOOD_PLUS_DEGRADED_LIGHT,
        window_quality_by_window=window_quality,
        minimum_window_quote_coverage_ratio=0.95,
    )

    assert good_only.snapshot_count == 1
    assert good_only.window_verdict_counts == {"good": 1}
    assert good_only.trade_count == 1
    assert good_only.hit_rate == Decimal("1")
    assert good_only.average_selected_net_edge == Decimal("0.03")

    assert degraded_only.snapshot_count == 2
    assert degraded_only.window_verdict_counts == {
        "degraded_light": 1,
        "degraded_medium": 1,
    }
    assert degraded_only.trade_count == 2
    assert degraded_only.hit_rate == Decimal("0.5")
    assert degraded_only.average_selected_raw_edge == Decimal("0.025")

    assert light_only.snapshot_count == 1
    assert light_only.window_verdict_counts == {"degraded_light": 1}
    assert light_only.trade_count == 1
    assert light_only.pnl_per_window == Decimal("-0.02")
    assert light_only.pnl_per_1000_snapshots == Decimal("-20")
    assert light_only.pnl_per_100_trades == Decimal("-2")

    assert medium_only.snapshot_count == 1
    assert medium_only.window_verdict_counts == {"degraded_medium": 1}
    assert medium_only.trade_count == 1

    assert heavy_only.snapshot_count == 0
    assert heavy_only.window_verdict_counts == {}
    assert heavy_only.trade_count == 0
    assert heavy_only.pnl_per_window is None

    assert light_plus_medium.snapshot_count == 2
    assert light_plus_medium.window_verdict_counts == {
        "degraded_light": 1,
        "degraded_medium": 1,
    }

    assert good_plus_light.snapshot_count == 2
    assert good_plus_light.window_verdict_counts == {"degraded_light": 1, "good": 1}

    assert good_plus_light_medium.snapshot_count == 3
    assert good_plus_light_medium.window_verdict_counts == {
        "degraded_light": 1,
        "degraded_medium": 1,
        "good": 1,
    }

    assert good_plus_all_degraded.snapshot_count == 3
    assert good_plus_all_degraded.window_verdict_counts == {
        "degraded_light": 1,
        "degraded_medium": 1,
        "good": 1,
    }

    assert all_windows.snapshot_count == 3
    assert all_windows.window_verdict_counts == {
        "degraded_light": 1,
        "degraded_medium": 1,
        "good": 1,
    }
    assert all_windows.trade_count == 3
    assert (
        all_windows.slices["composite_quality_state"][0]["slice_dimension"]
        == "composite_quality_state"
    )
    assert strict_light.snapshot_count == 1
    assert strict_light.window_verdict_counts == {"good": 1}


def _evaluation_row(
    *,
    window_id: str,
    preferred_side: str | None,
    raw_edge: Decimal | None,
    net_edge: Decimal | None,
    pnl: Decimal,
    roi: Decimal,
    outcome: str,
    seconds_remaining: int,
    sigma_eff: Decimal,
    quality: str,
):
    snapshot = SimpleNamespace(
        window_id=window_id,
        snapshot_id=f"snapshot-{window_id}",
        snapshot_usable_flag=quality == "green",
        exchange_quality_usable_flag=quality in {"green", "yellow"},
        reference_complete_flag=True,
        chainlink_quality_usable_flag=True,
        market_spread_up_abs=Decimal("0.01"),
        market_spread_down_abs=Decimal("0.01"),
    )
    label = SimpleNamespace(label_status="attached")
    edge = SimpleNamespace(
        preferred_side=preferred_side,
        edge_up_raw=raw_edge if preferred_side in {None, "up"} else Decimal("-0.01"),
        edge_down_raw=raw_edge if preferred_side == "down" else Decimal("-0.01"),
        edge_up_net=net_edge if preferred_side in {None, "up"} else Decimal("-0.02"),
        edge_down_net=net_edge if preferred_side == "down" else Decimal("-0.02"),
    )
    trade_direction = (
        "no_trade"
        if outcome == "no_trade"
        else ("buy_up" if preferred_side == "up" else "buy_down")
    )
    simulated_trade = SimulatedTrade(
        snapshot_id=f"snapshot-{window_id}",
        window_id=window_id,
        polymarket_market_id=f"market-{window_id}",
        sim_trade_direction=trade_direction,
        sim_entry_price=Decimal("0.5") if trade_direction != "no_trade" else None,
        sim_exit_price=Decimal("1") if trade_direction != "no_trade" else None,
        sim_fee_paid=Decimal("0.01") if trade_direction != "no_trade" else None,
        sim_slippage_paid=Decimal("0.01") if trade_direction != "no_trade" else None,
        sim_pnl=pnl,
        sim_roi=roi,
        sim_outcome=outcome,
        predicted_edge_net=net_edge if trade_direction != "no_trade" else None,
        realized_edge=pnl if trade_direction != "no_trade" else None,
        no_trade_reason="entry_rule_blocked" if trade_direction == "no_trade" else None,
        simulation_version="0.1.0",
    )
    return SimpleNamespace(
        snapshot=snapshot,
        labeled_snapshot=SimpleNamespace(snapshot=snapshot, label=label),
        edge=edge,
        simulated_trade=simulated_trade,
        seconds_remaining=seconds_remaining,
        volatility=SimpleNamespace(sigma_eff=sigma_eff),
        created_ts=datetime(2026, 3, 16, 10, 15, tzinfo=UTC),
    )
