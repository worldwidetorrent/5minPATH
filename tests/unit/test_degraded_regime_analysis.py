from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from rtds.features.executable_edge import EdgeCostPolicy
from rtds.replay.degraded_regime_analysis import (
    FOCUSED_DEGRADED_REGIME_ORDER,
    build_degraded_context_result,
    build_focused_degraded_stress_results,
)
from rtds.replay.regime_compare import WindowQualityRow
from rtds.replay.simulate import EntryRulePolicy, FeeCurvePolicy
from rtds.replay.slices import DEFAULT_REPLAY_SLICE_POLICY


def test_build_focused_degraded_stress_results_includes_light_and_medium() -> None:
    rows = (
        _evaluation_row(
            window_id="light-window",
            verdict="degraded_light",
            quote_event_ts=datetime(2026, 3, 17, 10, 0, 1, tzinfo=UTC),
            spread=Decimal("0.01"),
            net_edge=Decimal("0.04"),
        ),
        _evaluation_row(
            window_id="medium-window",
            verdict="degraded_medium",
            quote_event_ts=datetime(2026, 3, 17, 10, 5, 1, tzinfo=UTC),
            spread=Decimal("0.02"),
            net_edge=Decimal("0.03"),
        ),
    )
    window_quality = _window_quality()
    config = _config()

    results = build_focused_degraded_stress_results(
        rows,
        window_quality_by_window=window_quality,
        replay_config=config,
    )

    assert tuple(result.variant_name for result in results) == (
        "baseline_execution",
        "slippage_1_5x",
        "slippage_2x",
        "half_size",
    )
    assert tuple(
        regime.regime_name for regime in results[0].regime_results
    ) == FOCUSED_DEGRADED_REGIME_ORDER


def test_build_degraded_context_result_emits_requested_slices() -> None:
    rows = (
        _evaluation_row(
            window_id="medium-window",
            verdict="degraded_medium",
            quote_event_ts=datetime(2026, 3, 17, 10, 5, 1, tzinfo=UTC),
            spread=Decimal("0.04"),
            net_edge=Decimal("0.03"),
            seconds_remaining=45,
            sigma_eff=Decimal("0.00012"),
        ),
        _evaluation_row(
            window_id="medium-window-2",
            verdict="degraded_medium",
            quote_event_ts=datetime(2026, 3, 17, 10, 6, 1, tzinfo=UTC),
            spread=Decimal("0.01"),
            net_edge=Decimal("0.05"),
            seconds_remaining=220,
            sigma_eff=Decimal("0.00004"),
        ),
    )

    result = build_degraded_context_result(
        rows,
        window_quality_by_window=_window_quality(),
        regime_name="degraded_medium_only",
        slice_policy=DEFAULT_REPLAY_SLICE_POLICY,
    )

    assert result.snapshot_count == 2
    assert result.window_count == 2
    assert "seconds_remaining_bucket" in result.slices
    assert "volatility_regime" in result.slices
    assert "spread_bucket" in result.slices
    assert "net_edge_bucket" in result.slices
    assert "chainlink_confidence_state" in result.slices
    assert {row["slice_key"] for row in result.slices["spread_bucket"]} == {
        "tight_spread",
        "wide_spread",
    }


def _config():
    return SimpleNamespace(
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
    )


def _window_quality() -> dict[str, WindowQualityRow]:
    return {
        "light-window": WindowQualityRow(
            window_id="light-window",
            window_verdict="degraded_light",
            quote_coverage_ratio=0.96,
            degraded_samples_outside_rollover_grace_window=1,
            max_consecutive_valid_empty_book=2,
            snapshot_eligible_ratio=0.97,
        ),
        "medium-window": WindowQualityRow(
            window_id="medium-window",
            window_verdict="degraded_medium",
            quote_coverage_ratio=0.88,
            degraded_samples_outside_rollover_grace_window=3,
            max_consecutive_valid_empty_book=5,
            snapshot_eligible_ratio=0.85,
        ),
        "medium-window-2": WindowQualityRow(
            window_id="medium-window-2",
            window_verdict="degraded_medium",
            quote_coverage_ratio=0.86,
            degraded_samples_outside_rollover_grace_window=2,
            max_consecutive_valid_empty_book=4,
            snapshot_eligible_ratio=0.82,
        ),
    }


def _evaluation_row(
    *,
    window_id: str,
    verdict: str,
    quote_event_ts: datetime,
    spread: Decimal,
    net_edge: Decimal,
    seconds_remaining: int = 120,
    sigma_eff: Decimal = Decimal("0.00008"),
):
    snapshot = SimpleNamespace(
        snapshot_id=f"snapshot-{window_id}",
        window_id=window_id,
        asset_id="BTC",
        polymarket_market_id="0x" + ("1" * 64 if verdict == "degraded_light" else "2" * 64),
        created_ts=quote_event_ts,
        polymarket_quote_event_ts=quote_event_ts,
        polymarket_quote_recv_ts=quote_event_ts,
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
        market_spread_up_abs=spread,
        market_spread_down_abs=spread,
        snapshot_usable_flag=True,
        exchange_quality_usable_flag=True,
        reference_complete_flag=True,
        chainlink_quality_usable_flag=True,
        last_trade_price=None,
        last_trade_size_contracts=None,
    )
    label = SimpleNamespace(label_status="attached", resolved_up=True)
    return SimpleNamespace(
        snapshot=snapshot,
        labeled_snapshot=SimpleNamespace(snapshot=snapshot, label=label),
        fair_value=SimpleNamespace(fair_value_base=Decimal("0.56")),
        edge=SimpleNamespace(
            preferred_side="up",
            edge_up_raw=net_edge + Decimal("0.01"),
            edge_down_raw=Decimal("-0.01"),
            edge_up_net=net_edge,
            edge_down_net=Decimal("-0.02"),
        ),
        simulated_trade=SimpleNamespace(
            snapshot_id=f"snapshot-{window_id}",
            window_id=window_id,
            polymarket_market_id=snapshot.polymarket_market_id,
            sim_trade_direction="buy_up",
            sim_entry_price=Decimal("0.50"),
            sim_exit_price=Decimal("1"),
            sim_fee_paid=Decimal("0"),
            sim_slippage_paid=Decimal("0"),
            sim_pnl=Decimal("0.05"),
            sim_roi=Decimal("0.10"),
            sim_outcome="win",
            predicted_edge_net=net_edge,
            realized_edge=Decimal("0.05"),
            no_trade_reason=None,
            simulation_version="0.1.0",
        ),
        seconds_remaining=seconds_remaining,
        volatility=SimpleNamespace(sigma_eff=sigma_eff),
    )
