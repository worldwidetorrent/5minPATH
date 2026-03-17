from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from rtds.features.executable_edge import EdgeCostPolicy
from rtds.replay.execution_sensitivity import (
    ExecutionSensitivityVariant,
    build_execution_sensitivity_variant_result,
)
from rtds.replay.regime_compare import REGIME_DEGRADED_LIGHT_ONLY, WindowQualityRow
from rtds.replay.simulate import EntryRulePolicy, FeeCurvePolicy


def test_execution_sensitivity_variant_applies_spread_cap_and_coverage_filter() -> None:
    rows = (
        _evaluation_row(
            window_id="good-window",
            quote_event_ts=datetime(2026, 3, 16, 10, 0, 1, tzinfo=UTC),
            spread=Decimal("0.03"),
        ),
        _evaluation_row(
            window_id="degraded-window",
            quote_event_ts=datetime(2026, 3, 16, 10, 5, 1, tzinfo=UTC),
            spread=Decimal("0.03"),
        ),
    )
    window_quality = {
        "good-window": WindowQualityRow(
            window_id="good-window",
            window_verdict="good",
            quote_coverage_ratio=0.99,
            degraded_samples_outside_rollover_grace_window=0,
            max_consecutive_valid_empty_book=0,
            snapshot_eligible_ratio=1.0,
        ),
        "degraded-window": WindowQualityRow(
            window_id="degraded-window",
            window_verdict="degraded_light",
            quote_coverage_ratio=0.92,
            degraded_samples_outside_rollover_grace_window=1,
            max_consecutive_valid_empty_book=2,
            snapshot_eligible_ratio=0.9,
        ),
    }
    config = SimpleNamespace(
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

    tight_spread_result = build_execution_sensitivity_variant_result(
        rows,
        window_quality_by_window=window_quality,
        replay_config=config,
        variant=ExecutionSensitivityVariant(
            variant_name="tight",
            display_name="Tight",
            max_selected_spread_abs=Decimal("0.02"),
        ),
        regime_order=(REGIME_DEGRADED_LIGHT_ONLY,),
    )
    strict_coverage_result = build_execution_sensitivity_variant_result(
        rows,
        window_quality_by_window=window_quality,
        replay_config=config,
        variant=ExecutionSensitivityVariant(
            variant_name="strict_cov",
            display_name="Strict coverage",
            minimum_window_quote_coverage_ratio=0.95,
        ),
        regime_order=(REGIME_DEGRADED_LIGHT_ONLY,),
    )

    tight_regime = tight_spread_result.regime_results[0]
    strict_coverage_regime = strict_coverage_result.regime_results[0]

    assert tight_regime.snapshot_count == 1
    assert tight_regime.trade_count == 0
    assert strict_coverage_regime.snapshot_count == 0
    assert strict_coverage_regime.trade_count == 0


def test_execution_sensitivity_variant_reduces_net_edge_under_higher_slippage() -> None:
    rows = (
        _evaluation_row(
            window_id="degraded-window",
            quote_event_ts=datetime(2026, 3, 16, 10, 5, 1, tzinfo=UTC),
            spread=Decimal("0.01"),
        ),
    )
    window_quality = {
        "degraded-window": WindowQualityRow(
            window_id="degraded-window",
            window_verdict="degraded_light",
            quote_coverage_ratio=0.97,
            degraded_samples_outside_rollover_grace_window=0,
            max_consecutive_valid_empty_book=1,
            snapshot_eligible_ratio=0.95,
        )
    }
    config = SimpleNamespace(
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

    baseline = build_execution_sensitivity_variant_result(
        rows,
        window_quality_by_window=window_quality,
        replay_config=config,
        variant=ExecutionSensitivityVariant(
            variant_name="baseline",
            display_name="Baseline",
        ),
        regime_order=(REGIME_DEGRADED_LIGHT_ONLY,),
    )
    stressed = build_execution_sensitivity_variant_result(
        rows,
        window_quality_by_window=window_quality,
        replay_config=config,
        variant=ExecutionSensitivityVariant(
            variant_name="stressed",
            display_name="Stressed",
            slippage_multiplier=Decimal("2"),
        ),
        regime_order=(REGIME_DEGRADED_LIGHT_ONLY,),
    )

    assert (
        stressed.regime_results[0].average_selected_net_edge
        < baseline.regime_results[0].average_selected_net_edge
    )


def _evaluation_row(*, window_id: str, quote_event_ts: datetime, spread: Decimal):
    snapshot = SimpleNamespace(
        snapshot_id=f"snapshot-{window_id}",
        window_id=window_id,
        asset_id="BTC",
        polymarket_market_id="0x" + ("1" * 64 if window_id == "good-window" else "2" * 64),
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
        seconds_remaining=120,
        volatility=SimpleNamespace(sigma_eff=Decimal("0.00008")),
    )
