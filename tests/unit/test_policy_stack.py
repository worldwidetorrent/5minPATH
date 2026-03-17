from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from rtds.features.executable_edge import EdgeCostPolicy
from rtds.replay.policy_stack import build_policy_stack_result, load_policy_stack
from rtds.replay.regime_compare import WindowQualityRow
from rtds.replay.simulate import EntryRulePolicy, FeeCurvePolicy, SimulatedTrade


def test_build_policy_stack_result_applies_gated_medium_rule(tmp_path: Path) -> None:
    stack_path = tmp_path / "stack.yaml"
    baseline_path = tmp_path / "good.yaml"
    medium_path = tmp_path / "medium.yaml"
    baseline_path.write_text(
        "\n".join(
            [
                "policy_name: good_only_baseline",
                "policy_role: baseline",
                "window_quality_regime: good",
                "min_net_edge: 0",
                "target_trade_size_contracts: 1",
                "slippage_multiplier_assumption: 1",
                "status: active_baseline",
                "notes: baseline",
                "",
            ]
        ),
        encoding="utf-8",
    )
    medium_path.write_text(
        "\n".join(
            [
                "policy_name: degraded_medium_context_gated",
                "policy_role: exploratory_context_gate",
                "window_quality_regime: degraded_medium",
                "min_net_edge: 0.03",
                "target_trade_size_contracts: 0.5",
                "slippage_multiplier_assumption: 1.5",
                'required_volatility_regimes: ["mid_vol", "high_vol"]',
                'required_spread_buckets: ["wide_spread"]',
                'required_net_edge_buckets: ["large_positive_edge"]',
                "status: exploratory_only",
                "notes: medium gate",
                "",
            ]
        ),
        encoding="utf-8",
    )
    stack_path.write_text(
        "\n".join(
            [
                "stack_name: baseline_plus_gated_medium",
                "stack_role: exploratory_overlay_with_context_gate",
                f'policy_paths: ["{baseline_path}", "{medium_path}"]',
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = build_policy_stack_result(
        (
            _evaluation_row(
                window_id="good-window",
                raw_edge=Decimal("0.05"),
                net_edge=Decimal("0.04"),
                spread=Decimal("0.01"),
                sigma_eff=Decimal("0.00004"),
            ),
            _evaluation_row(
                window_id="medium-window-pass",
                raw_edge=Decimal("0.06"),
                net_edge=Decimal("0.04"),
                spread=Decimal("0.04"),
                sigma_eff=Decimal("0.00012"),
            ),
            _evaluation_row(
                window_id="medium-window-fail",
                raw_edge=Decimal("0.02"),
                net_edge=Decimal("0.005"),
                spread=Decimal("0.01"),
                sigma_eff=Decimal("0.00004"),
            ),
        ),
        window_quality_by_window={
            "good-window": WindowQualityRow(
                window_id="good-window",
                window_verdict="good",
                quote_coverage_ratio=0.99,
                degraded_samples_outside_rollover_grace_window=0,
                max_consecutive_valid_empty_book=0,
                snapshot_eligible_ratio=1.0,
            ),
            "medium-window-pass": WindowQualityRow(
                window_id="medium-window-pass",
                window_verdict="degraded_medium",
                quote_coverage_ratio=0.90,
                degraded_samples_outside_rollover_grace_window=2,
                max_consecutive_valid_empty_book=4,
                snapshot_eligible_ratio=0.88,
            ),
            "medium-window-fail": WindowQualityRow(
                window_id="medium-window-fail",
                window_verdict="degraded_medium",
                quote_coverage_ratio=0.90,
                degraded_samples_outside_rollover_grace_window=2,
                max_consecutive_valid_empty_book=4,
                snapshot_eligible_ratio=0.88,
            ),
        },
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
        stack=load_policy_stack(stack_path),
    )

    assert result.snapshot_count == 2
    assert result.window_count == 2
    assert result.included_window_verdict_counts == {"degraded_medium": 1, "good": 1}
    assert result.matched_policy_counts == {
        "degraded_medium_context_gated": 1,
        "good_only_baseline": 1,
    }


def _evaluation_row(
    *,
    window_id: str,
    raw_edge: Decimal,
    net_edge: Decimal,
    spread: Decimal,
    sigma_eff: Decimal,
):
    snapshot = SimpleNamespace(
        snapshot_id=f"snapshot-{window_id}",
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
        market_spread_up_abs=spread,
        market_spread_down_abs=spread,
        snapshot_usable_flag=True,
        exchange_quality_usable_flag=True,
        reference_complete_flag=True,
        chainlink_quality_usable_flag=True,
        last_trade_price=None,
        last_trade_size_contracts=None,
    )
    return SimpleNamespace(
        snapshot=snapshot,
        labeled_snapshot=SimpleNamespace(
            snapshot=snapshot,
            label=SimpleNamespace(label_status="attached", resolved_up=True),
        ),
        fair_value=SimpleNamespace(fair_value_base=Decimal("0.56")),
        edge=SimpleNamespace(
            preferred_side="up",
            edge_up_raw=raw_edge,
            edge_down_raw=Decimal("-0.01"),
            edge_up_net=net_edge,
            edge_down_net=Decimal("-0.02"),
        ),
        simulated_trade=SimulatedTrade(
            snapshot_id=snapshot.snapshot_id,
            window_id=window_id,
            polymarket_market_id=snapshot.polymarket_market_id,
            sim_trade_direction="buy_up",
            sim_entry_price=Decimal("0.5"),
            sim_exit_price=Decimal("1"),
            sim_fee_paid=Decimal("0"),
            sim_slippage_paid=Decimal("0"),
            sim_pnl=Decimal("0.05"),
            sim_roi=Decimal("0.1"),
            sim_outcome="win",
            predicted_edge_net=net_edge,
            realized_edge=Decimal("0.05"),
            no_trade_reason=None,
            simulation_version="0.1.0",
        ),
        seconds_remaining=90,
        volatility=SimpleNamespace(sigma_eff=sigma_eff),
    )
