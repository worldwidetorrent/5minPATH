from datetime import UTC, datetime
from decimal import Decimal

from rtds.features.executable_edge import (
    ExecutableEdgeEstimate,
    compute_executable_edge,
)
from rtds.replay.attach_labels import LabeledSnapshotRecord, SnapshotLabel
from rtds.replay.simulate import (
    SIM_BUY_DOWN,
    SIM_BUY_UP,
    SIM_NO_TRADE,
    SIM_OUTCOME_NO_TRADE,
    SIM_OUTCOME_WIN,
    EntryRulePolicy,
    FeeCurvePolicy,
    ReplaySimulationInput,
    simulate_replay,
    simulate_snapshot,
)
from rtds.schemas.snapshot import SnapshotRecord


def _snapshot(
    *,
    snapshot_id: str | None = None,
    market_id: str = "0xabc123",
    up_ask: str = "0.54",
    down_ask: str = "0.46",
) -> SnapshotRecord:
    ts = datetime(2026, 3, 14, 12, 5, 2, tzinfo=UTC)
    return SnapshotRecord(
        snapshot_ts=ts,
        window_id="btc-5m-20260314T120500Z",
        asset_id="BTC",
        polymarket_market_id=market_id,
        snapshot_origin="fixed_1s",
        window_start_ts=datetime(2026, 3, 14, 12, 5, 0, tzinfo=UTC),
        window_end_ts=datetime(2026, 3, 14, 12, 10, 0, tzinfo=UTC),
        polymarket_event_id="evt-1",
        polymarket_slug="btc-up-down-1205",
        mapping_status="mapped",
        assignment_status="complete",
        market_active_flag=True,
        market_closed_flag=False,
        oracle_feed_id="chainlink:stream:BTC-USD",
        chainlink_open_anchor_price=Decimal("84000"),
        chainlink_open_anchor_ts=datetime(2026, 3, 14, 12, 5, 0, tzinfo=UTC),
        chainlink_settle_price=Decimal("84025"),
        chainlink_settle_ts=datetime(2026, 3, 14, 12, 10, 0, tzinfo=UTC),
        chainlink_current_price=Decimal("84010"),
        chainlink_current_ts=ts,
        chainlink_current_age_ms=100,
        composite_now_price=Decimal("84010"),
        composite_method="median_3",
        composite_quality_score=Decimal("0.95"),
        composite_missing_flag=False,
        composite_contributing_venue_count=3,
        composite_contributing_venues=("binance", "coinbase", "kraken"),
        composite_per_venue_mids={"binance": Decimal("84010")},
        composite_per_venue_ages={"binance": 100},
        composite_dispersion_abs_usd=Decimal("2"),
        composite_dispersion_bps=Decimal("0.2"),
        polymarket_quote_event_ts=ts,
        polymarket_quote_recv_ts=ts,
        polymarket_quote_age_ms=100,
        up_bid=Decimal("0.52"),
        up_ask=Decimal(up_ask),
        down_bid=Decimal("0.44"),
        down_ask=Decimal(down_ask),
        up_bid_size_contracts=Decimal("100"),
        up_ask_size_contracts=Decimal("100"),
        down_bid_size_contracts=Decimal("100"),
        down_ask_size_contracts=Decimal("100"),
        market_mid_up=Decimal("0.53"),
        market_mid_down=Decimal("0.45"),
        market_spread_up_abs=Decimal("0.02"),
        market_spread_down_abs=Decimal("0.02"),
        last_trade_price=Decimal("0.53"),
        last_trade_size_contracts=Decimal("10"),
        exchange_quality_usable_flag=True,
        chainlink_quality_usable_flag=True,
        polymarket_quote_usable_flag=True,
        reference_complete_flag=True,
        snapshot_usable_flag=True,
        quality_diagnostics=(),
        schema_version="0.1.0",
        feature_version="0.1.0",
        created_ts=ts,
        snapshot_id=snapshot_id,
    )


def _label(
    snapshot_id: str,
    *,
    market_id: str = "0xabc123",
    resolved_up: bool | None = True,
    label_status: str = "attached",
) -> SnapshotLabel:
    ts = datetime(2026, 3, 14, 12, 5, 2, tzinfo=UTC)
    return SnapshotLabel(
        snapshot_id=snapshot_id,
        window_id="btc-5m-20260314T120500Z",
        polymarket_market_id=market_id,
        snapshot_ts=ts,
        resolved_up=resolved_up,
        chainlink_settle_price=Decimal("84025") if label_status == "attached" else None,
        chainlink_settle_ts=datetime(2026, 3, 14, 12, 10, 0, tzinfo=UTC)
        if label_status == "attached"
        else None,
        settle_minus_open=Decimal("25") if label_status == "attached" else None,
        realized_direction=(
            "up" if resolved_up is True else "down" if resolved_up is False else "unknown"
        ),
        label_status=label_status,
        label_quality_flags=() if label_status == "attached" else ("missing_settlement",),
    )


def _labeled_snapshot(
    *,
    up_ask: str = "0.54",
    down_ask: str = "0.46",
    resolved_up: bool | None = True,
    label_status: str = "attached",
) -> LabeledSnapshotRecord:
    snapshot = _snapshot(up_ask=up_ask, down_ask=down_ask)
    return LabeledSnapshotRecord(
        snapshot=snapshot,
        label=_label(
            snapshot.snapshot_id or "",
            resolved_up=resolved_up,
            label_status=label_status,
        ),
    )


def _edge(
    *,
    preferred_side: str | None,
    edge_up_net: str | None,
    edge_down_net: str | None,
    slippage_up: str = "0.002",
    slippage_down: str = "0.002",
) -> ExecutableEdgeEstimate:
    return ExecutableEdgeEstimate(
        fair_value_base=Decimal("0.60"),
        edge_up_raw=None if edge_up_net is None else Decimal(edge_up_net) + Decimal("0.006"),
        edge_down_raw=None
        if edge_down_net is None
        else Decimal(edge_down_net) + Decimal("0.006"),
        edge_up_net=None if edge_up_net is None else Decimal(edge_up_net),
        edge_down_net=None if edge_down_net is None else Decimal(edge_down_net),
        preferred_side=preferred_side,
        no_trade_reason=None if preferred_side is not None else "non_positive_net_edge",
        fee_rate_estimate=Decimal("0.001"),
        slippage_estimate_up=Decimal(slippage_up),
        slippage_estimate_down=Decimal(slippage_down),
        model_error_buffer=Decimal("0.003"),
        feature_version="0.1.0",
        diagnostics=(),
    )


def test_simulate_snapshot_executes_profitable_buy_up_trade() -> None:
    labeled_snapshot = _labeled_snapshot(up_ask="0.54", down_ask="0.46")
    edge = _edge(preferred_side="up", edge_up_net="0.074", edge_down_net="-0.086")

    trade = simulate_snapshot(
        ReplaySimulationInput(labeled_snapshot=labeled_snapshot, executable_edge=edge),
        fee_curve=FeeCurvePolicy(taker_fee_rate=Decimal("0.001")),
        entry_rules=EntryRulePolicy(target_trade_size_contracts=Decimal("1")),
    )

    assert trade.sim_trade_direction == SIM_BUY_UP
    assert trade.sim_entry_price == Decimal("0.54")
    assert trade.sim_exit_price == Decimal("1")
    assert trade.sim_slippage_paid == Decimal("0.002")
    assert trade.sim_fee_paid == Decimal("0.00054")
    assert trade.sim_pnl == Decimal("0.45746")
    assert trade.predicted_edge_net == Decimal("0.074")
    assert trade.realized_edge == Decimal("0.45746")
    assert trade.sim_outcome == SIM_OUTCOME_WIN


def test_simulate_snapshot_executes_profitable_buy_down_trade() -> None:
    labeled_snapshot = _labeled_snapshot(
        up_ask="0.36",
        down_ask="0.64",
        resolved_up=False,
    )
    edge = _edge(preferred_side="down", edge_up_net="-0.066", edge_down_net="0.054")

    trade = simulate_snapshot(
        ReplaySimulationInput(labeled_snapshot=labeled_snapshot, executable_edge=edge),
        fee_curve=FeeCurvePolicy(taker_fee_rate=Decimal("0.001")),
    )

    assert trade.sim_trade_direction == SIM_BUY_DOWN
    assert trade.sim_exit_price == Decimal("1")
    assert trade.sim_outcome == SIM_OUTCOME_WIN


def test_simulate_snapshot_returns_no_trade_for_unresolved_label() -> None:
    labeled_snapshot = _labeled_snapshot(
        resolved_up=None,
        label_status="unresolved",
    )
    edge = compute_executable_edge(
        fair_value_base=None,
        polymarket_quote=None,
    )

    trade = simulate_snapshot(
        ReplaySimulationInput(labeled_snapshot=labeled_snapshot, executable_edge=edge)
    )

    assert trade.sim_trade_direction == SIM_NO_TRADE
    assert trade.no_trade_reason == "label_unresolved"
    assert trade.sim_outcome == SIM_OUTCOME_NO_TRADE


def test_simulate_replay_aggregates_hit_rate_and_realized_vs_predicted_edge() -> None:
    first = simulate_snapshot(
        ReplaySimulationInput(
            labeled_snapshot=_labeled_snapshot(),
            executable_edge=_edge(
                preferred_side="up",
                edge_up_net="0.074",
                edge_down_net="-0.086",
            ),
        ),
        fee_curve=FeeCurvePolicy(taker_fee_rate=Decimal("0.001")),
    )
    second = simulate_snapshot(
        ReplaySimulationInput(
            labeled_snapshot=_labeled_snapshot(
                up_ask="0.70",
                down_ask="0.30",
                resolved_up=False,
            ),
            executable_edge=_edge(
                preferred_side="up",
                edge_up_net="0.01",
                edge_down_net="-0.03",
            ),
        ),
        fee_curve=FeeCurvePolicy(taker_fee_rate=Decimal("0.001")),
    )
    result = simulate_replay(
        [
            ReplaySimulationInput(
                labeled_snapshot=_labeled_snapshot(),
                executable_edge=_edge(
                    preferred_side="up",
                    edge_up_net="0.074",
                    edge_down_net="-0.086",
                ),
            ),
            ReplaySimulationInput(
                labeled_snapshot=_labeled_snapshot(
                    up_ask="0.70",
                    down_ask="0.30",
                    resolved_up=False,
                ),
                executable_edge=_edge(
                    preferred_side="up",
                    edge_up_net="0.01",
                    edge_down_net="-0.03",
                ),
            ),
            ReplaySimulationInput(
                labeled_snapshot=_labeled_snapshot(
                    label_status="unresolved",
                    resolved_up=None,
                ),
                executable_edge=compute_executable_edge(
                    fair_value_base=None,
                    polymarket_quote=None,
                ),
            ),
        ],
        fee_curve=FeeCurvePolicy(taker_fee_rate=Decimal("0.001")),
    )

    assert len(result.trades) == 3
    assert result.summary.trade_count == 2
    assert result.summary.hit_rate == Decimal("0.5")
    assert result.summary.total_pnl == first.sim_pnl + second.sim_pnl
    assert result.summary.average_predicted_edge == Decimal("0.042")
    assert result.summary.average_realized_edge == (
        first.realized_edge + second.realized_edge
    ) / Decimal("2")
