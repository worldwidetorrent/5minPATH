from datetime import UTC, datetime
from decimal import Decimal

from rtds.features.executable_edge import ExecutableEdgeEstimate
from rtds.replay.attach_labels import LabeledSnapshotRecord, SnapshotLabel
from rtds.replay.simulate import SimulatedTrade
from rtds.replay.slices import (
    CHAINLINK_CONFIDENCE_DIMENSION,
    COMPOSITE_QUALITY_DIMENSION,
    NET_EDGE_DIMENSION,
    RAW_EDGE_DIMENSION,
    SECONDS_REMAINING_DIMENSION,
    SIGNAL_DIRECTION_DIMENSION,
    SPREAD_DIMENSION,
    VOLATILITY_DIMENSION,
    ReplaySliceInput,
    ReplaySlicePolicy,
    generate_replay_slices,
)
from rtds.schemas.snapshot import SnapshotRecord


def _snapshot(
    *,
    market_id: str = "0xabc123",
    snapshot_usable_flag: bool = True,
    exchange_quality_usable_flag: bool = True,
    chainlink_quality_usable_flag: bool = True,
    reference_complete_flag: bool = True,
    spread_up: str = "0.01",
    spread_down: str = "0.01",
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
        up_ask=Decimal("0.54"),
        down_bid=Decimal("0.44"),
        down_ask=Decimal("0.46"),
        up_bid_size_contracts=Decimal("100"),
        up_ask_size_contracts=Decimal("100"),
        down_bid_size_contracts=Decimal("100"),
        down_ask_size_contracts=Decimal("100"),
        market_mid_up=Decimal("0.53"),
        market_mid_down=Decimal("0.45"),
        market_spread_up_abs=Decimal(spread_up),
        market_spread_down_abs=Decimal(spread_down),
        last_trade_price=Decimal("0.53"),
        last_trade_size_contracts=Decimal("10"),
        exchange_quality_usable_flag=exchange_quality_usable_flag,
        chainlink_quality_usable_flag=chainlink_quality_usable_flag,
        polymarket_quote_usable_flag=True,
        reference_complete_flag=reference_complete_flag,
        snapshot_usable_flag=snapshot_usable_flag,
        quality_diagnostics=(),
        schema_version="0.1.0",
        feature_version="0.1.0",
        created_ts=ts,
        snapshot_id=None,
    )


def _label(
    snapshot_id: str,
    *,
    resolved_up: bool | None,
    label_status: str = "attached",
) -> SnapshotLabel:
    ts = datetime(2026, 3, 14, 12, 5, 2, tzinfo=UTC)
    return SnapshotLabel(
        snapshot_id=snapshot_id,
        window_id="btc-5m-20260314T120500Z",
        polymarket_market_id="0xabc123",
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


def _edge(
    *,
    preferred_side: str | None,
    edge_up_raw: str | None,
    edge_down_raw: str | None,
    edge_up_net: str | None,
    edge_down_net: str | None,
) -> ExecutableEdgeEstimate:
    return ExecutableEdgeEstimate(
        fair_value_base=Decimal("0.60"),
        edge_up_raw=None if edge_up_raw is None else Decimal(edge_up_raw),
        edge_down_raw=None if edge_down_raw is None else Decimal(edge_down_raw),
        edge_up_net=None if edge_up_net is None else Decimal(edge_up_net),
        edge_down_net=None if edge_down_net is None else Decimal(edge_down_net),
        preferred_side=preferred_side,
        no_trade_reason=None if preferred_side is not None else "non_positive_net_edge",
        fee_rate_estimate=Decimal("0.001"),
        slippage_estimate_up=Decimal("0.002"),
        slippage_estimate_down=Decimal("0.002"),
        model_error_buffer=Decimal("0.003"),
        feature_version="0.1.0",
        diagnostics=(),
    )


def _trade(
    snapshot_id: str,
    *,
    direction: str,
    pnl: str,
    roi: str,
    predicted: str | None,
    realized: str | None,
) -> SimulatedTrade:
    return SimulatedTrade(
        snapshot_id=snapshot_id,
        window_id="btc-5m-20260314T120500Z",
        polymarket_market_id="0xabc123",
        sim_trade_direction=direction,
        sim_entry_price=Decimal("0.54") if direction != "no_trade" else None,
        sim_exit_price=Decimal("1") if direction != "no_trade" else None,
        sim_fee_paid=Decimal("0.001") if direction != "no_trade" else None,
        sim_slippage_paid=Decimal("0.002") if direction != "no_trade" else None,
        sim_pnl=Decimal(pnl),
        sim_roi=Decimal(roi),
        sim_outcome=(
            "win"
            if Decimal(pnl) > 0
            else "loss" if direction != "no_trade" else "no_trade"
        ),
        predicted_edge_net=None if predicted is None else Decimal(predicted),
        realized_edge=None if realized is None else Decimal(realized),
        no_trade_reason=None if direction != "no_trade" else "non_positive_net_edge",
        simulation_version="0.1.0",
    )


def _slice_input(
    *,
    seconds_remaining: int,
    sigma_eff: str | None,
    snapshot_usable_flag: bool,
    exchange_quality_usable_flag: bool,
    chainlink_quality_usable_flag: bool,
    reference_complete_flag: bool,
    spread_up: str,
    spread_down: str,
    preferred_side: str | None,
    direction: str,
    resolved_up: bool | None,
    label_status: str,
    edge_up_raw: str | None,
    edge_down_raw: str | None,
    edge_up_net: str | None,
    edge_down_net: str | None,
    pnl: str,
    roi: str,
    predicted: str | None,
    realized: str | None,
    composite_quality_state: str | None = None,
    chainlink_confidence_state: str | None = None,
) -> ReplaySliceInput:
    snapshot = _snapshot(
        snapshot_usable_flag=snapshot_usable_flag,
        exchange_quality_usable_flag=exchange_quality_usable_flag,
        chainlink_quality_usable_flag=chainlink_quality_usable_flag,
        reference_complete_flag=reference_complete_flag,
        spread_up=spread_up,
        spread_down=spread_down,
    )
    label = _label(snapshot.snapshot_id or "", resolved_up=resolved_up, label_status=label_status)
    return ReplaySliceInput(
        labeled_snapshot=LabeledSnapshotRecord(snapshot=snapshot, label=label),
        executable_edge=_edge(
            preferred_side=preferred_side,
            edge_up_raw=edge_up_raw,
            edge_down_raw=edge_down_raw,
            edge_up_net=edge_up_net,
            edge_down_net=edge_down_net,
        ),
        simulated_trade=_trade(
            snapshot.snapshot_id or "",
            direction=direction,
            pnl=pnl,
            roi=roi,
            predicted=predicted,
            realized=realized,
        ),
        seconds_remaining=seconds_remaining,
        sigma_eff=None if sigma_eff is None else Decimal(sigma_eff),
        composite_quality_state=composite_quality_state,
        chainlink_confidence_state=chainlink_confidence_state,
    )


def test_generate_replay_slices_groups_core_dimensions() -> None:
    inputs = [
        _slice_input(
            seconds_remaining=240,
            sigma_eff="0.00004",
            snapshot_usable_flag=True,
            exchange_quality_usable_flag=True,
            chainlink_quality_usable_flag=True,
            reference_complete_flag=True,
            spread_up="0.008",
            spread_down="0.012",
            preferred_side="up",
            direction="buy_up",
            resolved_up=True,
            label_status="attached",
            edge_up_raw="0.05",
            edge_down_raw="-0.03",
            edge_up_net="0.04",
            edge_down_net="-0.04",
            pnl="0.20",
            roi="0.30",
            predicted="0.04",
            realized="0.20",
            chainlink_confidence_state="high",
        ),
        _slice_input(
            seconds_remaining=120,
            sigma_eff="0.00008",
            snapshot_usable_flag=False,
            exchange_quality_usable_flag=True,
            chainlink_quality_usable_flag=True,
            reference_complete_flag=True,
            spread_up="0.02",
            spread_down="0.02",
            preferred_side="down",
            direction="buy_down",
            resolved_up=True,
            label_status="attached",
            edge_up_raw="-0.01",
            edge_down_raw="0.02",
            edge_up_net="-0.02",
            edge_down_net="0.015",
            pnl="-0.10",
            roi="-0.15",
            predicted="0.015",
            realized="-0.10",
            composite_quality_state="yellow",
            chainlink_confidence_state="medium",
        ),
        _slice_input(
            seconds_remaining=30,
            sigma_eff="0.00020",
            snapshot_usable_flag=False,
            exchange_quality_usable_flag=False,
            chainlink_quality_usable_flag=False,
            reference_complete_flag=False,
            spread_up="0.05",
            spread_down="0.05",
            preferred_side=None,
            direction="no_trade",
            resolved_up=None,
            label_status="unresolved",
            edge_up_raw="-0.01",
            edge_down_raw="-0.02",
            edge_up_net="-0.03",
            edge_down_net="-0.04",
            pnl="0",
            roi="0",
            predicted=None,
            realized=None,
        ),
    ]

    report = generate_replay_slices(inputs, policy=ReplaySlicePolicy())

    seconds_results = {
        result.slice_key: result
        for result in report.by_dimension[SECONDS_REMAINING_DIMENSION]
    }
    assert seconds_results["early_window"].trade_count == 1
    assert seconds_results["mid_window"].hit_rate == Decimal("0")
    assert seconds_results["late_window"].no_trade_count == 1

    volatility_results = {
        result.slice_key: result
        for result in report.by_dimension[VOLATILITY_DIMENSION]
    }
    assert set(volatility_results) == {"high_vol", "low_vol", "mid_vol"}

    quality_results = {
        result.slice_key: result
        for result in report.by_dimension[COMPOSITE_QUALITY_DIMENSION]
    }
    assert quality_results["green"].row_count == 1
    assert quality_results["yellow"].row_count == 1
    assert quality_results["red"].row_count == 1

    confidence_results = {
        result.slice_key: result for result in report.by_dimension[CHAINLINK_CONFIDENCE_DIMENSION]
    }
    assert confidence_results["high"].row_count == 1
    assert confidence_results["medium"].row_count == 1
    assert confidence_results["none"].row_count == 1

    signal_results = {
        result.slice_key: result
        for result in report.by_dimension[SIGNAL_DIRECTION_DIMENSION]
    }
    assert signal_results["buy_up"].hit_rate == Decimal("1")
    assert signal_results["buy_down"].hit_rate == Decimal("0")
    assert signal_results["none"].trade_count == 0


def test_generate_replay_slices_aggregates_edge_and_spread_buckets() -> None:
    inputs = [
        _slice_input(
            seconds_remaining=240,
            sigma_eff="0.00004",
            snapshot_usable_flag=True,
            exchange_quality_usable_flag=True,
            chainlink_quality_usable_flag=True,
            reference_complete_flag=True,
            spread_up="0.008",
            spread_down="0.012",
            preferred_side="up",
            direction="buy_up",
            resolved_up=True,
            label_status="attached",
            edge_up_raw="0.05",
            edge_down_raw="-0.03",
            edge_up_net="0.04",
            edge_down_net="-0.04",
            pnl="0.20",
            roi="0.30",
            predicted="0.04",
            realized="0.20",
        ),
        _slice_input(
            seconds_remaining=120,
            sigma_eff="0.00008",
            snapshot_usable_flag=False,
            exchange_quality_usable_flag=True,
            chainlink_quality_usable_flag=True,
            reference_complete_flag=True,
            spread_up="0.02",
            spread_down="0.02",
            preferred_side="down",
            direction="buy_down",
            resolved_up=True,
            label_status="attached",
            edge_up_raw="-0.01",
            edge_down_raw="0.02",
            edge_up_net="-0.02",
            edge_down_net="0.015",
            pnl="-0.10",
            roi="-0.15",
            predicted="0.015",
            realized="-0.10",
        ),
    ]

    report = generate_replay_slices(inputs, policy=ReplaySlicePolicy())

    raw_edge_results = {
        result.slice_key: result for result in report.by_dimension[RAW_EDGE_DIMENSION]
    }
    net_edge_results = {
        result.slice_key: result for result in report.by_dimension[NET_EDGE_DIMENSION]
    }
    spread_results = {
        result.slice_key: result for result in report.by_dimension[SPREAD_DIMENSION]
    }

    assert raw_edge_results["large_positive_edge"].row_count == 1
    assert raw_edge_results["medium_positive_edge"].row_count == 1
    assert net_edge_results["large_positive_edge"].average_realized_edge == Decimal("0.20")
    assert net_edge_results["medium_positive_edge"].average_predicted_edge == Decimal("0.015")
    assert spread_results["tight_spread"].trade_count == 1
    assert spread_results["medium_spread"].trade_count == 1
