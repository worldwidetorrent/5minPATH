from datetime import UTC, datetime
from decimal import Decimal

from rtds.features.executable_edge import (
    EdgeCostPolicy,
    compute_executable_edge,
)
from rtds.schemas.normalized import SCHEMA_VERSION, PolymarketQuote


def _quote(
    *,
    up_ask: str,
    down_ask: str,
) -> PolymarketQuote:
    event_ts = datetime(2026, 3, 14, 12, 5, 1, tzinfo=UTC)
    up_ask_decimal = Decimal(up_ask)
    down_ask_decimal = Decimal(down_ask)
    return PolymarketQuote(
        venue_id="polymarket",
        market_id="0xabc123",
        asset_id="BTC",
        event_ts=event_ts,
        recv_ts=event_ts,
        proc_ts=event_ts,
        up_bid=max(Decimal("0"), up_ask_decimal - Decimal("0.02")),
        up_ask=up_ask_decimal,
        down_bid=max(Decimal("0"), down_ask_decimal - Decimal("0.02")),
        down_ask=down_ask_decimal,
        up_bid_size_contracts=Decimal("100"),
        up_ask_size_contracts=Decimal("100"),
        down_bid_size_contracts=Decimal("100"),
        down_ask_size_contracts=Decimal("100"),
        raw_event_id="poly-edge-1",
        normalizer_version="0.1.0",
        schema_version=SCHEMA_VERSION,
        created_ts=event_ts,
    )


def test_compute_executable_edge_prefers_up_when_up_net_is_best() -> None:
    estimate = compute_executable_edge(
        fair_value_base=Decimal("0.60"),
        polymarket_quote=_quote(up_ask="0.54", down_ask="0.46"),
        cost_policy=EdgeCostPolicy(
            fee_rate_estimate=Decimal("0.001"),
            slippage_estimate_up=Decimal("0.002"),
            slippage_estimate_down=Decimal("0.002"),
            model_error_buffer=Decimal("0.003"),
        ),
    )

    assert estimate.edge_up_raw == Decimal("0.06")
    assert estimate.edge_down_raw == Decimal("-0.06")
    assert estimate.edge_up_net == Decimal("0.054")
    assert estimate.edge_down_net == Decimal("-0.066")
    assert estimate.preferred_side == "up"
    assert estimate.no_trade_reason is None


def test_compute_executable_edge_prefers_down_when_down_net_is_best() -> None:
    estimate = compute_executable_edge(
        fair_value_base=Decimal("0.30"),
        polymarket_quote=_quote(up_ask="0.36", down_ask="0.64"),
        cost_policy=EdgeCostPolicy(
            fee_rate_estimate=Decimal("0.001"),
            slippage_estimate_up=Decimal("0.002"),
            slippage_estimate_down=Decimal("0.002"),
            model_error_buffer=Decimal("0.003"),
        ),
    )

    assert estimate.edge_up_raw == Decimal("-0.06")
    assert estimate.edge_down_raw == Decimal("0.06")
    assert estimate.edge_up_net == Decimal("-0.066")
    assert estimate.edge_down_net == Decimal("0.054")
    assert estimate.preferred_side == "down"
    assert estimate.no_trade_reason is None


def test_compute_executable_edge_returns_no_trade_when_both_net_edges_are_non_positive() -> None:
    estimate = compute_executable_edge(
        fair_value_base=Decimal("0.55"),
        polymarket_quote=_quote(up_ask="0.56", down_ask="0.45"),
        cost_policy=EdgeCostPolicy(
            fee_rate_estimate=Decimal("0"),
            slippage_estimate_up=Decimal("0.001"),
            slippage_estimate_down=Decimal("0.001"),
            model_error_buffer=Decimal("0.001"),
        ),
    )

    assert estimate.edge_up_raw == Decimal("-0.01")
    assert estimate.edge_down_raw == Decimal("0.00")
    assert estimate.edge_up_net == Decimal("-0.012")
    assert estimate.edge_down_net == Decimal("-0.002")
    assert estimate.preferred_side is None
    assert estimate.no_trade_reason == "non_positive_net_edge"
    assert estimate.diagnostics == ("non_positive_net_edge",)


def test_compute_executable_edge_handles_missing_inputs() -> None:
    estimate = compute_executable_edge(
        fair_value_base=None,
        polymarket_quote=None,
    )

    assert estimate.edge_up_raw is None
    assert estimate.edge_down_net is None
    assert estimate.preferred_side is None
    assert estimate.no_trade_reason == "missing_fair_value"
    assert estimate.diagnostics == ("missing_book", "missing_fair_value")
