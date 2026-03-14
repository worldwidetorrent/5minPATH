"""Executable edge calculations."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from rtds.core.units import to_decimal, validate_contract_price
from rtds.schemas.normalized import PolymarketQuote

FEATURE_VERSION = "0.1.0"

NO_TRADE_MISSING_FAIR_VALUE = "missing_fair_value"
NO_TRADE_MISSING_BOOK = "missing_book"
NO_TRADE_NON_POSITIVE_EDGE = "non_positive_net_edge"
NO_TRADE_TIED_EDGE = "tied_positive_net_edge"


@dataclass(slots=True, frozen=True)
class EdgeCostPolicy:
    """Explicit costs subtracted from raw executable edge."""

    fee_rate_estimate: Decimal = Decimal("0")
    slippage_estimate_up: Decimal = Decimal("0.001")
    slippage_estimate_down: Decimal = Decimal("0.001")
    model_error_buffer: Decimal = Decimal("0.0015")

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fee_rate_estimate",
            validate_contract_price(self.fee_rate_estimate, field_name="fee_rate_estimate"),
        )
        object.__setattr__(
            self,
            "slippage_estimate_up",
            validate_contract_price(
                self.slippage_estimate_up,
                field_name="slippage_estimate_up",
            ),
        )
        object.__setattr__(
            self,
            "slippage_estimate_down",
            validate_contract_price(
                self.slippage_estimate_down,
                field_name="slippage_estimate_down",
            ),
        )
        object.__setattr__(
            self,
            "model_error_buffer",
            validate_contract_price(
                self.model_error_buffer,
                field_name="model_error_buffer",
            ),
        )

    @classmethod
    def from_bps(
        cls,
        *,
        taker_fee_bps: int = 0,
        slippage_up_bps: int = 10,
        slippage_down_bps: int = 10,
        model_uncertainty_bps: int = 15,
    ) -> "EdgeCostPolicy":
        """Build a cost policy from basis-point config values."""

        return cls(
            fee_rate_estimate=Decimal(taker_fee_bps) / Decimal("10000"),
            slippage_estimate_up=Decimal(slippage_up_bps) / Decimal("10000"),
            slippage_estimate_down=Decimal(slippage_down_bps) / Decimal("10000"),
            model_error_buffer=Decimal(model_uncertainty_bps) / Decimal("10000"),
        )


DEFAULT_EDGE_COST_POLICY = EdgeCostPolicy()


@dataclass(slots=True, frozen=True)
class ExecutableEdgeEstimate:
    """Raw and net executable edge for both contract sides."""

    fair_value_base: Decimal | None
    edge_up_raw: Decimal | None
    edge_down_raw: Decimal | None
    edge_up_net: Decimal | None
    edge_down_net: Decimal | None
    preferred_side: str | None
    no_trade_reason: str | None
    fee_rate_estimate: Decimal
    slippage_estimate_up: Decimal
    slippage_estimate_down: Decimal
    model_error_buffer: Decimal
    feature_version: str
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.fair_value_base is not None:
            object.__setattr__(
                self,
                "fair_value_base",
                validate_contract_price(self.fair_value_base, field_name="fair_value_base"),
            )
        for field_name in ("edge_up_raw", "edge_down_raw", "edge_up_net", "edge_down_net"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, to_decimal(value, field_name=field_name))
        for field_name in (
            "fee_rate_estimate",
            "slippage_estimate_up",
            "slippage_estimate_down",
            "model_error_buffer",
        ):
            object.__setattr__(
                self,
                field_name,
                validate_contract_price(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


def compute_executable_edge(
    *,
    fair_value_base: Decimal | str | int | float | None,
    polymarket_quote: PolymarketQuote | None,
    cost_policy: EdgeCostPolicy = DEFAULT_EDGE_COST_POLICY,
) -> ExecutableEdgeEstimate:
    """Compute raw and net executable edge for Up and Down."""

    diagnostics: list[str] = []
    normalized_fair_value = (
        None
        if fair_value_base is None
        else validate_contract_price(fair_value_base, field_name="fair_value_base")
    )

    if normalized_fair_value is None:
        diagnostics.append(NO_TRADE_MISSING_FAIR_VALUE)
    if polymarket_quote is None:
        diagnostics.append(NO_TRADE_MISSING_BOOK)

    if normalized_fair_value is None or polymarket_quote is None:
        return ExecutableEdgeEstimate(
            fair_value_base=normalized_fair_value,
            edge_up_raw=None,
            edge_down_raw=None,
            edge_up_net=None,
            edge_down_net=None,
            preferred_side=None,
            no_trade_reason=diagnostics[0] if diagnostics else None,
            fee_rate_estimate=cost_policy.fee_rate_estimate,
            slippage_estimate_up=cost_policy.slippage_estimate_up,
            slippage_estimate_down=cost_policy.slippage_estimate_down,
            model_error_buffer=cost_policy.model_error_buffer,
            feature_version=FEATURE_VERSION,
            diagnostics=tuple(diagnostics),
        )

    edge_up_raw = normalized_fair_value - polymarket_quote.up_ask
    edge_down_raw = (Decimal("1") - normalized_fair_value) - polymarket_quote.down_ask
    edge_up_net = (
        edge_up_raw
        - cost_policy.fee_rate_estimate
        - cost_policy.slippage_estimate_up
        - cost_policy.model_error_buffer
    )
    edge_down_net = (
        edge_down_raw
        - cost_policy.fee_rate_estimate
        - cost_policy.slippage_estimate_down
        - cost_policy.model_error_buffer
    )

    preferred_side: str | None = None
    no_trade_reason: str | None = None
    if edge_up_net <= 0 and edge_down_net <= 0:
        no_trade_reason = NO_TRADE_NON_POSITIVE_EDGE
        diagnostics.append(NO_TRADE_NON_POSITIVE_EDGE)
    elif edge_up_net > edge_down_net:
        preferred_side = "up"
    elif edge_down_net > edge_up_net:
        preferred_side = "down"
    else:
        no_trade_reason = NO_TRADE_TIED_EDGE
        diagnostics.append(NO_TRADE_TIED_EDGE)

    return ExecutableEdgeEstimate(
        fair_value_base=normalized_fair_value,
        edge_up_raw=edge_up_raw,
        edge_down_raw=edge_down_raw,
        edge_up_net=edge_up_net,
        edge_down_net=edge_down_net,
        preferred_side=preferred_side,
        no_trade_reason=no_trade_reason,
        fee_rate_estimate=cost_policy.fee_rate_estimate,
        slippage_estimate_up=cost_policy.slippage_estimate_up,
        slippage_estimate_down=cost_policy.slippage_estimate_down,
        model_error_buffer=cost_policy.model_error_buffer,
        feature_version=FEATURE_VERSION,
        diagnostics=tuple(diagnostics),
    )


__all__ = [
    "DEFAULT_EDGE_COST_POLICY",
    "FEATURE_VERSION",
    "NO_TRADE_MISSING_BOOK",
    "NO_TRADE_MISSING_FAIR_VALUE",
    "NO_TRADE_NON_POSITIVE_EDGE",
    "NO_TRADE_TIED_EDGE",
    "EdgeCostPolicy",
    "ExecutableEdgeEstimate",
    "compute_executable_edge",
]
