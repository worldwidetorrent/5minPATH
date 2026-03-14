"""Baseline fair-value features."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from math import sqrt
from statistics import NormalDist

from rtds.core.units import (
    to_decimal,
    validate_contract_price,
    validate_usd_price,
    validate_volatility,
)

FEATURE_VERSION = "0.1.0"
NORMAL_CDF = NormalDist()


@dataclass(slots=True, frozen=True)
class FairValueBaseEstimate:
    """Baseline oracle-anchored fair-value estimate for one snapshot."""

    chainlink_open_anchor_price: Decimal | None
    composite_now_price: Decimal | None
    seconds_remaining: int
    sigma_eff: Decimal | None
    log_move_from_open: Decimal | None
    abs_move_from_open: Decimal | None
    z_base: Decimal | None
    fair_value_base: Decimal | None
    denominator_sigma_horizon: Decimal | None
    feature_version: str
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.seconds_remaining < 0:
            raise ValueError("seconds_remaining must be non-negative")
        for field_name in (
            "chainlink_open_anchor_price",
            "composite_now_price",
            "log_move_from_open",
            "abs_move_from_open",
            "z_base",
            "denominator_sigma_horizon",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, to_decimal(value, field_name=field_name))
        if self.chainlink_open_anchor_price is not None:
            object.__setattr__(
                self,
                "chainlink_open_anchor_price",
                Decimal(
                    validate_usd_price(
                        self.chainlink_open_anchor_price,
                        field_name="chainlink_open_anchor_price",
                    )
                ),
            )
        if self.composite_now_price is not None:
            object.__setattr__(
                self,
                "composite_now_price",
                Decimal(
                    validate_usd_price(
                        self.composite_now_price,
                        field_name="composite_now_price",
                    )
                ),
            )
        if self.sigma_eff is not None:
            object.__setattr__(
                self,
                "sigma_eff",
                validate_volatility(self.sigma_eff, field_name="sigma_eff"),
            )
        if self.fair_value_base is not None:
            object.__setattr__(
                self,
                "fair_value_base",
                validate_contract_price(self.fair_value_base, field_name="fair_value_base"),
            )
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


def compute_fair_value_base(
    *,
    chainlink_open_anchor_price: Decimal | str | int | float | None,
    composite_now_price: Decimal | str | int | float | None,
    seconds_remaining: int,
    sigma_eff: Decimal | str | int | float | None,
) -> FairValueBaseEstimate:
    """Compute the baseline fair value from oracle anchor, composite price, and volatility.

    Phase-1 policy is explicit:

    - `log_move_from_open = ln(composite_now_price / chainlink_open_anchor_price)`
    - `z_base = log_move_from_open / (sigma_eff * sqrt(seconds_remaining))`
    - `fair_value_base = Phi(z_base)`

    At `seconds_remaining == 0`, the estimate collapses to the realized sign of the move:

    - positive move -> `1`
    - negative move -> `0`
    - flat move -> `0.5`
    """

    diagnostics: list[str] = []
    if seconds_remaining < 0:
        raise ValueError("seconds_remaining must be non-negative")

    if chainlink_open_anchor_price is None:
        diagnostics.append("missing_open_anchor")
    if composite_now_price is None:
        diagnostics.append("missing_composite_now_price")
    if sigma_eff is None:
        diagnostics.append("missing_sigma_eff")

    if diagnostics:
        return FairValueBaseEstimate(
            chainlink_open_anchor_price=None
            if chainlink_open_anchor_price is None
            else Decimal(
                validate_usd_price(
                    chainlink_open_anchor_price,
                    field_name="chainlink_open_anchor_price",
                )
            ),
            composite_now_price=None
            if composite_now_price is None
            else Decimal(
                validate_usd_price(composite_now_price, field_name="composite_now_price")
            ),
            seconds_remaining=seconds_remaining,
            sigma_eff=None
            if sigma_eff is None
            else validate_volatility(sigma_eff, field_name="sigma_eff"),
            log_move_from_open=None,
            abs_move_from_open=None,
            z_base=None,
            fair_value_base=None,
            denominator_sigma_horizon=None,
            feature_version=FEATURE_VERSION,
            diagnostics=tuple(diagnostics),
        )

    open_price = Decimal(
        validate_usd_price(chainlink_open_anchor_price, field_name="chainlink_open_anchor_price")
    )
    composite_price = Decimal(
        validate_usd_price(composite_now_price, field_name="composite_now_price")
    )
    sigma = validate_volatility(sigma_eff, field_name="sigma_eff")

    abs_move_from_open = composite_price - open_price
    log_move_from_open = (composite_price / open_price).ln()

    if seconds_remaining == 0:
        diagnostics.append("expiry_boundary")
        if log_move_from_open > 0:
            fair_value = Decimal("1")
        elif log_move_from_open < 0:
            fair_value = Decimal("0")
        else:
            fair_value = Decimal("0.5")

        return FairValueBaseEstimate(
            chainlink_open_anchor_price=open_price,
            composite_now_price=composite_price,
            seconds_remaining=seconds_remaining,
            sigma_eff=sigma,
            log_move_from_open=log_move_from_open,
            abs_move_from_open=abs_move_from_open,
            z_base=None,
            fair_value_base=fair_value,
            denominator_sigma_horizon=Decimal("0"),
            feature_version=FEATURE_VERSION,
            diagnostics=tuple(diagnostics),
        )

    denominator = sigma * Decimal(str(sqrt(seconds_remaining)))
    if denominator <= 0:
        diagnostics.append("non_positive_sigma_horizon")
        return FairValueBaseEstimate(
            chainlink_open_anchor_price=open_price,
            composite_now_price=composite_price,
            seconds_remaining=seconds_remaining,
            sigma_eff=sigma,
            log_move_from_open=log_move_from_open,
            abs_move_from_open=abs_move_from_open,
            z_base=None,
            fair_value_base=None,
            denominator_sigma_horizon=denominator,
            feature_version=FEATURE_VERSION,
            diagnostics=tuple(diagnostics),
        )

    z_base = log_move_from_open / denominator
    fair_value_base = Decimal(str(NORMAL_CDF.cdf(float(z_base))))

    return FairValueBaseEstimate(
        chainlink_open_anchor_price=open_price,
        composite_now_price=composite_price,
        seconds_remaining=seconds_remaining,
        sigma_eff=sigma,
        log_move_from_open=log_move_from_open,
        abs_move_from_open=abs_move_from_open,
        z_base=z_base,
        fair_value_base=fair_value_base,
        denominator_sigma_horizon=denominator,
        feature_version=FEATURE_VERSION,
        diagnostics=tuple(diagnostics),
    )


__all__ = [
    "FEATURE_VERSION",
    "FairValueBaseEstimate",
    "compute_fair_value_base",
]
