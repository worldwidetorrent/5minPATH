"""Volatility features."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal, validate_usd_price, validate_volatility
from rtds.features.composite_nowcast import CompositeNowcast

FEATURE_VERSION = "0.1.0"

DEFAULT_FAST_RETURN_COUNT = 20
DEFAULT_BASELINE_RETURN_COUNT = 120
DEFAULT_FAST_WEIGHT = Decimal("0.65")
DEFAULT_SIGMA_FLOOR = Decimal("0.00001")
DEFAULT_SIGMA_CAP = Decimal("0.01")


@dataclass(slots=True, frozen=True)
class CompositePriceObservation:
    """One composite-price observation for volatility estimation."""

    event_ts: datetime
    price: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_ts", ensure_utc(self.event_ts, field_name="event_ts"))
        object.__setattr__(
            self,
            "price",
            Decimal(validate_usd_price(self.price, field_name="price")),
        )


@dataclass(slots=True, frozen=True)
class VolatilityPolicy:
    """Explicit phase-1 volatility policy."""

    fast_return_count: int = DEFAULT_FAST_RETURN_COUNT
    baseline_return_count: int = DEFAULT_BASELINE_RETURN_COUNT
    fast_weight: Decimal = DEFAULT_FAST_WEIGHT
    sigma_floor: Decimal = DEFAULT_SIGMA_FLOOR
    sigma_cap: Decimal = DEFAULT_SIGMA_CAP

    def __post_init__(self) -> None:
        if self.fast_return_count <= 0:
            raise ValueError("fast_return_count must be positive")
        if self.baseline_return_count < self.fast_return_count:
            raise ValueError("baseline_return_count must be >= fast_return_count")

        fast_weight = to_decimal(self.fast_weight, field_name="fast_weight")
        if fast_weight < 0 or fast_weight > 1:
            raise ValueError("fast_weight must be in [0, 1]")

        sigma_floor = validate_volatility(self.sigma_floor, field_name="sigma_floor")
        sigma_cap = validate_volatility(self.sigma_cap, field_name="sigma_cap")
        if sigma_cap < sigma_floor:
            raise ValueError("sigma_cap must be >= sigma_floor")

        object.__setattr__(self, "fast_weight", fast_weight)
        object.__setattr__(self, "sigma_floor", sigma_floor)
        object.__setattr__(self, "sigma_cap", sigma_cap)


DEFAULT_VOLATILITY_POLICY = VolatilityPolicy()


@dataclass(slots=True, frozen=True)
class VolatilityEstimate:
    """Deterministic volatility state for one snapshot timestamp."""

    as_of_ts: datetime
    sigma_fast: Decimal
    sigma_baseline: Decimal
    sigma_eff: Decimal
    fast_return_count: int
    baseline_return_count: int
    feature_version: str
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_ts", ensure_utc(self.as_of_ts, field_name="as_of_ts"))
        for field_name in ("sigma_fast", "sigma_baseline", "sigma_eff"):
            object.__setattr__(
                self,
                field_name,
                validate_volatility(getattr(self, field_name), field_name=field_name),
            )
        if self.fast_return_count < 0:
            raise ValueError("fast_return_count must be non-negative")
        if self.baseline_return_count < 0:
            raise ValueError("baseline_return_count must be non-negative")
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


def compute_volatility_estimate(
    observations: Iterable[CompositePriceObservation],
    *,
    as_of_ts: datetime | None = None,
    policy: VolatilityPolicy = DEFAULT_VOLATILITY_POLICY,
) -> VolatilityEstimate:
    """Compute `sigma_fast`, `sigma_baseline`, and blended `sigma_eff`."""

    normalized_observations = _normalize_observations(observations, as_of_ts=as_of_ts)
    effective_as_of_ts = (
        ensure_utc(as_of_ts, field_name="as_of_ts")
        if as_of_ts is not None
        else normalized_observations[-1].event_ts
    )

    returns = _compute_returns(normalized_observations)
    diagnostics: list[str] = []

    if not returns:
        diagnostics.append("no_returns_available")
        sigma_fast = policy.sigma_floor
        sigma_baseline = policy.sigma_floor
        sigma_eff = policy.sigma_floor
        fast_count = 0
        baseline_count = 0
    else:
        fast_window = returns[-policy.fast_return_count :]
        baseline_window = returns[-policy.baseline_return_count :]

        if len(fast_window) < policy.fast_return_count:
            diagnostics.append("sigma_fast_window_truncated")
        if len(baseline_window) < policy.baseline_return_count:
            diagnostics.append("sigma_baseline_window_truncated")

        sigma_fast = _clamp_sigma(_realized_volatility(fast_window), policy=policy)
        sigma_baseline = _clamp_sigma(_realized_volatility(baseline_window), policy=policy)
        sigma_eff = _clamp_sigma(
            (policy.fast_weight * sigma_fast)
            + ((Decimal("1") - policy.fast_weight) * sigma_baseline),
            policy=policy,
        )
        fast_count = len(fast_window)
        baseline_count = len(baseline_window)

    return VolatilityEstimate(
        as_of_ts=effective_as_of_ts,
        sigma_fast=sigma_fast,
        sigma_baseline=sigma_baseline,
        sigma_eff=sigma_eff,
        fast_return_count=fast_count,
        baseline_return_count=baseline_count,
        feature_version=FEATURE_VERSION,
        diagnostics=tuple(diagnostics),
    )


def compute_volatility_from_nowcasts(
    nowcasts: Iterable[CompositeNowcast],
    *,
    as_of_ts: datetime | None = None,
    policy: VolatilityPolicy = DEFAULT_VOLATILITY_POLICY,
) -> VolatilityEstimate:
    """Convenience wrapper for composite-nowcast histories."""

    return compute_volatility_estimate(
        observations_from_nowcasts(nowcasts, as_of_ts=as_of_ts),
        as_of_ts=as_of_ts,
        policy=policy,
    )


def observations_from_nowcasts(
    nowcasts: Iterable[CompositeNowcast],
    *,
    as_of_ts: datetime | None = None,
) -> list[CompositePriceObservation]:
    """Extract composite-price observations from nowcasts with non-null prices."""

    normalized_as_of_ts = (
        None if as_of_ts is None else ensure_utc(as_of_ts, field_name="as_of_ts")
    )
    observations: list[CompositePriceObservation] = []
    for nowcast in nowcasts:
        if nowcast.composite_now_price is None:
            continue
        if normalized_as_of_ts is not None and nowcast.as_of_ts > normalized_as_of_ts:
            continue
        observations.append(
            CompositePriceObservation(
                event_ts=nowcast.as_of_ts,
                price=nowcast.composite_now_price,
            )
        )
    return observations


def _normalize_observations(
    observations: Iterable[CompositePriceObservation],
    *,
    as_of_ts: datetime | None,
) -> list[CompositePriceObservation]:
    normalized_as_of_ts = (
        None if as_of_ts is None else ensure_utc(as_of_ts, field_name="as_of_ts")
    )
    deduped: dict[datetime, CompositePriceObservation] = {}
    for observation in observations:
        if normalized_as_of_ts is not None and observation.event_ts > normalized_as_of_ts:
            continue
        deduped[observation.event_ts] = observation

    normalized = sorted(deduped.values(), key=lambda observation: observation.event_ts)
    if not normalized:
        raise ValueError("at least one observation is required")
    return normalized


def _compute_returns(observations: list[CompositePriceObservation]) -> list[Decimal]:
    return [
        (current.price / previous.price) - Decimal("1")
        for previous, current in zip(observations, observations[1:], strict=False)
    ]


def _realized_volatility(returns: list[Decimal]) -> Decimal:
    if not returns:
        return Decimal("0")
    mean_squared_return = sum((value * value) for value in returns) / Decimal(len(returns))
    return mean_squared_return.sqrt()


def _clamp_sigma(value: Decimal, *, policy: VolatilityPolicy) -> Decimal:
    if value < policy.sigma_floor:
        return policy.sigma_floor
    if value > policy.sigma_cap:
        return policy.sigma_cap
    return value


__all__ = [
    "DEFAULT_BASELINE_RETURN_COUNT",
    "DEFAULT_FAST_RETURN_COUNT",
    "DEFAULT_FAST_WEIGHT",
    "DEFAULT_SIGMA_CAP",
    "DEFAULT_SIGMA_FLOOR",
    "DEFAULT_VOLATILITY_POLICY",
    "FEATURE_VERSION",
    "CompositePriceObservation",
    "VolatilityEstimate",
    "VolatilityPolicy",
    "compute_volatility_estimate",
    "compute_volatility_from_nowcasts",
    "observations_from_nowcasts",
]
