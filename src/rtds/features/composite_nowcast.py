"""Composite nowcast features."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from statistics import median
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal
from rtds.quality.dispersion import (
    DEFAULT_OUTLIER_ABS_THRESHOLD_USD,
    DEFAULT_OUTLIER_BPS_THRESHOLD,
    DispersionPolicy,
    assess_exchange_composite_quality,
)
from rtds.quality.freshness import DEFAULT_FRESHNESS_POLICY, FreshnessPolicy
from rtds.schemas.normalized import ExchangeQuote

FEATURE_VERSION = "0.1.0"
DEFAULT_MINIMUM_VENUE_COUNT = 3
MISSING_COMPOSITE_METHOD = "missing"
TRIMMED_MEDIAN_METHOD = "trimmed_median"


@dataclass(slots=True, frozen=True)
class MedianFamilyAggregationPolicy:
    """Aggregation rules from ADR 0002."""

    trim_count_each_side: int = 1

    def __post_init__(self) -> None:
        if self.trim_count_each_side < 0:
            raise ValueError("trim_count_each_side must be non-negative")


DEFAULT_AGGREGATION_POLICY = MedianFamilyAggregationPolicy()


@dataclass(slots=True, frozen=True)
class CompositeNowcast:
    """Deterministic phase-1 exchange composite now-price state."""

    as_of_ts: datetime
    composite_now_price: Decimal | None
    composite_method: str
    feature_version: str
    composite_missing_flag: bool
    contributing_venue_count: int
    contributing_venues: tuple[str, ...]
    per_venue_mids: Mapping[str, Decimal | None]
    per_venue_ages: Mapping[str, int | None]
    dispersion_abs_usd: Decimal | None
    dispersion_bps: Decimal | None
    quality_score: Decimal
    outlier_venue_ids: tuple[str, ...]
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_ts", ensure_utc(self.as_of_ts, field_name="as_of_ts"))
        if self.composite_now_price is not None:
            object.__setattr__(
                self,
                "composite_now_price",
                to_decimal(self.composite_now_price, field_name="composite_now_price"),
            )
        if self.dispersion_abs_usd is not None:
            object.__setattr__(
                self,
                "dispersion_abs_usd",
                to_decimal(self.dispersion_abs_usd, field_name="dispersion_abs_usd"),
            )
        if self.dispersion_bps is not None:
            object.__setattr__(
                self,
                "dispersion_bps",
                to_decimal(self.dispersion_bps, field_name="dispersion_bps"),
            )
        quality_score = to_decimal(self.quality_score, field_name="quality_score")
        if quality_score < 0 or quality_score > 1:
            raise ValueError("quality_score must be in [0, 1]")
        object.__setattr__(self, "quality_score", quality_score)
        object.__setattr__(
            self,
            "contributing_venues",
            tuple(self.contributing_venues),
        )
        object.__setattr__(
            self,
            "outlier_venue_ids",
            tuple(sorted(set(self.outlier_venue_ids))),
        )
        object.__setattr__(
            self,
            "per_venue_mids",
            MappingProxyType(
                {
                    venue_id: (
                        None
                        if mid_price is None
                        else to_decimal(mid_price, field_name=f"per_venue_mids[{venue_id}]")
                    )
                    for venue_id, mid_price in self.per_venue_mids.items()
                }
            ),
        )
        object.__setattr__(
            self,
            "per_venue_ages",
            MappingProxyType(dict(self.per_venue_ages)),
        )
        object.__setattr__(self, "diagnostics", tuple(sorted(set(self.diagnostics))))


def compute_composite_nowcast(
    quotes: Iterable[ExchangeQuote],
    *,
    as_of_ts: datetime,
    freshness_policy: FreshnessPolicy = DEFAULT_FRESHNESS_POLICY,
    minimum_venue_count: int = DEFAULT_MINIMUM_VENUE_COUNT,
    aggregation_policy: MedianFamilyAggregationPolicy = DEFAULT_AGGREGATION_POLICY,
    outlier_abs_threshold_usd: Decimal = DEFAULT_OUTLIER_ABS_THRESHOLD_USD,
    outlier_bps_threshold: Decimal = DEFAULT_OUTLIER_BPS_THRESHOLD,
) -> CompositeNowcast:
    """Build the phase-1 composite now-price from normalized exchange quotes."""

    if minimum_venue_count <= 0:
        raise ValueError("minimum_venue_count must be positive")

    normalized_as_of_ts = ensure_utc(as_of_ts, field_name="as_of_ts")
    valid_quotes = _latest_valid_quotes_by_venue(quotes)
    quality_state = assess_exchange_composite_quality(
        valid_quotes.values(),
        as_of_ts=normalized_as_of_ts,
        freshness_policy=freshness_policy,
        dispersion_policy=DispersionPolicy(
            min_contributing_venues=minimum_venue_count,
            outlier_abs_threshold_usd=outlier_abs_threshold_usd,
            outlier_bps_threshold=outlier_bps_threshold,
        ),
    )

    diagnostics = list(quality_state.diagnostics)
    if quality_state.trusted_venue_count < minimum_venue_count:
        if "composite_missing" not in diagnostics:
            diagnostics.append("composite_missing")
        return CompositeNowcast(
            as_of_ts=normalized_as_of_ts,
            composite_now_price=None,
            composite_method=MISSING_COMPOSITE_METHOD,
            feature_version=FEATURE_VERSION,
            composite_missing_flag=True,
            contributing_venue_count=quality_state.trusted_venue_count,
            contributing_venues=quality_state.trusted_venue_ids,
            per_venue_mids=quality_state.per_venue_mid_price,
            per_venue_ages=quality_state.per_venue_age_ms,
            dispersion_abs_usd=quality_state.dispersion_abs_usd,
            dispersion_bps=quality_state.dispersion_bps,
            quality_score=Decimal("0"),
            outlier_venue_ids=quality_state.outlier_venue_ids,
            diagnostics=tuple(diagnostics),
        )

    trusted_mids = [
        quality_state.per_venue_mid_price[venue_id]
        for venue_id in quality_state.trusted_venue_ids
        if quality_state.per_venue_mid_price[venue_id] is not None
    ]
    composite_now_price, composite_method = aggregate_mid_prices(
        trusted_mids,
        policy=aggregation_policy,
    )

    return CompositeNowcast(
        as_of_ts=normalized_as_of_ts,
        composite_now_price=composite_now_price,
        composite_method=composite_method,
        feature_version=FEATURE_VERSION,
        composite_missing_flag=False,
        contributing_venue_count=quality_state.trusted_venue_count,
        contributing_venues=quality_state.trusted_venue_ids,
        per_venue_mids=quality_state.per_venue_mid_price,
        per_venue_ages=quality_state.per_venue_age_ms,
        dispersion_abs_usd=quality_state.dispersion_abs_usd,
        dispersion_bps=quality_state.dispersion_bps,
        quality_score=_compute_quality_score(
            quality_state=quality_state,
            freshness_policy=freshness_policy,
            minimum_venue_count=minimum_venue_count,
            outlier_bps_threshold=to_decimal(
                outlier_bps_threshold,
                field_name="outlier_bps_threshold",
            ),
        ),
        outlier_venue_ids=quality_state.outlier_venue_ids,
        diagnostics=quality_state.diagnostics,
    )


def aggregate_mid_prices(
    mid_prices: Sequence[Decimal],
    *,
    policy: MedianFamilyAggregationPolicy = DEFAULT_AGGREGATION_POLICY,
) -> tuple[Decimal, str]:
    """Aggregate venue mids using the ADR 0002 median family."""

    if not mid_prices:
        raise ValueError("mid_prices must not be empty")

    sorted_mids = sorted(
        to_decimal(mid_price, field_name="mid_price")
        for mid_price in mid_prices
    )
    count = len(sorted_mids)

    if count >= 5:
        trim_count = min(policy.trim_count_each_side, (count - 1) // 2)
        trimmed = sorted_mids[trim_count : count - trim_count]
        return (
            to_decimal(median(trimmed), field_name="trimmed_median"),
            TRIMMED_MEDIAN_METHOD,
        )

    return (
        to_decimal(median(sorted_mids), field_name=f"median_{count}"),
        f"median_{count}",
    )


def _latest_valid_quotes_by_venue(quotes: Iterable[ExchangeQuote]) -> dict[str, ExchangeQuote]:
    latest_quotes: dict[str, ExchangeQuote] = {}
    for quote in quotes:
        if quote.crossed_market_flag:
            continue
        if quote.normalization_status != "normalized":
            continue
        current = latest_quotes.get(quote.venue_id)
        if current is None or quote.event_ts >= current.event_ts:
            latest_quotes[quote.venue_id] = quote
    return latest_quotes


def _compute_quality_score(
    *,
    quality_state,
    freshness_policy: FreshnessPolicy,
    minimum_venue_count: int,
    outlier_bps_threshold: Decimal,
) -> Decimal:
    contributing_count = quality_state.contributing_venue_count
    trusted_count = quality_state.trusted_venue_count
    if trusted_count < minimum_venue_count:
        return Decimal("0")

    coverage_score = Decimal(trusted_count) / Decimal(max(contributing_count, minimum_venue_count))
    trusted_ages = [
        quality_state.per_venue_age_ms[venue_id]
        for venue_id in quality_state.trusted_venue_ids
        if quality_state.per_venue_age_ms[venue_id] is not None
    ]
    max_trusted_age_ms = max(trusted_ages) if trusted_ages else freshness_policy.stale_after_ms
    if freshness_policy.stale_after_ms == 0:
        age_score = Decimal("1") if max_trusted_age_ms == 0 else Decimal("0")
    else:
        age_score = _clamp_unit(
            Decimal("1")
            - (Decimal(max_trusted_age_ms) / Decimal(freshness_policy.stale_after_ms))
        )
    dispersion_bps = quality_state.dispersion_bps or Decimal("0")
    dispersion_score = (
        Decimal("1")
        if outlier_bps_threshold == 0
        else _clamp_unit(Decimal("1") - (dispersion_bps / outlier_bps_threshold))
    )
    outlier_score = Decimal("1") - (
        Decimal(len(quality_state.outlier_venue_ids)) / Decimal(max(contributing_count, 1))
    )

    score = (coverage_score + age_score + dispersion_score + outlier_score) / Decimal("4")
    return score.quantize(Decimal("0.0001"))


def _clamp_unit(value: Decimal) -> Decimal:
    if value < 0:
        return Decimal("0")
    if value > 1:
        return Decimal("1")
    return value


__all__ = [
    "DEFAULT_AGGREGATION_POLICY",
    "DEFAULT_MINIMUM_VENUE_COUNT",
    "FEATURE_VERSION",
    "CompositeNowcast",
    "MedianFamilyAggregationPolicy",
    "aggregate_mid_prices",
    "compute_composite_nowcast",
]
