"""Venue dispersion quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from statistics import median
from typing import Iterable

from rtds.core.time import ensure_utc
from rtds.core.units import to_decimal
from rtds.quality.freshness import (
    DEFAULT_FRESHNESS_POLICY,
    FreshnessPolicy,
    assess_source_freshness,
)
from rtds.schemas.normalized import ExchangeQuote
from rtds.schemas.quality import CompositeDispersionState

DEFAULT_OUTLIER_ABS_THRESHOLD_USD = Decimal("25")
DEFAULT_OUTLIER_BPS_THRESHOLD = Decimal("5")
DEFAULT_MIN_CONTRIBUTING_VENUES = 2


@dataclass(slots=True, frozen=True)
class DispersionPolicy:
    """Explicit thresholds for exchange quote trust and outlier detection."""

    min_contributing_venues: int = DEFAULT_MIN_CONTRIBUTING_VENUES
    outlier_abs_threshold_usd: Decimal = DEFAULT_OUTLIER_ABS_THRESHOLD_USD
    outlier_bps_threshold: Decimal = DEFAULT_OUTLIER_BPS_THRESHOLD

    def __post_init__(self) -> None:
        if self.min_contributing_venues <= 0:
            raise ValueError("min_contributing_venues must be positive")
        if self.outlier_abs_threshold_usd < 0:
            raise ValueError("outlier_abs_threshold_usd must be non-negative")
        if self.outlier_bps_threshold < 0:
            raise ValueError("outlier_bps_threshold must be non-negative")


DEFAULT_DISPERSION_POLICY = DispersionPolicy()


def assess_exchange_composite_quality(
    quotes: Iterable[ExchangeQuote],
    *,
    as_of_ts: datetime,
    freshness_policy: FreshnessPolicy = DEFAULT_FRESHNESS_POLICY,
    dispersion_policy: DispersionPolicy = DEFAULT_DISPERSION_POLICY,
) -> CompositeDispersionState:
    """Assess exchange-quote trust for a composite now-price snapshot."""

    normalized_as_of_ts = ensure_utc(as_of_ts, field_name="as_of_ts")
    latest_quotes = _latest_quotes_by_venue(quotes)

    per_venue_age_ms: dict[str, int | None] = {}
    per_venue_mid_price: dict[str, Decimal | None] = {}
    contributing_quotes: dict[str, ExchangeQuote] = {}
    contributing_venue_ids: list[str] = []

    for venue_id, quote in latest_quotes.items():
        freshness = assess_source_freshness(
            venue_id,
            as_of_ts=normalized_as_of_ts,
            last_event_ts=quote.event_ts,
            policy=freshness_policy,
        )
        per_venue_age_ms[venue_id] = freshness.last_event_age_ms
        per_venue_mid_price[venue_id] = quote.mid_price
        if freshness.usable_flag:
            contributing_quotes[venue_id] = quote
            contributing_venue_ids.append(venue_id)

    contributing_venue_ids = sorted(contributing_venue_ids)
    diagnostics: list[str] = []

    if len(contributing_quotes) < dispersion_policy.min_contributing_venues:
        diagnostics.append("insufficient_contributing_venues")
        return CompositeDispersionState(
            as_of_ts=normalized_as_of_ts,
            contributing_venue_count=len(contributing_quotes),
            trusted_venue_count=len(contributing_quotes),
            contributing_venue_ids=tuple(contributing_venue_ids),
            trusted_venue_ids=tuple(contributing_venue_ids),
            per_venue_age_ms=per_venue_age_ms,
            per_venue_mid_price=per_venue_mid_price,
            dispersion_abs_usd=None,
            dispersion_bps=None,
            outlier_venue_ids=(),
            insufficient_venues_flag=True,
            usable_flag=False,
            diagnostics=tuple(diagnostics),
        )

    mids = [quote.mid_price for quote in contributing_quotes.values()]
    reference_mid = to_decimal(median(mids), field_name="reference_mid")
    dispersion_abs_usd = max(mids) - min(mids)
    dispersion_bps = (
        Decimal("0")
        if reference_mid == 0
        else (dispersion_abs_usd / reference_mid) * Decimal("10000")
    )

    outlier_venue_ids: list[str] = []
    for venue_id, quote in contributing_quotes.items():
        deviation_abs = abs(quote.mid_price - reference_mid)
        deviation_bps = (
            Decimal("0")
            if reference_mid == 0
            else (deviation_abs / reference_mid) * Decimal("10000")
        )
        if (
            deviation_abs >= dispersion_policy.outlier_abs_threshold_usd
            or deviation_bps >= dispersion_policy.outlier_bps_threshold
        ):
            outlier_venue_ids.append(venue_id)

    trusted_venue_ids = sorted(
        venue_id for venue_id in contributing_venue_ids if venue_id not in set(outlier_venue_ids)
    )
    if outlier_venue_ids:
        diagnostics.append("outlier_detected")

    return CompositeDispersionState(
        as_of_ts=normalized_as_of_ts,
        contributing_venue_count=len(contributing_quotes),
        trusted_venue_count=len(trusted_venue_ids),
        contributing_venue_ids=tuple(contributing_venue_ids),
        trusted_venue_ids=tuple(trusted_venue_ids),
        per_venue_age_ms=per_venue_age_ms,
        per_venue_mid_price=per_venue_mid_price,
        dispersion_abs_usd=dispersion_abs_usd,
        dispersion_bps=dispersion_bps,
        outlier_venue_ids=tuple(outlier_venue_ids),
        insufficient_venues_flag=False,
        usable_flag=len(trusted_venue_ids) >= dispersion_policy.min_contributing_venues,
        diagnostics=tuple(diagnostics),
    )


def _latest_quotes_by_venue(quotes: Iterable[ExchangeQuote]) -> dict[str, ExchangeQuote]:
    latest_quotes: dict[str, ExchangeQuote] = {}
    for quote in quotes:
        current = latest_quotes.get(quote.venue_id)
        if current is None or quote.event_ts >= current.event_ts:
            latest_quotes[quote.venue_id] = quote
    return latest_quotes


__all__ = [
    "DEFAULT_DISPERSION_POLICY",
    "DEFAULT_MIN_CONTRIBUTING_VENUES",
    "DEFAULT_OUTLIER_ABS_THRESHOLD_USD",
    "DEFAULT_OUTLIER_BPS_THRESHOLD",
    "DispersionPolicy",
    "assess_exchange_composite_quality",
]
