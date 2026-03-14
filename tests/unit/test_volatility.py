from datetime import UTC, datetime, timedelta
from decimal import Decimal

from rtds.features.composite_nowcast import CompositeNowcast
from rtds.features.volatility import (
    CompositePriceObservation,
    VolatilityPolicy,
    compute_volatility_estimate,
    compute_volatility_from_nowcasts,
    observations_from_nowcasts,
)


def _observation(event_ts: datetime, price: str) -> CompositePriceObservation:
    return CompositePriceObservation(event_ts=event_ts, price=Decimal(price))


def _nowcast(as_of_ts: datetime, price: str | None) -> CompositeNowcast:
    return CompositeNowcast(
        as_of_ts=as_of_ts,
        composite_now_price=None if price is None else Decimal(price),
        composite_method="median_3" if price is not None else "missing",
        feature_version="0.1.0",
        composite_missing_flag=price is None,
        contributing_venue_count=3 if price is not None else 0,
        contributing_venues=("binance", "coinbase", "kraken") if price is not None else (),
        per_venue_mids={},
        per_venue_ages={},
        dispersion_abs_usd=Decimal("1") if price is not None else None,
        dispersion_bps=Decimal("0.1") if price is not None else None,
        quality_score=Decimal("0.9") if price is not None else Decimal("0"),
        outlier_venue_ids=(),
    )


def test_compute_volatility_estimate_blends_fast_and_baseline() -> None:
    start_ts = datetime(2026, 3, 14, 12, 0, 0, tzinfo=UTC)
    observations = [
        _observation(start_ts, "100"),
        _observation(start_ts + timedelta(seconds=1), "101"),
        _observation(start_ts + timedelta(seconds=2), "103.02"),
        _observation(start_ts + timedelta(seconds=3), "106.1106"),
        _observation(start_ts + timedelta(seconds=4), "110.355024"),
    ]
    policy = VolatilityPolicy(
        fast_return_count=2,
        baseline_return_count=4,
        fast_weight=Decimal("0.6"),
        sigma_floor=Decimal("0.00001"),
        sigma_cap=Decimal("1"),
    )

    estimate = compute_volatility_estimate(observations, policy=policy)

    assert estimate.fast_return_count == 2
    assert estimate.baseline_return_count == 4
    assert estimate.sigma_fast.quantize(Decimal("0.000000000001")) == Decimal("0.035355339059")
    assert estimate.sigma_baseline.quantize(Decimal("0.000000000001")) == Decimal(
        "0.027386127875"
    )
    assert estimate.sigma_eff.quantize(Decimal("0.000000000001")) == Decimal("0.032167654586")
    assert estimate.diagnostics == ()


def test_compute_volatility_estimate_uses_floor_when_no_returns_exist() -> None:
    estimate = compute_volatility_estimate(
        [_observation(datetime(2026, 3, 14, 12, 0, 0, tzinfo=UTC), "100")],
        policy=VolatilityPolicy(
            fast_return_count=2,
            baseline_return_count=4,
            fast_weight=Decimal("0.5"),
            sigma_floor=Decimal("0.0005"),
            sigma_cap=Decimal("1"),
        ),
    )

    assert estimate.sigma_fast == Decimal("0.0005")
    assert estimate.sigma_baseline == Decimal("0.0005")
    assert estimate.sigma_eff == Decimal("0.0005")
    assert estimate.fast_return_count == 0
    assert estimate.baseline_return_count == 0
    assert estimate.diagnostics == ("no_returns_available",)


def test_compute_volatility_estimate_caps_extreme_realized_volatility() -> None:
    start_ts = datetime(2026, 3, 14, 12, 0, 0, tzinfo=UTC)
    estimate = compute_volatility_estimate(
        [
            _observation(start_ts, "100"),
            _observation(start_ts + timedelta(seconds=1), "200"),
            _observation(start_ts + timedelta(seconds=2), "100"),
        ],
        policy=VolatilityPolicy(
            fast_return_count=2,
            baseline_return_count=2,
            fast_weight=Decimal("0.5"),
            sigma_floor=Decimal("0.00001"),
            sigma_cap=Decimal("0.25"),
        ),
    )

    assert estimate.sigma_fast == Decimal("0.25")
    assert estimate.sigma_baseline == Decimal("0.25")
    assert estimate.sigma_eff == Decimal("0.25")


def test_compute_volatility_from_nowcasts_ignores_missing_nowcasts() -> None:
    start_ts = datetime(2026, 3, 14, 12, 0, 0, tzinfo=UTC)
    nowcasts = [
        _nowcast(start_ts, "100"),
        _nowcast(start_ts + timedelta(seconds=1), None),
        _nowcast(start_ts + timedelta(seconds=2), "110"),
        _nowcast(start_ts + timedelta(seconds=3), "99"),
    ]

    observations = observations_from_nowcasts(nowcasts)
    estimate = compute_volatility_from_nowcasts(
        nowcasts,
        policy=VolatilityPolicy(
            fast_return_count=2,
            baseline_return_count=2,
            fast_weight=Decimal("0.5"),
            sigma_floor=Decimal("0.00001"),
            sigma_cap=Decimal("1"),
        ),
    )

    assert [observation.price for observation in observations] == [
        Decimal("100"),
        Decimal("110"),
        Decimal("99"),
    ]
    assert estimate.fast_return_count == 2
    assert estimate.baseline_return_count == 2
    assert estimate.sigma_fast == estimate.sigma_baseline
