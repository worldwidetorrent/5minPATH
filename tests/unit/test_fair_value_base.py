from decimal import Decimal

from rtds.features.fair_value_base import compute_fair_value_base


def test_compute_fair_value_base_returns_neutral_value_for_flat_move() -> None:
    estimate = compute_fair_value_base(
        chainlink_open_anchor_price=Decimal("100"),
        composite_now_price=Decimal("100"),
        seconds_remaining=180,
        sigma_eff=Decimal("0.01"),
    )

    assert estimate.log_move_from_open == Decimal("0")
    assert estimate.abs_move_from_open == Decimal("0")
    assert estimate.z_base == Decimal("0")
    assert estimate.fair_value_base == Decimal("0.5")
    assert estimate.diagnostics == ()


def test_compute_fair_value_base_moves_above_half_for_positive_log_move() -> None:
    estimate = compute_fair_value_base(
        chainlink_open_anchor_price=Decimal("100"),
        composite_now_price=Decimal("101"),
        seconds_remaining=100,
        sigma_eff=Decimal("0.01"),
    )

    assert estimate.abs_move_from_open == Decimal("1")
    assert estimate.log_move_from_open.quantize(Decimal("0.000000000001")) == Decimal(
        "0.009950330853"
    )
    assert estimate.denominator_sigma_horizon == Decimal("0.10")
    assert estimate.z_base.quantize(Decimal("0.000000000001")) == Decimal("0.099503308532")
    assert estimate.fair_value_base.quantize(Decimal("0.000000000001")) == Decimal(
        "0.539630669445"
    )


def test_compute_fair_value_base_returns_extreme_probabilities_at_expiry() -> None:
    higher = compute_fair_value_base(
        chainlink_open_anchor_price=Decimal("100"),
        composite_now_price=Decimal("101"),
        seconds_remaining=0,
        sigma_eff=Decimal("0.01"),
    )
    lower = compute_fair_value_base(
        chainlink_open_anchor_price=Decimal("100"),
        composite_now_price=Decimal("99"),
        seconds_remaining=0,
        sigma_eff=Decimal("0.01"),
    )
    flat = compute_fair_value_base(
        chainlink_open_anchor_price=Decimal("100"),
        composite_now_price=Decimal("100"),
        seconds_remaining=0,
        sigma_eff=Decimal("0.01"),
    )

    assert higher.fair_value_base == Decimal("1")
    assert lower.fair_value_base == Decimal("0")
    assert flat.fair_value_base == Decimal("0.5")
    assert higher.z_base is None
    assert lower.z_base is None
    assert flat.z_base is None
    assert higher.diagnostics == ("expiry_boundary",)
    assert lower.diagnostics == ("expiry_boundary",)
    assert flat.diagnostics == ("expiry_boundary",)


def test_compute_fair_value_base_returns_null_when_inputs_are_missing() -> None:
    estimate = compute_fair_value_base(
        chainlink_open_anchor_price=None,
        composite_now_price=Decimal("100"),
        seconds_remaining=120,
        sigma_eff=None,
    )

    assert estimate.z_base is None
    assert estimate.fair_value_base is None
    assert estimate.diagnostics == ("missing_open_anchor", "missing_sigma_eff")
