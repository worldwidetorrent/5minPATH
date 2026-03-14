"""Project-wide numeric validation helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from rtds.core.types import ContractPrice, DecimalLike, UsdPrice

ZERO = Decimal("0")
ONE = Decimal("1")
MAX_REASONABLE_VOLATILITY = Decimal("5")


def to_decimal(value: DecimalLike, *, field_name: str = "value") -> Decimal:
    """Convert numeric-like input to a finite Decimal."""

    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be numeric, not bool")

    try:
        if isinstance(value, Decimal):
            decimal_value = value
        elif isinstance(value, int):
            decimal_value = Decimal(value)
        else:
            decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid decimal-like value") from exc

    if not decimal_value.is_finite():
        raise ValueError(f"{field_name} must be finite")

    return decimal_value


def validate_probability(value: DecimalLike, *, field_name: str = "probability") -> Decimal:
    """Validate a probability-like value in the closed interval [0, 1]."""

    decimal_value = to_decimal(value, field_name=field_name)
    if decimal_value < ZERO or decimal_value > ONE:
        raise ValueError(f"{field_name} must be in [0, 1]")
    return decimal_value


def validate_contract_price(
    value: DecimalLike,
    *,
    field_name: str = "contract_price",
) -> ContractPrice:
    """Validate canonical binary contract prices stored as decimals."""

    return ContractPrice(validate_probability(value, field_name=field_name))


def validate_usd_price(value: DecimalLike, *, field_name: str = "usd_price") -> UsdPrice:
    """Validate a non-negative USD price."""

    decimal_value = to_decimal(value, field_name=field_name)
    if decimal_value < ZERO:
        raise ValueError(f"{field_name} must be non-negative")
    return UsdPrice(decimal_value)


def validate_size(value: DecimalLike, *, field_name: str = "size") -> Decimal:
    """Validate a non-negative size or quantity value."""

    decimal_value = to_decimal(value, field_name=field_name)
    if decimal_value < ZERO:
        raise ValueError(f"{field_name} must be non-negative")
    return decimal_value


def validate_volatility(value: DecimalLike, *, field_name: str = "volatility") -> Decimal:
    """Validate volatility stored as a decimal fraction rather than a percent."""

    decimal_value = to_decimal(value, field_name=field_name)
    if decimal_value < ZERO:
        raise ValueError(f"{field_name} must be non-negative")
    if decimal_value > MAX_REASONABLE_VOLATILITY:
        raise ValueError(f"{field_name} must be stored as a decimal, not a percent")
    return decimal_value


__all__ = [
    "MAX_REASONABLE_VOLATILITY",
    "ONE",
    "ZERO",
    "to_decimal",
    "validate_contract_price",
    "validate_probability",
    "validate_size",
    "validate_usd_price",
    "validate_volatility",
]
