"""Strict finite numeric helpers for execution decisions.

Every financial value that can influence an order must pass through here.
NaN, +/-Infinity, None and non-numeric values are rejected explicitly —
never via float comparisons (NaN compares false to everything, so
`value > 0` silently accepts NaN in some code paths).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


def to_finite_decimal(value) -> Decimal | None:
    """Convert to Decimal, rejecting None/NaN/Infinity/non-numeric.

    Bools are rejected: True/False must never stand in for a quantity.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not dec.is_finite():
        return None
    return dec


def positive_decimal(value) -> Decimal | None:
    """Finite Decimal strictly greater than zero, else None."""
    dec = to_finite_decimal(value)
    if dec is None or dec <= 0:
        return None
    return dec


def non_negative_decimal(value) -> Decimal | None:
    """Finite Decimal >= 0, else None (for available quantities)."""
    dec = to_finite_decimal(value)
    if dec is None or dec < 0:
        return None
    return dec


def decimal_str(value: Decimal, places: str = "0.01") -> str:
    """Stable positional serialization for audit payloads."""
    return format(value.quantize(Decimal(places)), "f")
