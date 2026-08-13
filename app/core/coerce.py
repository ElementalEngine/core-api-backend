from __future__ import annotations

from typing import Any


def as_float(value: Any, default: float) -> float:
    """Safely coerce Mongo values (None/Decimal128/Int64/str) to float."""
    if value is None:
        return default
    try:
        if hasattr(value, "to_decimal"):
            value = value.to_decimal()
        return float(value)
    except TypeError, ValueError:
        return default


def as_int(value: Any, default: int) -> int:
    """Safely coerce Mongo values (None/Decimal128/Int64/str) to int."""
    if value is None:
        return default
    try:
        if hasattr(value, "to_decimal"):
            value = value.to_decimal()
        if isinstance(value, str):
            return int(float(value))
        return int(value)
    except TypeError, ValueError:
        return default


__all__ = ["as_float", "as_int"]
