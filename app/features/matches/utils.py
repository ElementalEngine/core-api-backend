from __future__ import annotations

from typing import Any, Optional

from app.features.matches.parsers.civ6leaders import civ6_leaders_dict
from app.features.matches.parsers.civ7leaders import civ7_leaders_dict


def get_cpl_name(game: str, civ: str, leader: Optional[str] = None) -> str:
    """Return the canonical CPL civ/leader name used by stats and leaderboards.

    Civ6 uses civ-only mapping.
    Civ7 uses (civ, leader) mapping and requires leader to be provided.
    """

    if game == "civ6":
        return civ6_leaders_dict.get(civ, civ)

    if game == "civ7":
        if leader is None:
            raise ValueError("Leader name must be provided for Civ7.")
        return civ7_leaders_dict.get((civ, leader), civ)

    raise ValueError("Unsupported game type. Use 'civ6' or 'civ7'.")


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


__all__ = ["as_float", "as_int", "get_cpl_name"]
