
from __future__ import annotations

from typing import Optional

from app.parsers.civ6leaders import civ6_leaders_dict
from app.parsers.civ7leaders import civ7_leaders_dict


def get_cpl_name(game: str, civ: str, leader: Optional[str] = None) -> str:
    """Return a canonical civ/leader name used for stats/leaderboards.

    For Civ6, mapping is based on the civilization name.
    For Civ7, mapping uses (civ, leader) and requires a leader.

    Args:
        game: Either "civ6" or "civ7".
        civ: The in-game civilization name.
        leader: The in-game leader name (required for Civ7).

    Returns:
        The canonical name to use in stats.
    """

    if game == "civ6":
        return civ6_leaders_dict.get(civ, civ)

    if game == "civ7":
        if leader is None:
            raise ValueError("Leader name must be provided for Civ7.")
        return civ7_leaders_dict.get((civ, leader), civ)

    raise ValueError("Unsupported game type. Use 'civ6' or 'civ7'.")
