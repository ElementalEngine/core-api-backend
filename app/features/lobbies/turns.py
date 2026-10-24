"""Whose turn it is in a CWC draft, the only mode with turns. The order is
stored on the lobby when the draft is dealt, so a dispute reads what was used."""

from __future__ import annotations

from collections.abc import Sequence

CWC_PICK_ORDER = (0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1)


def cwc_order(captains: Sequence[str], turns: int) -> list[str]:
    """The captain who acts at each turn, for bans or for picks."""
    if len(captains) != 2:
        raise ValueError(f"cwc needs exactly two captains, got {len(captains)}")
    if turns > len(CWC_PICK_ORDER):
        raise ValueError(f"cwc order covers {len(CWC_PICK_ORDER)} turns, not {turns}")
    return [captains[team] for team in CWC_PICK_ORDER[:turns]]


def whose_turn(order: Sequence[str], turn_index: int) -> str | None:
    """The player owed the next action, or None when the order is spent."""
    if 0 <= turn_index < len(order):
        return order[turn_index]
    return None


__all__ = ["CWC_PICK_ORDER", "cwc_order", "whose_turn"]
