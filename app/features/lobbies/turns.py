"""Whose turn it is, for the modes that have turns.

⚠ **Two of five draft modes are turn-ordered and three are not.** `standard`
and `blind` let every seat pick whenever it likes; `snake` and `cwc` do not,
and `random` has no picks at all. `turn_index` was written by every advance
and read by nothing until this module existed, so `PUT /picks` accepted a
pick from any seat at any moment -- right for three modes, wrong for two.

Both mechanisms reduce to one shape: **an explicit list with one entry per
pick to be made**, which `turn_index` indexes. Nothing has to re-derive an
ordering rule at pick time, and the stored order is what a dispute reads.
"""

from __future__ import annotations

from collections.abc import Sequence

# ⚠ Carried verbatim from Mite's `draft.config.ts`. Sixteen entries of TEAM
# index, sliced to `team_size * 2` -- a 3v3 uses the first six. It is not a
# plain alternation and it is not derivable from a rule, which is why it is
# a table rather than a formula.
CWC_PICK_ORDER = (0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1)

TURN_ORDERED = ("snake", "cwc")


def snake_order(players: Sequence[str], rounds: int) -> list[str]:
    """One pass per round, reversing each time.

    ⚠ The snake is the reversal BETWEEN rounds, not within one. civ6 drafts
    leaders alone, so a single pass in seat order is the whole of it; civ7
    adds a civ round that runs backwards, which is what stops the first seat
    taking the best of both.
    """
    order: list[str] = []
    for index in range(rounds):
        leg = list(players) if index % 2 == 0 else list(reversed(players))
        order.extend(leg)
    return order


def cwc_order(captains: Sequence[str], picks: int) -> list[str]:
    """The captain who picks at each turn, by Mite's fixed table.

    Raises ValueError past the table's length rather than wrapping: wrapping
    would silently give a longer draft an ordering nobody chose.
    """
    if len(captains) != 2:
        raise ValueError(f"cwc needs exactly two captains, got {len(captains)}")
    if picks > len(CWC_PICK_ORDER):
        raise ValueError(f"cwc order covers {len(CWC_PICK_ORDER)} picks, not {picks}")
    return [captains[team] for team in CWC_PICK_ORDER[:picks]]


def whose_turn(order: Sequence[str], turn_index: int) -> str | None:
    """The player owed the next pick, or None when the order is spent."""
    if 0 <= turn_index < len(order):
        return order[turn_index]
    return None


def is_turn_ordered(draft_mode: str | None) -> bool:
    return draft_mode in TURN_ORDERED


__all__ = [
    "CWC_PICK_ORDER",
    "TURN_ORDERED",
    "cwc_order",
    "is_turn_ordered",
    "snake_order",
    "whose_turn",
]
