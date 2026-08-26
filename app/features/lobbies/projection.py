"""D73's per-recipient visibility projection.

The server projects a different document per recipient. **Hidden information
never leaves core-api** -- it is never filtered client-side.

Two censored surfaces in the whole lobby, and no others:

  settings     own ballot only, no tallies; observers see nothing until close
  blind draft  own pool AND own pick only; observers nothing until reveal

⚠ The blind row is the one that is easy to get wrong: **the pool is secret,
not just the pick.** Censoring `pick` alone leaks by elimination -- and so
does leaving `pool_appearances` in place, since pools are disjoint across
players (spec section 4), making that array the union of every pool.

Default-deny: a phase this module does not recognise is censored, not shown.
An over-censoring bug is visible and gets reported; an under-censoring one is
silent, which is why D86 Rule 3 calls this the only place where a defect is
adversarial.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

PHASE_SETTINGS = "settings"
PHASE_DRAFT = "draft"
PHASE_COMPLETE = "complete"
DRAFT_BLIND = "blind"

BALLOT = "ballot"
POOL = "pool"
PICK = "pick"
POOL_APPEARANCES = "pool_appearances"


def ballots_are_secret(lobby: Mapping[str, Any]) -> bool:
    """During `settings` a seat sees its own ballot and nobody else's.

    Once settings closes the ballots are public -- the table censors the
    `settings` row only.
    """
    return lobby.get("phase") == PHASE_SETTINGS


def pools_are_secret(lobby: Mapping[str, Any]) -> bool:
    """Blind draft, before the reveal.

    `complete` is not `draft`, so finishing reveals everything without a
    second rule. A cancelled blind draft stays censored: the table says
    nothing about it, and the safe reading of silence is to withhold.
    """
    return (
        lobby.get("phase") == PHASE_DRAFT
        and lobby.get("draft_mode") == DRAFT_BLIND
        and lobby.get("revealed_at") is None
    )


def project_lobby(
    lobby: Mapping[str, Any], viewer_discord_id: str | None
) -> dict[str, Any]:
    """The lobby as `viewer_discord_id` may see it.

    An observer -- anyone not holding a seat, including `None` -- matches no
    seat, so every per-seat rule below hides everything from them without a
    separate branch.
    """
    projected = dict(lobby)
    seats = [dict(seat) for seat in lobby.get("seats") or []]

    if ballots_are_secret(lobby):
        # Participation, not preference: "4 of 6 submitted". Counted before
        # the ballots are removed, or the number would always be 0 or 1.
        projected["ballots_submitted"] = sum(
            1 for seat in seats if seat.get(BALLOT) is not None
        )
        for seat in seats:
            if seat.get("discord_id") != viewer_discord_id:
                seat.pop(BALLOT, None)

    if pools_are_secret(lobby):
        for seat in seats:
            if seat.get("discord_id") != viewer_discord_id:
                seat.pop(POOL, None)
                seat.pop(PICK, None)
        # ⚠ The union of every pool. Disjoint pools mean a viewer who knows
        # the union and their own pool knows what the others were dealt.
        projected.pop(POOL_APPEARANCES, None)

    projected["seats"] = seats
    return projected


__all__ = [
    "ballots_are_secret",
    "pools_are_secret",
    "project_lobby",
]
