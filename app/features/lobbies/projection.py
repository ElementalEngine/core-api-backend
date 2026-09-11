"""What each viewer is allowed to see of a lobby.

The projection strips fields rather than building a response from an
allow-list, so anything added to a lobby document is exposed unless something
removes it. A guard test fails on any field the builder writes that is not
explicitly classified.

Two surfaces are censored: a blind draft's pools and picks stay hidden until
every seat has picked. A leak here is silent -- the response looks correct to
everyone except the player whose pick was shown early.

Governed by D73, D86, D167.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.features.lobbies.phases import DRAFT, SETTINGS

DRAFT_BLIND = "blind"

BALLOT = "ballot"
POOL = "pool"
PICK = "pick"
POOL_APPEARANCES = "pool_appearances"


def ballots_are_secret(lobby: Mapping[str, Any]) -> bool:
    """During `settings` a seat sees its own ballot and nobody else's."""
    return lobby.get("phase") == SETTINGS


def pools_are_secret(lobby: Mapping[str, Any]) -> bool:
    """Blind draft, before the reveal."""
    return (
        lobby.get("phase") == DRAFT
        and (lobby.get("settings") or {}).get("draft_mode") == DRAFT_BLIND
        and lobby.get("revealed_at") is None
    )


def project_lobby(
    lobby: Mapping[str, Any], viewer_discord_id: str | None
) -> dict[str, Any]:
    """The lobby as `viewer_discord_id` may see it."""
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
        # The union of every pool. Disjoint pools mean a viewer who knows
        # the union and their own pool knows what the others were dealt.
        projected.pop(POOL_APPEARANCES, None)

    projected["seats"] = seats
    return projected


__all__ = [
    "ballots_are_secret",
    "pools_are_secret",
    "project_lobby",
]
