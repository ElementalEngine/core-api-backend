"""What each viewer is allowed to see of a lobby.

The projection strips fields rather than building a response from an
allow-list, so anything added to a lobby document is exposed unless something
removes it. A guard test fails on any field the builder writes that is not
explicitly classified.

Four things are private until everyone has acted: a settings ballot, a set of
bans submitted together, a pick, and a blind draft's pools. A leak here is
silent -- the response looks correct to everyone except the player whose
choice was shown early.

Governed by D73, D86, D167, D235.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.features.lobbies.bans import ban_caps
from app.features.lobbies.phases import BANS, COMPLETE, DRAFT, SETTINGS
from app.features.lobbies.questions import questions_for

DRAFT_BLIND = "blind"

BALLOT = "ballot"
SEAT_BANS = "bans"
POOL = "pool"
PICK = "pick"
CIV_PICK = "civ_pick"
POOL_APPEARANCES = "pool_appearances"


def ballots_are_secret(lobby: Mapping[str, Any]) -> bool:
    """During `settings` a seat sees its own ballot and nobody else's."""
    return lobby.get("phase") == SETTINGS


def bans_are_secret(lobby: Mapping[str, Any]) -> bool:
    """Bans submitted together stay private until they resolve; turns are public."""
    return lobby.get("phase") == BANS and not lobby.get("ban_order")


def picks_are_secret(lobby: Mapping[str, Any]) -> bool:
    """Nobody sees another seat's pick until the draft closes."""
    return lobby.get("phase") == DRAFT


def pools_are_secret(lobby: Mapping[str, Any]) -> bool:
    """Blind draft, before the reveal."""
    return (
        lobby.get("phase") == DRAFT
        and (lobby.get("settings") or {}).get("draft_mode") == DRAFT_BLIND
        and lobby.get("revealed_at") is None
    )


def project_lobby(
    lobby: Mapping[str, Any],
    viewer_discord_id: str | None,
    *,
    censored: bool = True,
) -> dict[str, Any]:
    """The lobby as `viewer_discord_id` may see it. Staff observers pass
    `censored=False` and still get the phase's questions and caps."""
    projected = dict(lobby)
    seats = [dict(seat) for seat in lobby.get("seats") or []]
    others = [seat for seat in seats if seat.get("discord_id") != viewer_discord_id]

    if censored and ballots_are_secret(lobby):
        # Participation, not preference: "4 of 6 submitted". Counted before
        # the ballots are removed, or the number would always be 0 or 1.
        projected["ballots_submitted"] = sum(
            1 for seat in seats if seat.get(BALLOT) is not None
        )
        for seat in others:
            seat.pop(BALLOT, None)

    if censored and bans_are_secret(lobby):
        for seat in others:
            seat.pop(SEAT_BANS, None)

    if censored and picks_are_secret(lobby):
        # `ready` still says who has locked in; only the choice is withheld.
        for seat in others:
            seat.pop(PICK, None)
            seat.pop(CIV_PICK, None)

    if censored and pools_are_secret(lobby):
        for seat in others:
            seat.pop(POOL, None)
        # The union of every pool. Disjoint pools mean a viewer who knows
        # the union and their own pool knows what the others were dealt.
        projected.pop(POOL_APPEARANCES, None)

    # The ballot's questions travel with the phases that show them, so every
    # client renders titles and labels from one catalogue rather than a copy.
    edition, game_type = lobby.get("edition"), lobby.get("game_type")
    if lobby.get("phase") in (SETTINGS, COMPLETE) and edition and game_type:
        projected["questions"] = questions_for(edition, game_type)
    if lobby.get("phase") == BANS and edition:
        projected["ban_caps"] = ban_caps(edition, lobby.get("starting_age"))

    projected["seats"] = seats
    return projected


__all__ = [
    "ballots_are_secret",
    "bans_are_secret",
    "picks_are_secret",
    "pools_are_secret",
    "project_lobby",
]
