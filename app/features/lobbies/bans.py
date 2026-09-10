"""Resolving the ban phase.

Each seated player submits a set of leader and civ keys. A key lands when it
reaches a majority of all seats -- not of the players who submitted, since a
timeout creates non-submitters and must not make bans easier to land.

The number that can land is capped per edition, and for civ7 the civ cap
depends on the starting age chosen when the lobby was created. At the cap,
keys tied on the boundary all drop, but only when something below the cap
ties with them; a cap falling in a gap keeps the full set.

Bans name tokens from the civ-data catalogue. Civs are checked against the
starting age's pool only, so a real token for a civ that is not in the game
cannot use up one of the three slots.

Governed by D195, D196.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

BAN_KINDS = ("leader", "civ")

# civ-data's vocabulary, not Mite v1's. v1's slash command offers
# `Antiquity_Age`; every `CivRow.age_pool` says `AGE_ANTIQUITY`. A filter
# written against the wrong one matches nothing, so NO civ is bannable and
# nothing errors -- the cap simply never binds. One vocabulary behind the API,
# and Mite v2 maps at its own edge (D196).
AGE_POOLS = ("AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN")
NO_STARTING_AGE = None

CIV6_CAPS = {"leader": 20, "civ": 0}
CIV7_LEADER_CAP = 10
CIV7_CIV_CAP_ANY_AGE = 10
CIV7_CIV_CAP_FIXED_AGE = 3


def ban_caps(edition: str, starting_age: str | None) -> dict[str, int]:
    """How many bans may land, per kind.

    Civ7's civ cap depends on `starting_age`, which the SETTINGS vote
    resolved -- the first place one phase's outcome constrains the next
    phase's input. Read from the lobby, never from the request.
    """
    if edition == "civ6":
        return dict(CIV6_CAPS)
    fixed_age = starting_age not in (None, "none", "None")
    return {
        "leader": CIV7_LEADER_CAP,
        "civ": CIV7_CIV_CAP_FIXED_AGE if fixed_age else CIV7_CIV_CAP_ANY_AGE,
    }


def bannable_civs(
    civs: Sequence[Mapping[str, Any]], starting_age: str | None
) -> list[str]:
    """The civ tokens a ban may name.

    With a starting age chosen, only that age's pool is reachable in game,
    so only it is bannable -- roughly fifteen civs against a cap of three.
    Without one, all forty-four are, against a cap of ten. Filtering by an
    age nothing matches would leave nothing bannable and raise nothing.
    """
    if starting_age is NO_STARTING_AGE:
        return [civ["token"] for civ in civs]
    if starting_age not in AGE_POOLS:
        raise ValueError(f"unknown starting age: {starting_age!r}")
    return [civ["token"] for civ in civs if civ.get("age_pool") == starting_age]


def capped(counts: Mapping[str, int], qualified: Sequence[str], cap: int) -> list[str]:
    """The top `cap` bans, dropping every key tied at the boundary (D72).

    Seventeen qualify, the cap is fifteen, and ranks 15-17 all sit on six
    votes: all three drop and fourteen land. Deterministic, no RNG, and no
    submission timestamps to store. Random tie-breaks are unexplainable to
    players; earliest-to-threshold rewards fast clicking. Both were rejected.

    Every qualifying key tied above the cap therefore lands NOTHING -- the
    rule taken to its end, erring toward the larger pool as intended.
    """
    if cap <= 0:
        return []
    ranked = sorted(qualified, key=lambda key: (-counts[key], key))
    if len(ranked) <= cap:
        return ranked
    boundary = counts[ranked[cap - 1]]
    # Only a tie that CROSSES the cap drops the boundary. If the first
    # excluded key sits lower, the cap falls in a gap and the top `cap` land
    # intact -- dropping the boundary unconditionally would silently shrink
    # every capped result by one.
    if counts[ranked[cap]] != boundary:
        return ranked[:cap]
    return [key for key in ranked[:cap] if counts[key] > boundary]


def resolve_bans(
    seats: Sequence[Mapping[str, Any]], edition: str, starting_age: str | None
) -> dict[str, list[str]]:
    """The bans that land, per kind."""
    voters = [seat for seat in seats if seat.get("discord_id")]
    # Of ALL seats, not of submitters. A timeout creates non-submitters, and
    # counting only those who voted would make bans EASIER to land as people
    # drop out, which is backwards.
    need = len(voters) // 2 + 1
    caps = ban_caps(edition, starting_age)

    resolved: dict[str, list[str]] = {}
    for kind in BAN_KINDS:
        counts: Counter[str] = Counter()
        for seat in voters:
            submitted = (seat.get("bans") or {}).get(f"{kind}_keys") or []
            for key in submitted:
                counts[key] += 1
        qualified = [key for key, votes in counts.items() if votes >= need]
        resolved[kind] = capped(counts, qualified, caps[kind])
    return resolved


__all__ = [
    "AGE_POOLS",
    "BAN_KINDS",
    "ban_caps",
    "bannable_civs",
    "capped",
    "resolve_bans",
]
