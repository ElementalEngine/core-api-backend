"""Resolving the ban phase.

⚠ **One mechanism for every game type** (D195). Every seated player submits
bans, a key lands at `floor(n/2)+1` of ALL seats, and the total is capped per
edition. D72 specified captains banning in sequence for teamers and duels;
nothing in Mite has ever done that -- `majorityBans` runs over every voter
regardless of game type -- so this matches shipped behaviour and supersedes
that section rather than changing how a teamer plays.

⚠ **The caps are neither v1's nor D72's.** v1 ships civ6 leader 25, civ7
leader 10 with civ 15 or 5; D72 recorded 15 and 5 for leaders and forgot civ
bans existed. These are a deliberate change from the measured baseline.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

BAN_KINDS = ("leader", "civ")

CIV6_CAPS = {"leader": 20, "civ": 0}
CIV7_LEADER_CAP = 10
CIV7_CIV_CAP_ANY_AGE = 10
CIV7_CIV_CAP_FIXED_AGE = 3


def ban_caps(edition: str, starting_age: str | None) -> dict[str, int]:
    """How many bans may land, per kind.

    ⚠ Civ7's civ cap depends on `starting_age`, which the SETTINGS vote
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


def capped(counts: Mapping[str, int], qualified: Sequence[str], cap: int) -> list[str]:
    """The top `cap` bans, dropping every key tied at the boundary (D72).

    ⚠ Seventeen qualify, the cap is fifteen, and ranks 15-17 all sit on six
    votes: all three drop and fourteen land. Deterministic, no RNG, and no
    submission timestamps to store. Random tie-breaks are unexplainable to
    players; earliest-to-threshold rewards fast clicking. Both were rejected.

    ⚠ Every qualifying key tied above the cap therefore lands NOTHING -- the
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
    # ⚠ Of ALL seats, not of submitters. A timeout creates non-submitters, and
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


__all__ = ["BAN_KINDS", "ban_caps", "capped", "resolve_bans"]
