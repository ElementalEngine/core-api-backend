"""How many bans each edition allows, and which voted bans land."""

from __future__ import annotations

from collections.abc import Mapping, Sequence, Set
from typing import Any

BAN_KINDS = ("leader", "civ")

AGE_POOLS = ("AGE_ANTIQUITY", "AGE_EXPLORATION", "AGE_MODERN")
NO_STARTING_AGE = None

CIV6_CAPS = {"leader": 20, "civ": 0}
CIV7_LEADER_CAP = 10
CIV7_CIV_CAP_ANY_AGE = 10
CIV7_CIV_CAP_FIXED_AGE = 3


def ban_caps(edition: str, starting_age: str | None) -> dict[str, int]:
    """How many bans may land, per kind."""
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
    """The civ tokens a ban may name."""
    if starting_age is NO_STARTING_AGE:
        return [civ["token"] for civ in civs]
    if starting_age not in AGE_POOLS:
        raise ValueError(f"unknown starting age: {starting_age!r}")
    return [civ["token"] for civ in civs if civ.get("age_pool") == starting_age]


def capped(counts: Mapping[str, int], qualified: Sequence[str], cap: int) -> list[str]:
    """The top `cap` bans, dropping every key tied at the boundary."""
    if cap <= 0:
        return []
    ranked = sorted(qualified, key=lambda key: (-counts[key], key))
    if len(ranked) <= cap:
        return ranked
    boundary = counts[ranked[cap - 1]]
    if counts[ranked[cap]] != boundary:
        return ranked[:cap]
    return [key for key in ranked[:cap] if counts[key] > boundary]


def voted_bans(
    votes: Mapping[str, Sequence[str]],
    voters: Set[str],
    allowed: Sequence[str],
    room: int,
) -> list[str]:
    """The voted bans that land: each needs a majority of all the players, not
    of those who voted, and at most `room` land."""
    need = len(voters) // 2 + 1
    counts = {
        token: len(voters & set(votes.get(token, ())))
        for token in dict.fromkeys(allowed)
    }
    return capped(counts, [t for t, n in counts.items() if n >= need], room)


__all__ = [
    "AGE_POOLS",
    "BAN_KINDS",
    "ban_caps",
    "bannable_civs",
    "capped",
    "voted_bans",
]
