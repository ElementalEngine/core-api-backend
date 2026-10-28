"""Deals draft pools from what is left after bans. Every pool is the same size,
and deals are unseeded so nobody can work out a blind pool in advance."""

from __future__ import annotations

from collections.abc import Sequence
from random import SystemRandom
from typing import Any

_RANDOM = SystemRandom()


class NotEnoughPool(ValueError):
    """The post-ban set cannot cover what the draft needs."""


def even_split(total: int, players: int) -> tuple[int, int]:
    """Per-player pool size, and how many entries fall off the end."""
    if players <= 0:
        raise NotEnoughPool("a draft needs at least one player")
    per_player = total // players
    if per_player < 1:
        raise NotEnoughPool(
            f"{total} remain after bans and {players} players need one each"
        )
    return per_player, total % players


def deal(
    tokens: Sequence[str],
    players: int,
    rng: Any = _RANDOM,
    per_player: int | None = None,
) -> list[list[str]]:
    """One disjoint pool per player, every pool the same size: `per_player`, or
    the even split of what is left."""
    if per_player is None:
        per_player, _ = even_split(len(tokens), players)
    elif per_player * players > len(tokens):
        raise NotEnoughPool(
            f"{len(tokens)} remain after bans and {players} pools of "
            f"{per_player} need {per_player * players}"
        )
    shuffled = list(tokens)
    rng.shuffle(shuffled)
    return [
        shuffled[index * per_player : (index + 1) * per_player]
        for index in range(players)
    ]


CIV_TARGET_PER_PLAYER = 4
CIV_TARGET_BY_TEAM_COUNT = {2: 7, 3: 5, 4: 5}


def civ_target(game_type: str, groups: int) -> int:
    """How many civs each player or team is offered: a fixed target, because
    there are too few civs to split by player count."""
    if game_type == "teamer":
        return CIV_TARGET_BY_TEAM_COUNT.get(groups, CIV_TARGET_PER_PLAYER)
    return CIV_TARGET_PER_PLAYER


def deal_civs(
    tokens: Sequence[str], groups: int, target: int, rng: Any = _RANDOM
) -> list[list[str]]:
    """Civ pools, disjoint while there are enough civs; otherwise each group draws
    on its own and a civ can be offered to more than one."""
    if target < 1:
        raise NotEnoughPool(f"a civ target of {target} offers nothing")
    if not tokens:
        raise NotEnoughPool("no civs remain after bans")
    if len(tokens) >= target * groups:
        shuffled = list(tokens)
        rng.shuffle(shuffled)
        return [shuffled[i * target : (i + 1) * target] for i in range(groups)]
    return [rng.sample(list(tokens), min(target, len(tokens))) for _ in range(groups)]


def assign_one_each(
    tokens: Sequence[str], players: int, rng: Any = _RANDOM
) -> list[str]:
    """One token per player, all distinct -- `random` mode's whole draft."""
    if len(tokens) < players:
        raise NotEnoughPool(
            f"{len(tokens)} remain after bans and {players} players need one each"
        )
    shuffled = list(tokens)
    rng.shuffle(shuffled)
    return shuffled[:players]


def remaining_after_bans(tokens: Sequence[str], banned: Sequence[str]) -> list[str]:
    """The draftable set, in the catalogue's own order."""
    refused = set(banned)
    return [token for token in tokens if token not in refused]


__all__ = [
    "NotEnoughPool",
    "assign_one_each",
    "civ_target",
    "deal",
    "deal_civs",
    "even_split",
    "remaining_after_bans",
]
