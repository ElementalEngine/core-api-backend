"""Deals draft pools from what is left after bans. Every pool is the same size,
and deals are unseeded so nobody can work out a blind pool in advance."""

from __future__ import annotations

from collections.abc import Sequence
from random import SystemRandom
from typing import Any

_RANDOM = SystemRandom()
# A blind pool is picked from one menu, which holds 25.
BLIND_POOL = 25


class NotEnoughPool(ValueError):
    """The post-ban set cannot cover what the draft needs."""


def even_split(total: int, players: int) -> int:
    """Per-player pool size; what is left over is not dealt."""
    if players <= 0:
        raise NotEnoughPool("a draft needs at least one player")
    per_player = total // players
    if per_player < 1:
        raise NotEnoughPool(
            f"{total} remain after bans and {players} players need one each"
        )
    return per_player


def deal(
    tokens: Sequence[str], players: int, per_player: int, rng: Any = _RANDOM
) -> list[list[str]]:
    """One disjoint pool per player, each of `per_player`."""
    if per_player * players > len(tokens):
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


def leader_pools(
    tokens: Sequence[str], groups: int, draft_mode: str, rng: Any = _RANDOM
) -> list[list[str]]:
    """A draft's leader pools: random gives each one leader, blind up to a
    menu's worth, standard an even split of what is left."""
    if draft_mode == "random":
        return [[token] for token in assign_one_each(tokens, groups, rng)]
    share = even_split(len(tokens), groups)
    size = min(share, BLIND_POOL) if draft_mode == "blind" else share
    return deal(tokens, groups, size, rng)


def remaining_after_bans(tokens: Sequence[str], banned: Sequence[str]) -> list[str]:
    """The draftable set, in the catalogue's own order."""
    refused = set(banned)
    return [token for token in tokens if token not in refused]


__all__ = [
    "BLIND_POOL",
    "NotEnoughPool",
    "assign_one_each",
    "civ_target",
    "deal",
    "deal_civs",
    "even_split",
    "leader_pools",
    "remaining_after_bans",
]
