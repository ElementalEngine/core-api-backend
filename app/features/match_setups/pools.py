"""Deals draft pools from what is left after bans. Every pool is the same size,
each leader type is spread across the pools as evenly as they allow, and deals
are unseeded so nobody can work out a blind pool in advance."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from random import SystemRandom
from typing import Any

logger = logging.getLogger(__name__)
_RANDOM = SystemRandom()
# A blind pool is picked from one menu, which holds 25.
BLIND_POOL = 25
NAVAL, WAR, UNTYPED = "Naval", "War", "None"
# Naval is spread first and war last; these go between, in a fresh order each deal.
BETWEEN = ("Religious", "Culture", "Science", "Industrial", UNTYPED)
LEADER_TYPES = (NAVAL, *BETWEEN, WAR)


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


def _shares(
    counts: Mapping[str, int], slots: int, order: Sequence[str]
) -> dict[str, int]:
    """Each type's slots, in proportion to what is left of it. The largest
    remainders take the slots rounding leaves, the earlier type on a tie."""
    total = sum(counts.values())
    shares = {kind: slots * counts[kind] // total for kind in order}
    spare = slots - sum(shares.values())
    by_remainder = sorted(order, key=lambda kind: -(slots * counts[kind] % total))
    for kind in by_remainder[:spare]:
        shares[kind] += 1
    return shares


def deal(
    tokens: Sequence[str],
    players: int,
    per_pool: int,
    types: Mapping[str, str],
    rng: Any = _RANDOM,
) -> list[list[str]]:
    """One disjoint pool per player, each of `per_pool`, every leader type
    spread across them to within one. While enough are left, pools of more than
    one get a naval leader each, its slots taken from war first."""
    if players < 1 or per_pool < 1:
        raise NotEnoughPool("a deal needs at least one pool of at least one")
    if per_pool * players > len(tokens):
        raise NotEnoughPool(
            f"{len(tokens)} remain after bans and {players} pools of "
            f"{per_pool} need {per_pool * players}"
        )
    order = (NAVAL, *rng.sample(BETWEEN, len(BETWEEN)), WAR)
    buckets: dict[str, list[str]] = {kind: [] for kind in order}
    for token in tokens:
        buckets.get(types.get(token, UNTYPED), buckets[UNTYPED]).append(token)
    counts = {kind: len(bucket) for kind, bucket in buckets.items()}
    shares = _shares(counts, players * per_pool, order)
    if per_pool > 1:
        wanted = min(players, counts[NAVAL])
        for kind in reversed(order[1:]):
            moved = min(shares[kind], max(wanted - shares[NAVAL], 0))
            shares[kind] -= moved
            shares[NAVAL] += moved
    dealt: list[str] = []
    for kind in order:
        rng.shuffle(buckets[kind])
        dealt += buckets[kind][: shares[kind]]
    # Dealt round the pools in type order, so each type differs by at most one.
    seats = rng.sample(range(players), players)
    pools: list[list[str]] = [[] for _ in range(players)]
    for index, token in enumerate(dealt):
        pools[seats[index % players]].append(token)
    for pool in pools:
        rng.shuffle(pool)
    logger.info(
        "Leader pools dealt. pools=%s size=%s naval_in=%s types=%s",
        players,
        per_pool,
        min(shares[NAVAL], players),
        " ".join(f"{kind}={shares[kind]}" for kind in LEADER_TYPES),
    )
    return pools


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


def leader_pools(
    tokens: Sequence[str],
    groups: int,
    draft_mode: str,
    types: Mapping[str, str],
    rng: Any = _RANDOM,
) -> list[list[str]]:
    """A draft's leader pools: random gives each one leader, blind up to a
    menu's worth, standard an even split of what is left."""
    if draft_mode == "random":
        return deal(tokens, groups, 1, types, rng)
    share = even_split(len(tokens), groups)
    size = min(share, BLIND_POOL) if draft_mode == "blind" else share
    return deal(tokens, groups, size, types, rng)


def remaining_after_bans(tokens: Sequence[str], banned: Sequence[str]) -> list[str]:
    """The draftable set, in the catalogue's own order."""
    refused = set(banned)
    return [token for token in tokens if token not in refused]


__all__ = [
    "BLIND_POOL",
    "LEADER_TYPES",
    "NAVAL",
    "NotEnoughPool",
    "civ_target",
    "deal",
    "deal_civs",
    "even_split",
    "leader_pools",
    "remaining_after_bans",
]
