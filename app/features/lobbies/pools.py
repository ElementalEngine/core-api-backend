"""Dealing draft pools from the leaders and civs left after bans.

Every player gets the same number of options and the remainder is discarded:
forty-four leaders across five players deals eight each and drops four. An
uneven split would give somebody a wider choice than the rest.

Deals use system randomness and are not seeded. A pool dealt from a
reproducible seed could be worked out in advance by anyone holding the lobby
id, which would defeat the blind draft. The dealt pools stored on the seats
are the record, not the seed.

Governed by D197, D198.
"""

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


def deal(tokens: Sequence[str], players: int, rng: Any = _RANDOM) -> list[list[str]]:
    """One disjoint pool per player, every pool the same size."""
    per_player, _ = even_split(len(tokens), players)
    shuffled = list(tokens)
    rng.shuffle(shuffled)
    return [
        shuffled[index * per_player : (index + 1) * per_player]
        for index in range(players)
    ]


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
    "deal",
    "even_split",
    "remaining_after_bans",
]
