"""Dealing draft pools from the post-ban set.

⚠ **Deals are unseeded, and that is the opposite of the tie-break on purpose
(D197).** D191 seeds ties on `sha256(lobby_id:question_id)` so a disputed
result recomputes forever. A pool deal must never be reproducible from the
lobby id: anyone holding it could compute a blind pool before it is dealt,
which is the whole of blind. The record is the stored `seats[].pool`, not a
seed anybody can replay.

⚠ **The pool is truncated so every player gets the same count**, carried from
Mite's `buildUniformTargets`: forty-four leaders across five players deals
eight each and DISCARDS four. An uneven split would hand somebody a wider
choice, which matters more than four leaders do.
"""

from __future__ import annotations

from collections.abc import Sequence
from random import SystemRandom
from typing import Any

_RANDOM = SystemRandom()


class NotEnoughPool(ValueError):
    """The post-ban set cannot cover what the draft needs."""


def even_split(total: int, players: int) -> tuple[int, int]:
    """Per-player pool size, and how many entries fall off the end.

    Raises NotEnoughPool when a player would get nothing -- a draft where
    somebody has an empty pool is not a draft, and failing here is far
    cheaper than failing at the pick.
    """
    if players <= 0:
        raise NotEnoughPool("a draft needs at least one player")
    per_player = total // players
    if per_player < 1:
        raise NotEnoughPool(
            f"{total} remain after bans and {players} players need one each"
        )
    return per_player, total % players


def deal(tokens: Sequence[str], players: int, rng: Any = _RANDOM) -> list[list[str]]:
    """One disjoint pool per player, every pool the same size.

    ⚠ Disjoint by construction rather than by a check: the shuffled list is
    consumed from the front, so no token can reach two players even if the
    sizing arithmetic is wrong.
    """
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
    """One token per player, all distinct -- `random` mode's whole draft.

    ⚠ Mite samples WITH REPLACEMENT when the pool is smaller than the player
    count, handing two players the same leader rather than failing. That
    cannot arise under the current ban caps, and a silent duplicate is worse
    than a refusal, so this raises instead.
    """
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
