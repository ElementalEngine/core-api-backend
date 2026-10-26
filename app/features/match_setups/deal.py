"""/draft's deal: pools from what is left after the host's bans."""

from __future__ import annotations

from collections.abc import Mapping
from random import SystemRandom
from typing import Any

from app.features.match_setups.bans import ban_caps, bannable_civs
from app.features.match_setups.pools import (
    NotEnoughPool,
    deal,
    deal_civs,
    remaining_after_bans,
)
from app.features.match_setups.schemas import DraftRequest

_RANDOM = SystemRandom()


class DraftRefused(ValueError):
    """A draft that cannot be dealt, said so the host can fix it."""


def checked_bans(
    asked: list[str], allowed: list[str], cap: int, kind: str
) -> list[str]:
    """The bans asked for, once each, refused if unknown or over the cap."""
    bans = list(dict.fromkeys(asked))
    unknown = [token for token in bans if token not in set(allowed)]
    if unknown:
        raise DraftRefused(f"These cannot be banned here: {', '.join(unknown)}.")
    if len(bans) > cap:
        raise DraftRefused(f"At most {cap} {kind} bans; {len(bans)} were chosen.")
    return bans


def deal_draft(
    civ_data: Mapping[str, Any], request: DraftRequest, rng: Any = _RANDOM
) -> dict[str, Any]:
    """One pool per draft: leaders never repeat, civ7 civs only when too few."""
    caps = ban_caps(request.edition, request.starting_age)
    leaders = [row["token"] for row in civ_data["leaders"]]
    civs = bannable_civs(civ_data["civs"], request.starting_age)
    leader_bans = checked_bans(request.leader_bans, leaders, caps["leader"], "leader")
    civ_bans = checked_bans(request.civ_bans, civs, caps["civ"], "civ")
    try:
        leader_pools = deal(
            remaining_after_bans(leaders, leader_bans),
            request.drafts,
            rng,
            per_player=request.leaders,
        )
        civ_pools: list[list[str]] = [[] for _ in leader_pools]
        if request.civs is not None:
            civ_pools = deal_civs(
                remaining_after_bans(civs, civ_bans), request.drafts, request.civs, rng
            )
    except NotEnoughPool as exc:
        raise DraftRefused(f"Not enough to deal: {exc}.") from exc
    return {
        "pools": [
            {"leaders": mine, "civs": theirs}
            for mine, theirs in zip(leader_pools, civ_pools, strict=True)
        ],
        "bans": {"leader": leader_bans, "civ": civ_bans},
    }


__all__ = ["DraftRefused", "checked_bans", "deal_draft"]
