"""A /vote's rules: players from a voice channel vote by reaction on every
setting, the draft mode and bans, then the draft is dealt. Pure functions over
the stored document; the router writes what they return."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from app.features.match_setups.lobby import (
    BLIND_POOL,
    DONE,
    OPEN,
    PICK_LIMIT,
    PICKING,
    VOTING,
    LobbyRefused,
    require_host,
    require_phase,
    with_voted_bans,
)
from app.features.match_setups.pools import (
    NotEnoughPool,
    assign_one_each,
    civ_target,
    deal,
    deal_civs,
    even_split,
    remaining_after_bans,
)

VOTE_LIMIT = timedelta(minutes=10)
MAX_PLAYERS = 14
TEAMS = 2


def new_vote(
    *,
    edition: str,
    mode: str,
    age: str | None,
    host_id: str,
    guild_id: str,
    channel_id: str,
    players: Sequence[str],
    bans: Sequence[Mapping[str, str]],
    code: str,
    now: datetime,
) -> dict[str, Any]:
    """A vote open at once for ten minutes; the bans its starter chose stand,
    and the players vote on everything else."""
    voters = list(dict.fromkeys(players))
    if not 2 <= len(voters) <= MAX_PLAYERS:
        raise LobbyRefused(f"a vote needs 2 to {MAX_PLAYERS} players")
    if mode == "duel" and len(voters) != 2:
        raise LobbyRefused("a duel is two players")
    if mode == "teamer" and len(voters) < 2 * TEAMS:
        raise LobbyRefused("a teamer is two teams of two or more")
    if edition == "civ6" and age is not None:
        raise LobbyRefused("civ6 has no starting age")
    return {
        "command": "vote",
        "edition": edition,
        "mode": mode,
        "teams": TEAMS if mode == "teamer" else 0,
        "age": age,
        "draft_mode": None,
        "status": OPEN,
        "phase": VOTING,
        "code": code,
        "host_id": host_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "players": [{"discord_id": voter, "team": None} for voter in voters],
        "settings": [],
        "bans": [{**ban, "by": "host"} for ban in bans],
        "pools": [],
        "picks": [],
        "vote": {"channel_id": None, "messages": {}, "finished": []},
        "deadline": now + VOTE_LIMIT,
        "cancel_reason": None,
        "revision": 1,
        "created_at": now,
        "updated_at": now,
    }


def seeded_index(seed: str, question_id: str, count: int) -> int:
    """The tie-break, fixed by the vote and the question, so it can be checked."""
    digest = hashlib.sha256(f"{seed}:{question_id}".encode()).hexdigest()
    return int(digest[:8], 16) % count


def _backing(
    question: Mapping[str, Any], votes: Mapping[str, Sequence[str]], voters: set[str]
) -> dict[str, int]:
    return {
        o["id"]: len(voters & set(votes.get(o["id"], ()))) for o in question["options"]
    }


def winner(
    question: Mapping[str, Any],
    votes: Mapping[str, Sequence[str]],
    voters: set[str],
    seed: str,
) -> str:
    """The option most players reacted for. A tie, none reacting included, goes
    to one of the tied options by a fixed hash."""
    counts = _backing(question, votes, voters)
    most = max(counts.values(), default=0)
    tied = sorted(option for option, count in counts.items() if count == most)
    return tied[seeded_index(seed, question["id"], len(tied))]


def settled(
    vote: Mapping[str, Any],
    setting_votes: Mapping[str, Mapping[str, Sequence[str]]],
    questions: Sequence[Mapping[str, Any]],
    *,
    final: bool,
) -> list[dict[str, Any]]:
    """The settings won so far: one an option has a majority of the players
    for is won at once and stays won. The final count settles the rest."""
    voters = {p["discord_id"] for p in vote["players"]}
    need = len(voters) // 2 + 1
    chosen = {s["id"]: s for s in vote["settings"]}
    for q in questions:
        votes = setting_votes.get(q["id"], {})
        won = max(_backing(q, votes, voters).values(), default=0) >= need
        if q["id"] not in chosen and (final or won):
            value = winner(q, votes, voters, str(vote["_id"]))
            chosen[q["id"]] = {"id": q["id"], "value": value, "by": "vote"}
    return [chosen[q["id"]] for q in questions if q["id"] in chosen]


def _pools(
    vote: Mapping[str, Any],
    draft_mode: str,
    leaders: Sequence[str],
    civs: Sequence[str],
    rng: Any,
) -> list[dict[str, Any]]:
    """The draft: random gives each player one leader (and civ); blind deals
    each player a menu's worth; standard splits what is left between the
    players, or the teams. Civ7 civs go to a fixed target, shared when few."""
    banned = {
        kind: [b["token"] for b in vote["bans"] if b["kind"] == kind]
        for kind in ("leader", "civ")
    }
    leaders_left = remaining_after_bans(leaders, banned["leader"])
    civs_left = remaining_after_bans(civs, banned["civ"])
    players = [p["discord_id"] for p in vote["players"]]
    on_team = draft_mode == "standard" and vote["mode"] == "teamer"
    groups = vote["teams"] if on_team else len(players)

    def civ_pools(target: int) -> list[list[str]]:
        return (
            deal_civs(civs_left, groups, target, rng)
            if civs
            else [[] for _ in range(groups)]
        )

    try:
        if draft_mode == "random":
            picked = assign_one_each(leaders_left, groups, rng)
            return [
                {"player_id": player, "team": None, "leaders": [leader], "civs": civ}
                for player, leader, civ in zip(
                    players, picked, civ_pools(1), strict=True
                )
            ]
        share, _ = even_split(len(leaders_left), groups)
        size = min(share, BLIND_POOL) if draft_mode == "blind" else share
        target = min(civ_target("teamer" if on_team else "ffa", groups), BLIND_POOL)
        dealt = zip(
            deal(leaders_left, groups, rng, per_player=size),
            civ_pools(target),
            strict=True,
        )
    except NotEnoughPool as exc:
        raise LobbyRefused(f"not enough to deal: {exc}") from exc
    return [
        {
            "player_id": None if on_team else players[i],
            "team": i + 1 if on_team else None,
            "leaders": leader_pool,
            "civs": civ_pool,
        }
        for i, (leader_pool, civ_pool) in enumerate(dealt)
    ]


def _bannable(leaders: Sequence[str], civs: Sequence[str]) -> dict[str, Sequence[str]]:
    return {"leader": leaders, "civ": civs} if civs else {"leader": leaders}


def vote_counted(
    vote: Mapping[str, Any],
    actor_id: str,
    *,
    setting_votes: Mapping[str, Mapping[str, Sequence[str]]],
    ban_votes: Mapping[str, Sequence[str]],
    questions: Sequence[Mapping[str, Any]],
    leaders: Sequence[str],
    civs: Sequence[str],
) -> dict[str, Any]:
    """A running count while the players vote: a setting option or a ban
    reaching a majority is won at once, and stays won."""
    require_host(vote, actor_id)
    require_phase(vote, VOTING)
    return {
        "settings": settled(vote, setting_votes, questions, final=False),
        "bans": with_voted_bans(vote, ban_votes, _bannable(leaders, civs)),
    }


def vote_tallied(
    vote: Mapping[str, Any],
    actor_id: str,
    *,
    setting_votes: Mapping[str, Mapping[str, Sequence[str]]],
    ban_votes: Mapping[str, Sequence[str]],
    questions: Sequence[Mapping[str, Any]],
    leaders: Sequence[str],
    civs: Sequence[str],
    now: datetime,
    rng: Any,
) -> dict[str, Any]:
    """The vote counted: each setting not yet won goes to the most reactions,
    the bans a majority backs land within the caps, and the draft is dealt. A
    blind draft then has its players pick; any other is done."""
    require_host(vote, actor_id)
    require_phase(vote, VOTING)
    settings = settled(vote, setting_votes, questions, final=True)
    draft_mode = next(s["value"] for s in settings if s["id"] == "draft_mode")
    bans = with_voted_bans(vote, ban_votes, _bannable(leaders, civs))
    changes = {
        "settings": settings,
        "draft_mode": draft_mode,
        "bans": bans,
        "pools": _pools({**vote, "bans": bans}, draft_mode, leaders, civs, rng),
    }
    if draft_mode == "blind":
        return {**changes, "phase": PICKING, "deadline": now + PICK_LIMIT}
    return {**changes, "status": DONE, "deadline": None}


__all__ = [
    "VOTE_LIMIT",
    "new_vote",
    "seeded_index",
    "settled",
    "vote_counted",
    "vote_tallied",
    "winner",
]
