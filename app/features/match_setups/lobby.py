"""A hosted lobby's rules: its shapes, who sits where, its vote, and when it
drafts. Pure functions over the stored document; the router writes what they
return."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from random import SystemRandom
from typing import Any

from app.features.match_setups.bans import ban_caps, voted_bans
from app.features.match_setups.pools import (
    NotEnoughPool,
    assign_one_each,
    deal,
    remaining_after_bans,
)
from app.features.match_setups.tally import voted_settings

_RANDOM = SystemRandom()

OPEN = "open"
DONE = "done"
CANCELLED = "cancelled"
IDLE_LIMIT = timedelta(hours=2, minutes=30)
VOTE_LIMIT = timedelta(minutes=10)
STEAM_LINK = re.compile(r"steam://joinlobby/289070/[0-9]+/7656119[0-9]+")


@dataclass(frozen=True)
class Shape:
    mode: str
    seats: int
    # 0 for every player on their own.
    teams: int = 0


SHAPES: dict[str, Shape] = {
    "ffa-8": Shape("ffa", 8),
    "ffa-10": Shape("ffa", 10),
    "ffa-12": Shape("ffa", 12),
    "ffa-14": Shape("ffa", 14),
    "duel": Shape("duel", 2),
    "teamer-2v2": Shape("teamer", 4, 2),
    "teamer-3v3": Shape("teamer", 6, 2),
    "teamer-4v4": Shape("teamer", 8, 2),
    "teamer-5v5": Shape("teamer", 10, 2),
    "teamer-2v2v2v2": Shape("teamer", 8, 4),
}


class LobbyRefused(ValueError):
    """A change the lobby's state or rules do not allow, said for the player."""


class NotTheHost(LobbyRefused):
    """A change only the host may make."""


def new_lobby(
    *,
    shape: str,
    draft_mode: str,
    host_id: str,
    guild_id: str,
    channel_id: str,
    steam_link: str | None,
    now: datetime,
) -> dict[str, Any]:
    """A civ6 lobby in setup, its host in the first seat. Players join once the
    host makes it live."""
    if steam_link is not None and not STEAM_LINK.fullmatch(steam_link):
        raise LobbyRefused("the Steam link is not a Civ6 lobby link")
    chosen = SHAPES[shape]
    return {
        "command": "lobby",
        "edition": "civ6",
        "mode": chosen.mode,
        "shape": shape,
        "seats": chosen.seats,
        "teams": chosen.teams,
        "draft_mode": draft_mode,
        "status": OPEN,
        "live": False,
        "host_id": host_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "message_id": None,
        "steam_link": steam_link,
        "players": [{"discord_id": host_id, "team": 1 if chosen.teams else None}],
        "settings": [],
        "bans": [],
        "pools": [],
        "vote": None,
        "cancel_reason": None,
        "revision": 1,
        "created_at": now,
        "updated_at": now,
    }


def _open(lobby: Mapping[str, Any]) -> None:
    if lobby["status"] != OPEN:
        raise LobbyRefused("the lobby is closed")


def _host(lobby: Mapping[str, Any], actor_id: str) -> None:
    _open(lobby)
    if actor_id != lobby["host_id"]:
        raise NotTheHost("only the host can do that")


def _live(lobby: Mapping[str, Any]) -> None:
    _open(lobby)
    if not lobby["live"]:
        raise LobbyRefused("the lobby is not open to players yet")


def _unlocked(lobby: Mapping[str, Any]) -> None:
    if lobby.get("vote"):
        raise LobbyRefused("the lobby is voting, so its seats are locked")


def _voting(lobby: Mapping[str, Any]) -> Mapping[str, Any]:
    _open(lobby)
    vote: Mapping[str, Any] | None = lobby.get("vote")
    if not vote:
        raise LobbyRefused("the lobby is not voting")
    return vote


def went_live(
    lobby: Mapping[str, Any], actor_id: str, message_id: str
) -> dict[str, Any]:
    """The host confirms the setup; the lobby's post opens it to players."""
    _host(lobby, actor_id)
    if lobby["live"]:
        raise LobbyRefused("the lobby is already open")
    return {"live": True, "message_id": message_id}


def team_sizes(lobby: Mapping[str, Any]) -> list[int]:
    """Players per team, team 1 first."""
    sizes = [0] * lobby["teams"]
    for player in lobby["players"]:
        sizes[player["team"] - 1] += 1
    return sizes


def joined(lobby: Mapping[str, Any], player_id: str) -> list[dict[str, Any]]:
    """The players once this one sits down, on the emptiest team in a teamer."""
    _live(lobby)
    _unlocked(lobby)
    players = list(lobby["players"])
    if any(p["discord_id"] == player_id for p in players):
        raise LobbyRefused("you are already in this lobby")
    if len(players) >= lobby["seats"]:
        raise LobbyRefused("the lobby is full")
    team = None
    if lobby["teams"]:
        sizes = team_sizes(lobby)
        team = sizes.index(min(sizes)) + 1
    return [*players, {"discord_id": player_id, "team": team}]


def left(lobby: Mapping[str, Any], player_id: str) -> list[dict[str, Any]]:
    """The players once this one stands up. The host cancels instead."""
    _open(lobby)
    _unlocked(lobby)
    if player_id == lobby["host_id"]:
        raise LobbyRefused("the host cancels the lobby rather than leaving it")
    players = [p for p in lobby["players"] if p["discord_id"] != player_id]
    if len(players) == len(lobby["players"]):
        raise LobbyRefused("you are not in this lobby")
    return players


def edited(
    lobby: Mapping[str, Any],
    actor_id: str,
    *,
    shape: str,
    questions: Sequence[Mapping[str, Any]],
    settings: Mapping[str, str],
    bans: Sequence[str],
    teams: Mapping[str, int],
    removed: Sequence[str],
) -> dict[str, Any]:
    """The host's edit as one change: the size, the settings they fix (the rest
    are voted), the leaders they ban, and players moved or removed. Players who
    joined meanwhile keep their seats."""
    _host(lobby, actor_id)
    _unlocked(lobby)
    chosen, current = SHAPES[shape], SHAPES[lobby["shape"]]
    if (chosen.mode, chosen.teams) != (current.mode, current.teams):
        raise LobbyRefused("a lobby changes its size, not its mode")
    if lobby["host_id"] in removed:
        raise LobbyRefused("the host cannot be removed")
    seated = {p["discord_id"] for p in lobby["players"]}
    if (set(removed) | set(teams)) - seated:
        raise LobbyRefused("a player you changed has left the lobby")
    if teams and not chosen.teams:
        raise LobbyRefused("only a teamer has teams")
    for team in teams.values():
        if not 1 <= team <= chosen.teams:
            raise LobbyRefused(f"there is no team {team}")
    players = [
        {**p, "team": teams.get(p["discord_id"], p["team"])}
        for p in lobby["players"]
        if p["discord_id"] not in removed
    ]
    if len(players) > chosen.seats:
        raise LobbyRefused(f"{len(players)} players do not fit in {chosen.seats} seats")
    if chosen.teams:
        size = chosen.seats // chosen.teams
        for team, count in enumerate(team_sizes({**lobby, "players": players}), 1):
            if count > size:
                raise LobbyRefused(f"team {team} has more than {size} players")
    options = {q["id"]: {o["id"] for o in q["options"]} for q in questions}
    for question, option in settings.items():
        if option not in options.get(question, ()):
            raise LobbyRefused(f"{question} has no option {option}")
    return {
        "shape": shape,
        "seats": chosen.seats,
        "players": players,
        "settings": [
            {"id": q["id"], "value": settings[q["id"]], "by": "host"}
            for q in questions
            if q["id"] in settings
        ],
        "bans": [
            {"token": token, "kind": "leader", "by": "host"}
            for token in dict.fromkeys(bans)
        ],
    }


def draft_groups(lobby: Mapping[str, Any], actor_id: str) -> list[list[str]]:
    """Who drafts together: each player alone, or each team. Refused until the
    lobby can start: two duellists, two players, or equal teams."""
    _host(lobby, actor_id)
    _live(lobby)
    players = lobby["players"]
    if lobby["mode"] == "duel" and len(players) != 2:
        raise LobbyRefused("a duel starts with two players")
    if len(players) < 2:
        raise LobbyRefused("the lobby needs at least two players")
    if not lobby["teams"]:
        return [[p["discord_id"]] for p in players]
    sizes = team_sizes(lobby)
    if len(set(sizes)) != 1:
        raise LobbyRefused("the teams must be the same size")
    return [
        [p["discord_id"] for p in players if p["team"] == team]
        for team in range(1, lobby["teams"] + 1)
    ]


def vote_opened(
    lobby: Mapping[str, Any], actor_id: str, now: datetime
) -> dict[str, Any]:
    """The host starts: the seats lock while the players vote on the settings
    left open and on more bans, until all finish or the time is up."""
    draft_groups(lobby, actor_id)
    _unlocked(lobby)
    return {
        "vote": {
            "ends_at": now + VOTE_LIMIT,
            "channel_id": None,
            "messages": {},
            "finished": [],
        }
    }


def vote_placed(
    lobby: Mapping[str, Any],
    actor_id: str,
    channel_id: str,
    messages: Mapping[str, str],
) -> dict[str, Any]:
    """Where the vote runs, so it can be counted from there."""
    _host(lobby, actor_id)
    vote = _voting(lobby)
    if vote["channel_id"]:
        raise LobbyRefused("the vote is already posted")
    return {"vote": {**vote, "channel_id": channel_id, "messages": dict(messages)}}


def finished(lobby: Mapping[str, Any], player_id: str) -> dict[str, Any]:
    """A player is done voting."""
    vote = _voting(lobby)
    if all(p["discord_id"] != player_id for p in lobby["players"]):
        raise LobbyRefused("only the lobby's players vote")
    if player_id in vote["finished"]:
        raise LobbyRefused("you have already finished")
    return {"vote": {**vote, "finished": [*vote["finished"], player_id]}}


def tallied(
    lobby: Mapping[str, Any],
    actor_id: str,
    *,
    questions: Sequence[Mapping[str, Any]],
    settings_votes: Mapping[str, Mapping[str, Sequence[str]]],
    ban_votes: Mapping[str, Sequence[str]],
    leaders: Sequence[str],
    rng: Any = _RANDOM,
) -> dict[str, Any]:
    """The vote counted and the lobby drafted. Only players' votes count: the
    settings left open go to the most chosen, and more leader bans land, as
    many as the cap leaves room for."""
    groups = draft_groups(lobby, actor_id)
    _voting(lobby)
    voters = {p["discord_id"] for p in lobby["players"]}
    fixed = {s["id"]: s for s in lobby["settings"]}
    won = voted_settings(
        [q for q in questions if q["id"] not in fixed],
        settings_votes,
        voters,
        str(lobby["_id"]),
    )
    host_bans = {b["token"] for b in lobby["bans"]}
    room = ban_caps(lobby["edition"], None)["leader"] - len(host_bans)
    banned = voted_bans(
        ban_votes, voters, [t for t in leaders if t not in host_bans], room
    )
    bans = [
        *lobby["bans"],
        *({"token": token, "kind": "leader", "by": "vote"} for token in banned),
    ]
    return {
        "settings": [
            fixed.get(q["id"]) or {"id": q["id"], "value": won[q["id"]], "by": "vote"}
            for q in questions
        ],
        "bans": bans,
        "pools": dealt({**lobby, "bans": bans}, groups, leaders, rng),
        "status": DONE,
    }


def dealt(
    lobby: Mapping[str, Any],
    teams: Sequence[Sequence[str]],
    leaders: Sequence[str],
    rng: Any = _RANDOM,
) -> list[dict[str, Any]]:
    """The draft: standard deals each player or team an even share of the
    leaders left after bans; random gives each player one."""
    remaining = remaining_after_bans(leaders, [b["token"] for b in lobby["bans"]])
    on_team = bool(lobby["teams"])
    try:
        if lobby["draft_mode"] == "random":
            players = [p for team in teams for p in team]
            picks = assign_one_each(remaining, len(players), rng)
            team_of = {p: i + 1 for i, team in enumerate(teams) for p in team}
            return [
                {
                    "player_id": player,
                    "team": team_of[player] if on_team else None,
                    "leaders": [pick],
                }
                for player, pick in zip(players, picks, strict=True)
            ]
        pools = deal(remaining, len(teams), rng)
    except NotEnoughPool as exc:
        raise LobbyRefused(f"not enough leaders to deal: {exc}") from exc
    return [
        {
            "player_id": None if on_team else team[0],
            "team": i + 1 if on_team else None,
            "leaders": pool,
        }
        for i, (team, pool) in enumerate(zip(teams, pools, strict=True))
    ]


def cancel_reason(lobby: Mapping[str, Any], actor_id: str, staff: bool) -> str:
    """Who closed it: the host, or staff on the host's behalf."""
    _open(lobby)
    if actor_id == lobby["host_id"]:
        return "host"
    if staff:
        return "staff"
    raise NotTheHost("only the host or staff can cancel the lobby")


__all__ = [
    "CANCELLED",
    "DONE",
    "IDLE_LIMIT",
    "OPEN",
    "SHAPES",
    "VOTE_LIMIT",
    "LobbyRefused",
    "NotTheHost",
    "Shape",
    "cancel_reason",
    "dealt",
    "draft_groups",
    "edited",
    "finished",
    "joined",
    "left",
    "new_lobby",
    "tallied",
    "team_sizes",
    "vote_opened",
    "vote_placed",
    "went_live",
]
