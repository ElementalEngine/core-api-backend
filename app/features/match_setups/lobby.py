"""A hosted lobby's rules: its shapes, who sits where, and when it may start.
Pure functions over the stored document; the router writes what they return."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from random import SystemRandom
from typing import Any

from app.features.match_setups.pools import (
    NotEnoughPool,
    assign_one_each,
    deal,
    remaining_after_bans,
)

_RANDOM = SystemRandom()

OPEN = "open"
DONE = "done"
CANCELLED = "cancelled"
IDLE_LIMIT = timedelta(hours=2, minutes=30)
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
    steam_link: str,
    rules: str | None,
    now: datetime,
) -> dict[str, Any]:
    """An open civ6 lobby with its host in the first seat."""
    if not STEAM_LINK.fullmatch(steam_link):
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
        "host_id": host_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "message_id": None,
        "steam_link": steam_link,
        "rules": rules,
        "players": [{"discord_id": host_id, "team": 1 if chosen.teams else None}],
        "settings": [],
        "bans": [],
        "pools": [],
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


def team_sizes(lobby: Mapping[str, Any]) -> list[int]:
    """Players per team, team 1 first."""
    sizes = [0] * lobby["teams"]
    for player in lobby["players"]:
        sizes[player["team"] - 1] += 1
    return sizes


def joined(lobby: Mapping[str, Any], player_id: str) -> list[dict[str, Any]]:
    """The players once this one sits down, on the emptiest team in a teamer."""
    _open(lobby)
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
    if player_id == lobby["host_id"]:
        raise LobbyRefused("the host cancels the lobby rather than leaving it")
    players = [p for p in lobby["players"] if p["discord_id"] != player_id]
    if len(players) == len(lobby["players"]):
        raise LobbyRefused("you are not in this lobby")
    return players


def moved(
    lobby: Mapping[str, Any], actor_id: str, player_id: str, team: int
) -> list[dict[str, Any]]:
    """The players once the host puts one on another team."""
    _host(lobby, actor_id)
    if not lobby["teams"]:
        raise LobbyRefused("only a teamer has teams")
    if not 1 <= team <= lobby["teams"]:
        raise LobbyRefused(f"there is no team {team}")
    player = next((p for p in lobby["players"] if p["discord_id"] == player_id), None)
    if player is None:
        raise LobbyRefused("that player is not in this lobby")
    if player["team"] == team:
        raise LobbyRefused(f"that player is already on team {team}")
    if team_sizes(lobby)[team - 1] >= lobby["seats"] // lobby["teams"]:
        raise LobbyRefused(f"team {team} is full")
    return [
        {**p, "team": team} if p["discord_id"] == player_id else p
        for p in lobby["players"]
    ]


def host_settings(
    lobby: Mapping[str, Any],
    actor_id: str,
    questions: Sequence[Mapping[str, Any]],
    chosen: Mapping[str, str],
) -> list[dict[str, str]]:
    """The settings the host fixes, in the catalogue's order; the rest are voted."""
    _host(lobby, actor_id)
    options = {q["id"]: {o["id"] for o in q["options"]} for q in questions}
    for question, option in chosen.items():
        if option not in options.get(question, ()):
            raise LobbyRefused(f"{question} has no option {option}")
    return [
        {"id": q["id"], "value": chosen[q["id"]], "by": "host"}
        for q in questions
        if q["id"] in chosen
    ]


def host_bans(
    lobby: Mapping[str, Any], actor_id: str, leaders: Sequence[str]
) -> list[dict[str, str]]:
    """The leaders the host bans; the router checks them against civ data."""
    _host(lobby, actor_id)
    return [
        {"token": token, "kind": "leader", "by": "host"}
        for token in dict.fromkeys(leaders)
    ]


def draft_groups(lobby: Mapping[str, Any], actor_id: str) -> list[list[str]]:
    """Who drafts together: each player alone, or each team. Refused until the
    lobby can start: two duellists, two players, or equal teams."""
    _host(lobby, actor_id)
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
    "LobbyRefused",
    "NotTheHost",
    "Shape",
    "cancel_reason",
    "dealt",
    "draft_groups",
    "host_bans",
    "host_settings",
    "joined",
    "left",
    "moved",
    "new_lobby",
    "team_sizes",
]
