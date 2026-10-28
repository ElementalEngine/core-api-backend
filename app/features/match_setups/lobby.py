"""A hosted lobby's rules: its shapes and phases, who sits where, the bans vote
and the draft. Pure functions over the stored document; the router writes
what they return."""

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
    even_split,
    remaining_after_bans,
)

_RANDOM = SystemRandom()

OPEN = "open"
DONE = "done"
CANCELLED = "cancelled"

# An open lobby is set up by its host, then live for players to join; it then
# votes on more bans, and a blind draft has its players pick.
SETUP = "setup"
LIVE = "live"
VOTING = "voting"
PICKING = "picking"

IDLE_LIMIT = timedelta(hours=2, minutes=30)
# A lobby still in setup has outlived its editor by then, so it frees its host.
SETUP_IDLE_LIMIT = timedelta(minutes=15)
VOTE_LIMIT = timedelta(minutes=3)
# A full lobby waits this long before its vote, so players can check everyone
# is there; the host may start sooner.
FILL_WAIT = timedelta(minutes=3)
PICK_LIMIT = timedelta(minutes=3)
# A blind pool is picked from one menu, which holds 25.
BLIND_POOL = 25
STEAM_LINK = re.compile(r"steam://joinlobby/289070/[0-9]+/7656119[0-9]+")
CODE_LETTERS = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"
CODE_LENGTH = 6


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
    "teamer-2v2v2": Shape("teamer", 6, 3),
    "teamer-2v2v2v2": Shape("teamer", 8, 4),
}


class LobbyRefused(ValueError):
    """A change the lobby's state or rules do not allow, said for the player."""


class NotTheHost(LobbyRefused):
    """A change only the host may make."""


def lobby_code(rng: Any = _RANDOM) -> str:
    """The short code a lobby is cancelled by."""
    return "".join(rng.choice(CODE_LETTERS) for _ in range(CODE_LENGTH))


def new_lobby(
    *,
    shape: str,
    host_id: str,
    guild_id: str,
    channel_id: str,
    steam_link: str | None,
    message: str | None,
    lfg: bool,
    code: str,
    now: datetime,
) -> dict[str, Any]:
    """A civ6 lobby in setup, its host in the first seat and no setting set
    until the host sets each one."""
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
        "draft_mode": None,
        "status": OPEN,
        "phase": SETUP,
        "code": code,
        "host_id": host_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "message_id": None,
        "steam_link": steam_link,
        "message": message,
        "lfg": lfg,
        "players": [{"discord_id": host_id, "team": 1 if chosen.teams else None}],
        "settings": [],
        "bans": [],
        "pools": [],
        "picks": [],
        "vote": None,
        "deadline": None,
        "cancel_reason": None,
        "revision": 1,
        "created_at": now,
        "updated_at": now,
    }


def _noun(setup: Mapping[str, Any]) -> str:
    """What the players call it: a lobby, or a /vote."""
    return "vote" if setup["command"] == "vote" else "lobby"


def _open(lobby: Mapping[str, Any]) -> None:
    if lobby["status"] != OPEN:
        raise LobbyRefused(f"the {_noun(lobby)} is closed")


def require_host(lobby: Mapping[str, Any], actor_id: str) -> None:
    _open(lobby)
    if actor_id != lobby["host_id"]:
        raise NotTheHost("only the host can do that")


def require_phase(lobby: Mapping[str, Any], *phases: str) -> None:
    """Refused unless the open lobby is in one of these phases."""
    _open(lobby)
    if lobby["phase"] in phases:
        return
    if lobby["phase"] == SETUP:
        raise LobbyRefused("the lobby is not open to players yet")
    if LIVE in phases:
        raise LobbyRefused("the lobby has started, so its seats are locked")
    raise LobbyRefused(f"the {_noun(lobby)} is not {phases[0]}")


def went_live(
    lobby: Mapping[str, Any],
    actor_id: str,
    message_id: str,
    questions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """The host confirms the setup, every setting set; the lobby's post opens
    it to players."""
    require_host(lobby, actor_id)
    if lobby["phase"] != SETUP:
        raise LobbyRefused("the lobby is already open")
    chosen = {s["id"] for s in lobby["settings"]}
    unset = [q["title"] for q in questions if q["id"] not in chosen]
    if unset:
        raise LobbyRefused(f"set every setting first: {', '.join(unset)}")
    return {"phase": LIVE, "message_id": message_id}


def team_sizes(lobby: Mapping[str, Any]) -> list[int]:
    """Players per team, team 1 first."""
    sizes = [0] * lobby["teams"]
    for player in lobby["players"]:
        sizes[player["team"] - 1] += 1
    return sizes


def _vote(now: datetime) -> dict[str, Any]:
    return {
        "phase": VOTING,
        "deadline": now + VOTE_LIMIT,
        "vote": {"channel_id": None, "messages": {}, "finished": []},
    }


def _countdown(
    lobby: Mapping[str, Any], players: Sequence[Any], seats: int, now: datetime
) -> datetime | None:
    """When a full live lobby starts its vote: a clock that starts as the last
    seat fills and stops if a seat frees."""
    if len(players) < seats:
        return None
    deadline: datetime | None = lobby["deadline"]
    return deadline or now + FILL_WAIT


def joined(lobby: Mapping[str, Any], player_id: str, now: datetime) -> dict[str, Any]:
    """This player sits down, on the emptiest team in a teamer. The last seat
    filled starts the countdown to the bans vote."""
    require_phase(lobby, LIVE)
    players = list(lobby["players"])
    if any(p["discord_id"] == player_id for p in players):
        raise LobbyRefused("you are already in this lobby")
    if len(players) >= lobby["seats"]:
        raise LobbyRefused("the lobby is full")
    team = None
    if lobby["teams"]:
        sizes = team_sizes(lobby)
        team = sizes.index(min(sizes)) + 1
    players.append({"discord_id": player_id, "team": team})
    return {
        "players": players,
        "deadline": _countdown(lobby, players, lobby["seats"], now),
    }


def left(lobby: Mapping[str, Any], player_id: str) -> dict[str, Any]:
    """This player stands up, stopping any countdown. The host cancels
    instead."""
    require_phase(lobby, LIVE)
    if player_id == lobby["host_id"]:
        raise LobbyRefused("the host cancels the lobby rather than leaving it")
    players = [p for p in lobby["players"] if p["discord_id"] != player_id]
    if len(players) == len(lobby["players"]):
        raise LobbyRefused("you are not in this lobby")
    return {"players": players, "deadline": None}


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
    now: datetime,
) -> dict[str, Any]:
    """The host's edit as one change: the size, the settings (every one, once
    live), the leaders they ban, and players moved or removed. Players who
    joined meanwhile keep their seats; a lobby the edit fills, or frees, starts
    or stops its countdown."""
    require_host(lobby, actor_id)
    require_phase(lobby, SETUP, LIVE)
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
    if lobby["phase"] != SETUP and set(settings) != set(options):
        raise LobbyRefused("every setting needs one value")
    return {
        "shape": shape,
        "seats": chosen.seats,
        "players": players,
        "deadline": _countdown(lobby, players, chosen.seats, now)
        if lobby["phase"] == LIVE
        else None,
        "draft_mode": settings.get("draft_mode"),
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


def _groups(lobby: Mapping[str, Any]) -> list[list[str]]:
    """Who drafts together: each player alone, or each team."""
    players = lobby["players"]
    if not lobby["teams"]:
        return [[p["discord_id"]] for p in players]
    return [
        [p["discord_id"] for p in players if p["team"] == team]
        for team in range(1, lobby["teams"] + 1)
    ]


def started(lobby: Mapping[str, Any], actor_id: str, now: datetime) -> dict[str, Any]:
    """The host starts, or the bot for the host once a full lobby's countdown
    ends: two duellists, two players, or equal teams. The seats lock while the
    players vote on more bans."""
    require_host(lobby, actor_id)
    require_phase(lobby, LIVE)
    players = lobby["players"]
    if lobby["mode"] == "duel" and len(players) != 2:
        raise LobbyRefused("a duel starts with two players")
    if len(players) < 2:
        raise LobbyRefused("the lobby needs at least two players")
    if lobby["teams"] and len(set(team_sizes(lobby))) != 1:
        raise LobbyRefused("the teams must be the same size")
    return _vote(now)


def vote_placed(
    lobby: Mapping[str, Any],
    actor_id: str,
    channel_id: str,
    messages: Mapping[str, str],
) -> dict[str, Any]:
    """Where the vote runs, so it can be counted from there; then where the
    blind picks run, in the same channel."""
    require_host(lobby, actor_id)
    require_phase(lobby, VOTING, PICKING)
    vote = lobby["vote"]
    if lobby["phase"] == VOTING and vote["channel_id"]:
        raise LobbyRefused("the vote is already posted")
    if lobby["phase"] == PICKING and channel_id != vote["channel_id"]:
        raise LobbyRefused("the picks run where the vote did")
    return {"vote": {**vote, "channel_id": channel_id, "messages": dict(messages)}}


def finished(lobby: Mapping[str, Any], player_id: str) -> dict[str, Any]:
    """A player is done voting."""
    require_phase(lobby, VOTING)
    vote = lobby["vote"]
    if all(p["discord_id"] != player_id for p in lobby["players"]):
        raise LobbyRefused(f"you are not a player in this {_noun(lobby)}")
    if player_id in vote["finished"]:
        raise LobbyRefused("you have already finished")
    return {"vote": {**vote, "finished": [*vote["finished"], player_id]}}


def dealt(
    lobby: Mapping[str, Any],
    teams: Sequence[Sequence[str]],
    leaders: Sequence[str],
    rng: Any = _RANDOM,
) -> list[dict[str, Any]]:
    """The draft: standard deals each player or team an even share of the
    leaders left after bans, blind deals each player up to a menu's worth, and
    random gives each player one."""
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
        share, _ = even_split(len(remaining), len(teams))
        size = min(share, BLIND_POOL) if lobby["draft_mode"] == "blind" else share
        pools = deal(remaining, len(teams), rng, per_player=size)
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


def with_voted_bans(
    lobby: Mapping[str, Any],
    ban_votes: Mapping[str, Sequence[str]],
    allowed: Mapping[str, Sequence[str]],
) -> list[dict[str, Any]]:
    """The bans, with each leader or civ a majority of the players vote for
    added, as many as each kind's cap leaves room for. A ban already placed
    stays."""
    voters = {p["discord_id"] for p in lobby["players"]}
    caps = ban_caps(lobby["edition"], lobby.get("age"))
    bans = list(lobby["bans"])
    for kind, tokens in allowed.items():
        placed = {b["token"] for b in bans if b["kind"] == kind}
        room = caps[kind] - len(placed)
        open_to_vote = [t for t in tokens if t not in placed]
        for token in voted_bans(ban_votes, voters, open_to_vote, room):
            bans.append({"token": token, "kind": kind, "by": "vote"})
    return bans


def ballot_counted(
    lobby: Mapping[str, Any],
    actor_id: str,
    *,
    ban_votes: Mapping[str, Sequence[str]],
    leaders: Sequence[str],
) -> dict[str, Any]:
    """A running count while the players vote: a leader reaching a majority is
    banned at once, and stays banned if a vote is then taken back."""
    require_host(lobby, actor_id)
    require_phase(lobby, VOTING)
    return {"bans": with_voted_bans(lobby, ban_votes, {"leader": leaders})}


def tallied(
    lobby: Mapping[str, Any],
    actor_id: str,
    *,
    ban_votes: Mapping[str, Sequence[str]],
    leaders: Sequence[str],
    now: datetime,
    rng: Any = _RANDOM,
) -> dict[str, Any]:
    """The vote counted and the draft dealt: more leader bans land with a
    majority of the players, as many as the cap leaves room for. A blind draft
    then has its players pick; any other is done."""
    require_host(lobby, actor_id)
    require_phase(lobby, VOTING)
    bans = with_voted_bans(lobby, ban_votes, {"leader": leaders})
    pools = dealt({**lobby, "bans": bans}, _groups(lobby), leaders, rng)
    if lobby["draft_mode"] == "blind":
        return {
            "bans": bans,
            "pools": pools,
            "phase": PICKING,
            "deadline": now + PICK_LIMIT,
        }
    return {"bans": bans, "pools": pools, "status": DONE, "deadline": None}


def picked(
    lobby: Mapping[str, Any], player_id: str, leader: str, civ: str | None = None
) -> dict[str, Any]:
    """A player's blind pick from their own pool, with a civ where the pool has
    civs, locked in once made."""
    require_phase(lobby, PICKING)
    pool = next((p for p in lobby["pools"] if p["player_id"] == player_id), None)
    if pool is None:
        raise LobbyRefused(f"you are not a player in this {_noun(lobby)}")
    if any(p["player_id"] == player_id for p in lobby["picks"]):
        raise LobbyRefused("you have already locked in your pick")
    if leader not in pool["leaders"]:
        raise LobbyRefused("that leader is not in your pool")
    civs = pool.get("civs") or []
    if civs and civ not in civs:
        raise LobbyRefused("pick a civ from your pool too")
    pick: dict[str, Any] = {"player_id": player_id, "leader": leader, "by": "player"}
    if civs:
        pick["civ"] = civ
    return {"picks": [*lobby["picks"], pick]}


def revealed(
    lobby: Mapping[str, Any], actor_id: str, rng: Any = _RANDOM
) -> dict[str, Any]:
    """The blind picks revealed together, in seat order; a player who did not
    pick is auto-picked from their pool at random."""
    require_host(lobby, actor_id)
    require_phase(lobby, PICKING)
    made = {p["player_id"]: p for p in lobby["picks"]}

    def auto(pool: Mapping[str, Any]) -> dict[str, Any]:
        pick = {
            "player_id": pool["player_id"],
            "leader": rng.choice(pool["leaders"]),
            "by": "auto",
        }
        if pool.get("civs"):
            pick["civ"] = rng.choice(pool["civs"])
        return pick

    picks = [made.get(pool["player_id"]) or auto(pool) for pool in lobby["pools"]]
    return {"picks": picks, "status": DONE, "deadline": None}


def cancel_reason(lobby: Mapping[str, Any], actor_id: str, staff: bool) -> str:
    """Who closed it: the host, or staff on the host's behalf."""
    _open(lobby)
    if actor_id == lobby["host_id"]:
        return "host"
    if staff:
        return "staff"
    raise NotTheHost(f"only the host or staff can cancel the {_noun(lobby)}")


__all__ = [
    "BLIND_POOL",
    "CANCELLED",
    "DONE",
    "FILL_WAIT",
    "IDLE_LIMIT",
    "LIVE",
    "OPEN",
    "PICKING",
    "PICK_LIMIT",
    "SETUP",
    "SETUP_IDLE_LIMIT",
    "SHAPES",
    "VOTE_LIMIT",
    "VOTING",
    "LobbyRefused",
    "NotTheHost",
    "Shape",
    "ballot_counted",
    "cancel_reason",
    "dealt",
    "edited",
    "finished",
    "joined",
    "left",
    "lobby_code",
    "new_lobby",
    "picked",
    "revealed",
    "started",
    "tallied",
    "team_sizes",
    "vote_placed",
    "went_live",
]
