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

from app.features.civdata.seeds import leader_types
from app.features.match_setups.bans import ban_caps, voted_bans
from app.features.match_setups.pools import (
    NotEnoughPool,
    leader_pools,
    remaining_after_bans,
)
from app.features.match_setups.questions import VOTE, preset, settled

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
BANS_VOTE_LIMIT = timedelta(minutes=3)
# A full lobby waits this long before its vote, so players can check everyone
# is there; the host may start sooner.
FILL_WAIT = timedelta(minutes=3)
PICK_LIMIT = timedelta(minutes=3)
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
    questions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """A civ6 lobby in setup, its host in the first seat and each setting at
    its default until the host changes it; one with no default is put to the vote."""
    if steam_link is not None and not STEAM_LINK.fullmatch(steam_link):
        raise LobbyRefused("the Steam link is not a Civ6 lobby link")
    chosen = SHAPES[shape]
    settings = preset(questions, host_only=False)
    return {
        "command": "lobby",
        "edition": "civ6",
        "mode": chosen.mode,
        "shape": shape,
        "seats": chosen.seats,
        "teams": chosen.teams,
        "draft_mode": next(
            (s["value"] for s in settings if s["id"] == "draft_mode"), None
        ),
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
        "settings": settings,
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
    options = {q["id"]: {VOTE, *(o["id"] for o in q["options"])} for q in questions}
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
    ends: two duellists, three or more in an FFA, or equal teams of two or
    more. The seats lock while the players vote on more bans."""
    require_host(lobby, actor_id)
    require_phase(lobby, LIVE)
    players = lobby["players"]
    sizes = team_sizes(lobby) if lobby["teams"] else []
    if lobby["mode"] == "duel" and len(players) != 2:
        raise LobbyRefused("a duel starts with two players")
    if lobby["mode"] == "ffa" and len(players) < 3:
        raise LobbyRefused("an FFA starts with three or more players")
    if sizes and min(sizes) < 2:
        raise LobbyRefused("each team needs two or more players")
    if len(set(sizes)) > 1:
        raise LobbyRefused("the teams must be the same size")
    return {
        "phase": VOTING,
        "deadline": now + BANS_VOTE_LIMIT,
        "vote": {"channel_id": None, "messages": {}, "finished": []},
    }


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
    """The draft from the leaders left after bans: a pool each player, or each
    team in a teamer; random gives each player one."""
    remaining = remaining_after_bans(leaders, [b["token"] for b in lobby["bans"]])
    by_team = bool(lobby["teams"]) and lobby["draft_mode"] != "random"
    team_of = {p: i + 1 for i, team in enumerate(teams) for p in team}
    groups = teams if by_team else [[p] for team in teams for p in team]
    try:
        pools = leader_pools(
            remaining,
            len(groups),
            lobby["draft_mode"],
            leader_types(lobby["edition"]),
            rng,
        )
    except NotEnoughPool as exc:
        raise LobbyRefused(f"not enough leaders to deal: {exc}") from exc
    return [
        {
            "player_id": None if by_team else group[0],
            "team": (i + 1 if by_team else team_of[group[0]])
            if lobby["teams"]
            else None,
            "leaders": pool,
        }
        for i, (group, pool) in enumerate(zip(groups, pools, strict=True))
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


def with_voted_settings(
    lobby: Mapping[str, Any],
    setting_votes: Mapping[str, Mapping[str, Sequence[str]]],
    questions: Sequence[Mapping[str, Any]],
    *,
    final: bool,
) -> list[dict[str, Any]]:
    """The settings, with each one the host put to the vote settled as /vote
    settles it: at once by a majority, or by the final count."""
    held = {s["id"]: s for s in lobby["settings"]}
    asked = [q for q in questions if q["id"] in held]
    fixed = [s for s in lobby["settings"] if s["value"] != VOTE]
    won = {
        s["id"]: s
        for s in settled(
            {**lobby, "settings": fixed}, setting_votes, asked, final=final
        )
    }
    return [won.get(q["id"], held[q["id"]]) for q in asked]


def _draft_mode(settings: Sequence[Mapping[str, Any]]) -> str | None:
    return next((s["value"] for s in settings if s["id"] == "draft_mode"), None)


def ballot_counted(
    lobby: Mapping[str, Any],
    actor_id: str,
    *,
    ban_votes: Mapping[str, Sequence[str]],
    setting_votes: Mapping[str, Mapping[str, Sequence[str]]],
    leaders: Sequence[str],
    questions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """A running count while the players vote: a leader or a setting option
    reaching a majority wins at once, and stays won if a vote is taken back."""
    require_host(lobby, actor_id)
    require_phase(lobby, VOTING)
    settings = with_voted_settings(lobby, setting_votes, questions, final=False)
    return {
        "bans": with_voted_bans(lobby, ban_votes, {"leader": leaders}),
        "settings": settings,
        "draft_mode": _draft_mode(settings),
    }


def drafted(
    changes: dict[str, Any], draft_mode: str | None, now: datetime
) -> dict[str, Any]:
    """A dealt draft: a blind one goes on to its picks, any other is done."""
    if draft_mode == "blind":
        return {**changes, "phase": PICKING, "deadline": now + PICK_LIMIT}
    return {**changes, "status": DONE, "deadline": None}


def tallied(
    lobby: Mapping[str, Any],
    actor_id: str,
    *,
    ban_votes: Mapping[str, Sequence[str]],
    setting_votes: Mapping[str, Mapping[str, Sequence[str]]],
    leaders: Sequence[str],
    questions: Sequence[Mapping[str, Any]],
    now: datetime,
    rng: Any = _RANDOM,
) -> dict[str, Any]:
    """The vote counted and the draft dealt: every setting put to the vote is
    settled, and more leader bans land with a majority of the players, as many
    as the cap leaves room for. A blind draft then has its players pick; any
    other is done."""
    require_host(lobby, actor_id)
    require_phase(lobby, VOTING)
    settings = with_voted_settings(lobby, setting_votes, questions, final=True)
    draft_mode = _draft_mode(settings)
    bans = with_voted_bans(lobby, ban_votes, {"leader": leaders})
    voted = {**lobby, "bans": bans, "draft_mode": draft_mode}
    pools = dealt(voted, _groups(lobby), leaders, rng)
    changes = {
        "settings": settings,
        "draft_mode": draft_mode,
        "bans": bans,
        "pools": pools,
    }
    return drafted(changes, draft_mode, now)


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
    "BANS_VOTE_LIMIT",
    "CANCELLED",
    "CODE_LETTERS",
    "DONE",
    "IDLE_LIMIT",
    "LIVE",
    "OPEN",
    "PICKING",
    "PICK_LIMIT",
    "SETUP",
    "SETUP_IDLE_LIMIT",
    "SHAPES",
    "VOTING",
    "LobbyRefused",
    "NotTheHost",
    "ballot_counted",
    "cancel_reason",
    "dealt",
    "drafted",
    "edited",
    "finished",
    "joined",
    "left",
    "lobby_code",
    "new_lobby",
    "picked",
    "require_host",
    "require_phase",
    "revealed",
    "started",
    "tallied",
    "vote_placed",
    "went_live",
    "with_voted_bans",
    "with_voted_settings",
]
