#!/usr/bin/env python3
"""smoke_lobby [--base URL]

Drives a lobby through every phase against a running instance: create, join,
start, vote, ready, ban, pick, complete. Runs an ffa, a cwc teamer, and an
observer read, then deletes the lobbies it made.

Unit tests run against fakes, and a fake answers what it was told to. This
answers what the cluster does.

Default base: http://127.0.0.1:8001 (dev).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bson import ObjectId
from pymongo import AsyncMongoClient

from app.core.config import settings
from app.core.constants import COL_LOBBIES, GAMES_DB

BASE = "http://127.0.0.1:8001"
MITO = settings.mito_service_token.get_secret_value()
ACTIVITY = settings.activity_service_token.get_secret_value()

failures: list[str] = []
made: list[str] = []
PLAYERS = ("alice", "bob", "carol", "dave", "erin", "frank")
WATCHED = ("kate", "liam", "mona", "nils", "omar", "pria")


def call(
    method: str,
    path: str,
    token: str,
    actor: str | None = None,
    body: Any = None,
    staff: bool = False,
) -> tuple[int, Any]:
    req = urllib.request.Request(BASE + path, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if actor:
        req.add_header("X-Actor-Discord-Id", actor)
    if staff:
        req.add_header("X-Actor-Is-Staff", "true")
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


def step(label: str, want: int, *args: Any, **kwargs: Any) -> Any:
    code, payload = call(*args, **kwargs)
    ok = code == want
    note = ""
    if isinstance(payload, dict):
        if "phase" in payload:
            note = f"rev={payload['revision']} {payload['phase']}"
        elif payload.get("detail"):
            note = str(payload["detail"].get("error", {}).get("code", ""))
    print(f"{'ok  ' if ok else 'FAIL'} {label:38} {code:3} want {want:3}  {note}")
    if not ok:
        failures.append(label)
    return payload


def ffa_lifecycle() -> None:
    print("\n-- ffa, standard draft, two players --")
    lobby = step(
        "create",
        201,
        "POST",
        "/api/v2/lobbies",
        MITO,
        body={
            "guild_id": "smoke",
            "channel_id": f"c-{ObjectId()}",
            "voice_channel_id": "v-smoke",
            "host_discord_id": "alice",
            "edition": "civ6",
            "game_type": "ffa",
            "size": 8,
            "host_rules": "smoke test",
        },
    )
    if "_id" not in lobby:
        return
    made.append(lobby["_id"])
    path = f"/api/v2/lobbies/{lobby['_id']}"
    rev = lobby["revision"]

    # An ffa floors at six seats however small the size, so the host cannot
    # start until six are seated.
    for index, who in enumerate(PLAYERS[1:], start=1):
        lobby = step(
            f"{who} joins",
            200,
            "PATCH",
            f"{path}/seats",
            ACTIVITY,
            who,
            {"expected_revision": rev, "action": "place", "seat_index": index},
        )
        rev = lobby["revision"]
    step(
        "bob cannot start",
        403,
        "POST",
        f"{path}/start",
        ACTIVITY,
        "bob",
        {"expected_revision": rev},
    )
    lobby = step(
        "host starts",
        200,
        "POST",
        f"{path}/start",
        ACTIVITY,
        "alice",
        {"expected_revision": rev},
    )
    rev = lobby["revision"]

    for who in PLAYERS:
        lobby = step(
            f"{who} votes",
            200,
            "PUT",
            f"{path}/votes",
            ACTIVITY,
            who,
            {"expected_revision": rev, "selections": {"draft_mode": "standard"}},
        )
        rev = lobby["revision"]
    step("voting is not finishing", 200, "GET", path, ACTIVITY, "alice")

    for who in PLAYERS:
        lobby = step(
            f"{who} ready",
            200,
            "PUT",
            f"{path}/ready",
            ACTIVITY,
            who,
            {"expected_revision": rev},
        )
        rev = lobby["revision"]
    print(f"     phase after settings: {lobby['phase']}")

    for who in PLAYERS:
        lobby = step(
            f"{who} bans nothing",
            200,
            "PUT",
            f"{path}/bans",
            ACTIVITY,
            who,
            {"expected_revision": rev},
        )
        rev = lobby["revision"]
        lobby = step(
            f"{who} ready",
            200,
            "PUT",
            f"{path}/ready",
            ACTIVITY,
            who,
            {"expected_revision": rev},
        )
        rev = lobby["revision"]
    print(f"     phase after bans: {lobby['phase']}")

    seats = {s["discord_id"]: s for s in lobby.get("seats") or []}
    pool = seats.get("alice", {}).get("pool") or []
    print(f"     alice was dealt {len(pool)} leaders")
    if not pool:
        failures.append("no pool dealt")
        return
    lobby = step(
        "alice picks",
        200,
        "PUT",
        f"{path}/picks",
        ACTIVITY,
        "alice",
        {"expected_revision": rev, "token": pool[0]},
    )
    rev = lobby["revision"]
    step(
        "alice cannot pick twice",
        409,
        "PUT",
        f"{path}/picks",
        ACTIVITY,
        "alice",
        {"expected_revision": rev, "token": pool[0]},
    )
    bob_pool = (seats.get("bob") or {}).get("pool") or []
    if bob_pool:
        lobby = step(
            "bob picks",
            200,
            "PUT",
            f"{path}/picks",
            ACTIVITY,
            "bob",
            {"expected_revision": rev, "token": bob_pool[0]},
        )
    print(f"     phase after picks: {lobby.get('phase')}")


def cwc_turns() -> None:
    print("\n-- cwc teamer, 2v2, captains ban in turn --")
    lobby = step(
        "create cwc",
        201,
        "POST",
        "/api/v2/lobbies",
        MITO,
        body={
            "guild_id": "smoke",
            "channel_id": f"c-{ObjectId()}",
            "voice_channel_id": "v-smoke",
            "host_discord_id": "gwen",
            "edition": "civ6",
            "game_type": "teamer",
            "number_teams": 2,
            "team_size": 2,
            "draft_mode": "cwc",
        },
    )
    if "_id" not in lobby:
        return
    made.append(lobby["_id"])
    path = f"/api/v2/lobbies/{lobby['_id']}"
    rev = lobby["revision"]

    for index, (who, team) in enumerate(
        [("gwen", 0), ("hugo", 1), ("iris", 0), ("jack", 1)]
    ):
        lobby = step(
            f"{who} takes seat {index}",
            200,
            "PATCH",
            f"{path}/seats",
            ACTIVITY,
            who,
            {
                "expected_revision": rev,
                "action": "place",
                "seat_index": index,
                "team": team,
            },
        )
        rev = lobby["revision"]

    lobby = step(
        "host starts",
        200,
        "POST",
        f"{path}/start",
        ACTIVITY,
        "gwen",
        {"expected_revision": rev},
    )
    rev = lobby["revision"]
    order = lobby.get("ban_order") or []
    print(f"     ban order: {order}")
    if len(order) != 4:
        failures.append("ban order is not team_size * 2")
        return

    for who in ("gwen", "hugo", "iris", "jack"):
        lobby = step(
            f"{who} ready",
            200,
            "PUT",
            f"{path}/ready",
            ACTIVITY,
            who,
            {"expected_revision": rev},
        )
        rev = lobby["revision"]
    print(f"     phase: {lobby['phase']}, turn {lobby.get('turn_index')}")

    step(
        "wrong captain refused",
        403,
        "PUT",
        f"{path}/bans",
        ACTIVITY,
        order[1],
        {"expected_revision": rev, "leader_keys": ["LEADER_TRAJAN"]},
    )
    step(
        "two keys in one turn refused",
        400,
        "PUT",
        f"{path}/bans",
        ACTIVITY,
        order[0],
        {"expected_revision": rev, "leader_keys": ["LEADER_TRAJAN", "LEADER_GANDHI"]},
    )
    lobby = step(
        "captain bans one",
        200,
        "PUT",
        f"{path}/bans",
        ACTIVITY,
        order[0],
        {"expected_revision": rev, "leader_keys": ["LEADER_TRAJAN"]},
    )
    print(f"     turn {lobby.get('turn_index')}, banned {lobby.get('bans')}")


def observer_read() -> None:
    """A blind draft, so censoring is actually doing something."""
    print("\n-- staff observer, blind draft --")
    lobby = step(
        "create blind",
        201,
        "POST",
        "/api/v2/lobbies",
        MITO,
        body={
            "guild_id": "smoke",
            "channel_id": f"c-{ObjectId()}",
            "voice_channel_id": "v-smoke",
            "host_discord_id": "kate",
            "edition": "civ6",
            "game_type": "ffa",
            "size": 8,
        },
    )
    if "_id" not in lobby:
        return
    made.append(lobby["_id"])
    path = f"/api/v2/lobbies/{lobby['_id']}"
    rev = lobby["revision"]

    for index, who in enumerate(WATCHED[1:], start=1):
        lobby = step(
            f"{who} joins",
            200,
            "PATCH",
            f"{path}/seats",
            ACTIVITY,
            who,
            {"expected_revision": rev, "action": "place", "seat_index": index},
        )
        rev = lobby["revision"]
    lobby = step(
        "host starts",
        200,
        "POST",
        f"{path}/start",
        ACTIVITY,
        "kate",
        {"expected_revision": rev},
    )
    rev = lobby["revision"]

    for who in WATCHED:
        lobby = step(
            f"{who} votes blind",
            200,
            "PUT",
            f"{path}/votes",
            ACTIVITY,
            who,
            {"expected_revision": rev, "selections": {"draft_mode": "blind"}},
        )
        rev = lobby["revision"]
        lobby = step(
            f"{who} ready",
            200,
            "PUT",
            f"{path}/ready",
            ACTIVITY,
            who,
            {"expected_revision": rev},
        )
        rev = lobby["revision"]
    for who in WATCHED:
        lobby = step(
            f"{who} bans nothing",
            200,
            "PUT",
            f"{path}/bans",
            ACTIVITY,
            who,
            {"expected_revision": rev},
        )
        rev = lobby["revision"]
        lobby = step(
            f"{who} ready",
            200,
            "PUT",
            f"{path}/ready",
            ACTIVITY,
            who,
            {"expected_revision": rev},
        )
        rev = lobby["revision"]
    print(
        f"     mode {(lobby.get('settings') or {}).get('draft_mode')}, "
        f"phase {lobby['phase']}"
    )

    # A blind draft hides every other seat's pool, so kate's pool has to be
    # read from a request made as kate.
    mine = step("kate reads her own seat", 200, "GET", path, ACTIVITY, "kate")
    seats = {s["discord_id"]: s for s in (mine or {}).get("seats") or []}
    pool = (seats.get("kate") or {}).get("pool") or []
    if not pool:
        failures.append("blind lobby dealt no pool")
        return
    lobby = step(
        "kate picks",
        200,
        "PUT",
        f"{path}/picks",
        ACTIVITY,
        "kate",
        {"expected_revision": rev, "token": pool[0]},
    )

    player = step("liam reads", 200, "GET", path, ACTIVITY, "liam")
    staff = step("staff reads", 200, "GET", path, ACTIVITY, "moderator", staff=True)
    hidden = [s.get("pick") for s in (player or {}).get("seats") or []]
    seen = [s.get("pick") for s in (staff or {}).get("seats") or []]
    print(f"     liam sees picks {hidden}, observer sees {seen}")
    if any(hidden):
        failures.append("a player saw another blind pick")
    if not any(seen):
        failures.append("the observer saw no picks")


async def drop_smoke_lobbies() -> int:
    """Clear anything a previous run left behind.

    A run that dies mid-lobby leaves its players seated, and the one-seat
    rule then refuses the next run for a whole hour until the stale-eviction
    cutoff passes. Clearing first makes the script repeatable.
    """
    client = AsyncMongoClient(settings.mongo_url.get_secret_value())
    try:
        result = await client[GAMES_DB][COL_LOBBIES].delete_many({"guild_id": "smoke"})
        return result.deleted_count
    finally:
        await client.close()


async def cleanup() -> None:
    if not made:
        return
    client = AsyncMongoClient(settings.mongo_url.get_secret_value())
    try:
        result = await client[GAMES_DB][COL_LOBBIES].delete_many(
            {"_id": {"$in": [ObjectId(i) for i in made]}}
        )
        print(f"\ncleaned up {result.deleted_count} lobbies")
    finally:
        await client.close()


def main() -> int:
    global BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8001")
    BASE = parser.parse_args().base
    left = asyncio.run(drop_smoke_lobbies())
    if left:
        print(f"cleared {left} lobbies from an earlier run")
    ffa_lifecycle()
    cwc_turns()
    observer_read()
    asyncio.run(cleanup())
    print(f"\n{len(failures)} failed" if failures else "\nall steps passed")
    for name in failures:
        print(f"  - {name}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
