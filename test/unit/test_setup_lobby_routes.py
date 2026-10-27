from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId
from pydantic import SecretStr

from app.features.match_setups.repository import SeatedElsewhere

TOKEN = "mito-test-token"
LOBBIES = "/api/v2/match-setups/lobbies"
SEASON = ObjectId()
CIV6 = {
    "leader_data_version": 5,
    "leaders": [{"token": f"LEADER_{n:02}"} for n in range(30)],
    "civs": [],
}
HOST = {
    "shape": "ffa-8",
    "draft_mode": "standard",
    "guild_id": "g",
    "channel_id": "c",
    "steam_link": "steam://joinlobby/289070/109775241000000000/76561198000000000",
}


def as_player(player: str, staff: bool = False) -> dict[str, str]:
    headers = {"authorization": f"Bearer {TOKEN}", "x-actor-discord-id": player}
    return {**headers, "x-actor-is-staff": "true"} if staff else headers


class Store:
    """match_setups in memory, with the unique open seat and the revision gate."""

    def __init__(self) -> None:
        self.docs: dict[ObjectId, dict] = {}
        self.stale_writes = 0

    def __call__(self, _db):
        return self

    def _seated_elsewhere(self, doc: dict) -> bool:
        mine = {p["discord_id"] for p in doc["players"]}
        return any(
            other["_id"] != doc.get("_id")
            and other["status"] == "open"
            and mine & {p["discord_id"] for p in other["players"]}
            for other in self.docs.values()
        )

    async def insert(self, doc):
        if doc.get("command") == "lobby" and self._seated_elsewhere(doc):
            raise SeatedElsewhere
        doc["_id"] = ObjectId()
        self.docs[doc["_id"]] = dict(doc)
        return doc["_id"]

    async def lobby(self, lobby_id):
        doc = self.docs.get(ObjectId(lobby_id)) if ObjectId.is_valid(lobby_id) else None
        return dict(doc) if doc else None

    async def update_lobby(self, lobby, changes, now):
        if self.stale_writes:
            self.stale_writes -= 1
            return None
        current = self.docs[lobby["_id"]]
        if current["revision"] != lobby["revision"]:
            return None
        written = {
            **current,
            **changes,
            "updated_at": now,
            "revision": current["revision"] + 1,
        }
        if written["status"] == "open" and self._seated_elsewhere(written):
            raise SeatedElsewhere
        self.docs[lobby["_id"]] = written
        return dict(written)

    async def open_lobbies(self, guild_id, host_id=None):
        return [
            d
            for d in self.docs.values()
            if d["status"] == "open"
            and d["guild_id"] == guild_id
            and (d["host_id"] == host_id if host_id else d["live"])
        ]

    async def votes_due(self, now):
        return [
            dict(d)
            for d in self.docs.values()
            if d["status"] == "open" and d.get("vote") and d["vote"]["ends_at"] < now
        ]

    async def close_idle(self, cutoff, now):
        closed = []
        for doc in self.docs.values():
            if doc["status"] == "open" and doc["updated_at"] < cutoff:
                doc.update(status="cancelled", cancel_reason="idle", closed_at=now)
                closed.append(dict(doc))
        return closed


@pytest.fixture
def lobbies(monkeypatch):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies
    from app.features.match_setups import router as match_setups

    store = Store()

    class FakeCivData:
        def __init__(self, _db):
            pass

        async def fetch(self, edition):
            return CIV6

    class FakeSeasons:
        def __init__(self, _db):
            pass

        async def get_current_season(self, edition):
            return {"_id": SEASON}

    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(TOKEN))
    monkeypatch.setattr(match_setups, "CivDataRepository", FakeCivData)
    monkeypatch.setattr(match_setups, "SeasonsRepository", FakeSeasons)
    monkeypatch.setattr(match_setups, "MatchSetupRepository", store)
    app = FastAPI()
    app.include_router(match_setups.router)
    app.dependency_overrides[dependencies.get_database] = lambda: object()
    return TestClient(app, raise_server_exceptions=False), store


def hosted(client, host="host", live=True, **fields):
    reply = client.post(LOBBIES, json={**HOST, **fields}, headers=as_player(host))
    assert reply.status_code == 201, reply.json()
    if not live:
        return reply.json()
    url = f"{LOBBIES}/{reply.json()['id']}/live"
    opened = client.post(url, json={"message_id": "m"}, headers=as_player(host))
    assert opened.status_code == 200, opened.json()
    return opened.json()


def edit(client, lobby, host="host", **fields):
    body = {"shape": lobby["shape"], "settings": {}, "bans": [], **fields}
    url = f"{LOBBIES}/{lobby['id']}/edit"
    return client.put(url, json=body, headers=as_player(host))


def test_a_lobby_votes_then_drafts_into_a_recorded_setup(lobbies):
    client, store = lobbies
    lobby = hosted(client)
    url = f"{LOBBIES}/{lobby['id']}"
    assert client.post(f"{url}/join", headers=as_player("a")).status_code == 200
    banned = edit(client, lobby, bans=["LEADER_00"])
    assert banned.json()["bans"] == [
        {"token": "LEADER_00", "kind": "leader", "by": "host"}
    ]
    started = client.post(f"{url}/start", headers=as_player("host")).json()
    assert (started["status"], started["vote"]["finished"]) == ("open", [])
    place = {"channel_id": "t", "messages": {"map": "m1", "bans": "m2"}}
    placed = client.put(f"{url}/vote", json=place, headers=as_player("host")).json()
    assert placed["vote"]["messages"] == place["messages"]
    done = client.post(f"{url}/finished", headers=as_player("a")).json()
    assert done["vote"]["finished"] == ["a"]
    ballot = {
        "settings": {"map": {"pangea_classic": ["host", "a"]}},
        "bans": {"LEADER_01": ["host", "a"], "LEADER_02": ["a"]},
    }
    counted = client.post(f"{url}/tally", json=ballot, headers=as_player("host"))
    drafted = counted.json()
    assert drafted["status"] == "done"
    assert {"id": "map", "value": "pangea_classic", "by": "vote"} in drafted["settings"]
    assert [b["token"] for b in drafted["bans"]] == ["LEADER_00", "LEADER_01"]
    assert [(p["player_id"], len(p["leaders"])) for p in drafted["pools"]] == [
        ("host", 14),
        ("a", 14),
    ]
    assert store.docs[ObjectId(lobby["id"])]["season_id"] == SEASON
    again = client.post(f"{url}/tally", json=ballot, headers=as_player("host"))
    assert again.status_code == 409


def test_a_vote_out_of_time_is_listed_for_counting(lobbies):
    client, store = lobbies
    url = f"{LOBBIES}/{hosted(client)['id']}"
    client.post(f"{url}/join", headers=as_player("a"))
    started = client.post(f"{url}/start", headers=as_player("host")).json()
    due = client.get(f"{LOBBIES}/votes-due", headers=as_player("bot"))
    assert due.json() == []
    store.docs[ObjectId(started["id"])]["vote"]["ends_at"] = datetime.now(
        UTC
    ) - timedelta(seconds=1)
    due = client.get(f"{LOBBIES}/votes-due", headers=as_player("bot")).json()
    assert [lobby["id"] for lobby in due] == [started["id"]]
    too_many = {"settings": {}, "bans": {"LEADER_01": [str(n) for n in range(15)]}}
    refused = client.post(f"{url}/tally", json=too_many, headers=as_player("host"))
    assert refused.status_code == 422


def test_refusals_say_why_and_only_the_host_starts(lobbies):
    client, _ = lobbies
    url = f"{LOBBIES}/{hosted(client, shape='duel')['id']}"
    client.post(f"{url}/join", headers=as_player("a"))
    full = client.post(f"{url}/join", headers=as_player("b"))
    assert full.status_code == 409
    assert full.json()["detail"]["error"]["message"] == "the lobby is full"
    assert client.post(f"{url}/start", headers=as_player("a")).status_code == 403
    assert client.post(f"{url}/start", headers=as_player("host")).status_code == 200
    assert client.post(f"{url}/leave", headers=as_player("a")).status_code == 409


def test_a_player_sits_in_one_open_lobby_at_a_time(lobbies):
    client, _ = lobbies
    first = hosted(client)
    second = client.post(LOBBIES, json=HOST, headers=as_player("host"))
    assert second.status_code == 409
    other = hosted(client, host="other")
    client.post(f"{LOBBIES}/{first['id']}/join", headers=as_player("a"))
    moved_on = client.post(f"{LOBBIES}/{other['id']}/join", headers=as_player("a"))
    assert (
        moved_on.json()["detail"]["error"]["message"]
        == "you are already in another open lobby"
    )


def test_a_lobby_in_setup_is_hidden_and_closed_to_players(lobbies):
    client, _ = lobbies
    setup = hosted(client, live=False, steam_link=None)
    assert (setup["live"], setup["steam_link"]) == (False, None)
    url = f"{LOBBIES}/{setup['id']}"
    assert client.post(f"{url}/join", headers=as_player("a")).status_code == 409
    listed = client.get(LOBBIES, params={"guild_id": "g"}, headers=as_player("a"))
    assert listed.json() == []
    mine = {"guild_id": "g", "host_id": "host"}
    found = client.get(LOBBIES, params=mine, headers=as_player("host")).json()
    assert [lobby["id"] for lobby in found] == [setup["id"]]
    live = client.post(f"{url}/live", json={"message_id": "m"}, headers=as_player("a"))
    assert live.status_code == 403


def test_an_edit_is_checked_whole_against_civ_data_and_the_catalogue(lobbies):
    client, _ = lobbies
    lobby = hosted(client)
    assert edit(client, lobby, bans=["LEADER_NOBODY"]).status_code == 400
    assert edit(client, lobby, settings={"huts": "x9"}).status_code == 409
    assert edit(client, lobby, shape="duel").status_code == 409
    saved = edit(client, lobby, shape="ffa-10", settings={"huts": "x1_5"}).json()
    assert saved["settings"] == [{"id": "huts", "value": "x1_5", "by": "host"}]
    assert saved["seats"] == 10
    questions = client.get(
        "/api/v2/match-setups/settings/civ6/ffa", headers=as_player("host")
    )
    assert "huts" in [q["id"] for q in questions.json()]


def test_a_write_that_lost_a_race_is_tried_again(lobbies):
    client, store = lobbies
    url = f"{LOBBIES}/{hosted(client)['id']}"
    store.stale_writes = 2
    assert client.post(f"{url}/join", headers=as_player("a")).status_code == 200
    store.stale_writes = 3
    lost = client.post(f"{url}/join", headers=as_player("b"))
    assert (
        lost.status_code == 409 and lost.json()["detail"]["error"]["retryable"] is True
    )


def test_staff_cancel_for_the_host_and_idle_lobbies_close(lobbies):
    client, store = lobbies
    url = f"{LOBBIES}/{hosted(client)['id']}"
    assert client.post(f"{url}/cancel", headers=as_player("a")).status_code == 403
    cancelled = client.post(
        f"{url}/cancel", headers=as_player("mod", staff=True)
    ).json()
    assert (cancelled["status"], cancelled["cancel_reason"]) == ("cancelled", "staff")
    idle = hosted(client, host="sleepy")
    store.docs[ObjectId(idle["id"])]["updated_at"] = datetime.now(UTC) - timedelta(
        hours=3
    )
    closed = client.post(f"{LOBBIES}/close-idle", headers=as_player("bot")).json()
    assert [c["id"] for c in closed] == [idle["id"]]
    assert (
        client.get(LOBBIES, params={"guild_id": "g"}, headers=as_player("bot")).json()
        == []
    )
