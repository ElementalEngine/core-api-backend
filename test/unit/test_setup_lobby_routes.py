from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId
from pydantic import SecretStr

from app.features.match_setups.repository import CodeTaken, SeatedElsewhere

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
        if doc.get("command") == "lobby" and any(
            d["status"] == "open" and d["code"] == doc["code"]
            for d in self.docs.values()
        ):
            raise CodeTaken
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

    async def lobby_by_code(self, guild_id, code):
        return next(
            (
                dict(d)
                for d in self.docs.values()
                if d["status"] == "open"
                and d["guild_id"] == guild_id
                and d["code"] == code
            ),
            None,
        )

    async def open_lobbies(self, guild_id, host_id=None):
        return [
            d
            for d in self.docs.values()
            if d["status"] == "open"
            and d["guild_id"] == guild_id
            and (d["host_id"] == host_id if host_id else d["phase"] != "setup")
        ]

    async def due(self, now):
        return [
            dict(d)
            for d in self.docs.values()
            if d["status"] == "open" and d["deadline"] and d["deadline"] < now
        ]

    async def close_idle(self, cutoff, setup_cutoff, now):
        closed = []
        for doc in self.docs.values():
            limit = setup_cutoff if doc["phase"] == "setup" else cutoff
            if doc["status"] == "open" and doc["updated_at"] < limit:
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


def every_setting(client, lobby):
    url = f"/api/v2/match-setups/settings/civ6/{lobby['mode']}"
    asked = client.get(url, headers=as_player("host")).json()
    return {q["id"]: q["default_option_id"] for q in asked}


def hosted(client, host="host", live=True, **fields):
    reply = client.post(LOBBIES, json={**HOST, **fields}, headers=as_player(host))
    assert reply.status_code == 201, reply.json()
    if not live:
        return reply.json()
    set_up = edit(client, reply.json(), host, every_setting(client, reply.json()))
    assert set_up.status_code == 200, set_up.json()
    url = f"{LOBBIES}/{reply.json()['id']}/live"
    opened = client.post(url, json={"message_id": "m"}, headers=as_player(host))
    assert opened.status_code == 200, opened.json()
    return opened.json()


def edit(client, lobby, host="host", settings=None, **fields):
    chosen = {s["id"]: s["value"] for s in lobby["settings"]} | (settings or {})
    body = {"shape": lobby["shape"], "settings": chosen, "bans": [], **fields}
    url = f"{LOBBIES}/{lobby['id']}/edit"
    return client.put(url, json=body, headers=as_player(host))


def post(client, lobby, action, player, **kwargs):
    return client.post(
        f"{LOBBIES}/{lobby['id']}/{action}", headers=as_player(player), **kwargs
    )


def cancel(client, code, player, staff=False):
    body = {"guild_id": "g", "code": code}
    return client.post(
        f"{LOBBIES}/cancel", json=body, headers=as_player(player, staff=staff)
    )


def test_a_lobby_votes_on_bans_then_drafts_into_a_recorded_setup(lobbies):
    client, store = lobbies
    lobby = hosted(client)
    assert len(lobby["code"]) == 6 and lobby["phase"] == "live"
    assert post(client, lobby, "join", "a").status_code == 200
    banned = edit(client, lobby, bans=["LEADER_00"])
    assert banned.json()["bans"] == [
        {"token": "LEADER_00", "kind": "leader", "by": "host"}
    ]
    started = post(client, lobby, "start", "host").json()
    assert (started["phase"], started["vote"]["finished"]) == ("voting", [])
    place = {"channel_id": "v", "messages": {"status": "m1"}}
    url = f"{LOBBIES}/{lobby['id']}"
    placed = client.put(f"{url}/vote", json=place, headers=as_player("host")).json()
    assert placed["vote"]["messages"] == place["messages"]
    assert post(client, lobby, "finished", "a").json()["vote"]["finished"] == ["a"]
    ballot = {"bans": {"LEADER_01": ["host", "a"], "LEADER_02": ["a"]}}
    drafted = post(client, lobby, "tally", "host", json=ballot).json()
    assert (drafted["status"], drafted["deadline"]) == ("done", None)
    assert [b["token"] for b in drafted["bans"]] == ["LEADER_00", "LEADER_01"]
    assert [(p["player_id"], len(p["leaders"])) for p in drafted["pools"]] == [
        ("host", 14),
        ("a", 14),
    ]
    assert store.docs[ObjectId(lobby["id"])]["season_id"] == SEASON
    assert post(client, lobby, "tally", "host", json=ballot).status_code == 409


def test_a_full_lobby_counts_down_and_only_the_host_starts_it_sooner(lobbies):
    client, _ = lobbies
    duel = hosted(client, shape="duel", message="No novices", lfg=True)
    assert (duel["message"], duel["lfg"]) == ("No novices", True)
    full = post(client, duel, "join", "a").json()
    assert (full["phase"], full["deadline"] is not None) == ("live", True)
    freed = post(client, duel, "leave", "a").json()
    assert (freed["phase"], freed["deadline"]) == ("live", None)
    post(client, duel, "join", "a")
    assert post(client, duel, "start", "host").json()["phase"] == "voting"
    assert post(client, duel, "leave", "a").status_code == 409
    ffa = hosted(client, host="other")
    post(client, ffa, "join", "b")
    assert post(client, ffa, "start", "b").status_code == 403
    assert post(client, ffa, "start", "other").json()["phase"] == "voting"


def test_a_blind_draft_is_picked_privately_then_revealed(lobbies):
    client, _ = lobbies
    lobby = hosted(client)
    assert edit(client, lobby, settings={"draft_mode": "blind"}).status_code == 200
    post(client, lobby, "join", "a")
    post(client, lobby, "start", "host")
    blind = post(client, lobby, "tally", "host", json={"bans": {}}).json()
    assert (blind["status"], blind["phase"]) == ("open", "picking")
    mine = blind["pools"][1]["leaders"]
    pick = {"leader": mine[0]}
    assert post(client, lobby, "pick", "a", json=pick).json()["picks"] == [
        {"player_id": "a", "leader": mine[0], "by": "player"}
    ]
    assert post(client, lobby, "pick", "z", json=pick).status_code == 409
    assert post(client, lobby, "reveal", "a").status_code == 403
    shown = post(client, lobby, "reveal", "host").json()
    assert shown["status"] == "done"
    assert [p["by"] for p in shown["picks"]] == ["auto", "player"]


def test_votes_and_picks_past_their_time_are_listed(lobbies):
    client, store = lobbies
    lobby = hosted(client)
    post(client, lobby, "join", "a")
    post(client, lobby, "start", "host")
    due = client.get(f"{LOBBIES}/due", headers=as_player("bot"))
    assert due.json() == []
    store.docs[ObjectId(lobby["id"])]["deadline"] = datetime.now(UTC) - timedelta(
        seconds=1
    )
    due = client.get(f"{LOBBIES}/due", headers=as_player("bot")).json()
    assert [found["id"] for found in due] == [lobby["id"]]
    too_many = {"bans": {"LEADER_01": [str(n) for n in range(15)]}}
    assert post(client, lobby, "tally", "host", json=too_many).status_code == 422


def test_refusals_say_why(lobbies):
    client, _ = lobbies
    duel = hosted(client, shape="duel")
    post(client, duel, "join", "a")
    full = post(client, duel, "join", "b")
    assert full.status_code == 409
    assert full.json()["detail"]["error"]["message"] == "the lobby is full"


def test_a_lobby_is_cancelled_by_its_code_by_its_host_or_staff(lobbies):
    client, store = lobbies
    lobby = hosted(client)
    assert cancel(client, "ZZZZ", "host").status_code == 404
    assert cancel(client, lobby["code"], "a").status_code == 403
    staffed = cancel(client, lobby["code"], "mod", staff=True).json()
    assert (staffed["status"], staffed["cancel_reason"]) == ("cancelled", "staff")
    mine = hosted(client)
    closed = cancel(client, mine["code"].lower(), "host").json()
    assert (closed["status"], closed["cancel_reason"]) == ("cancelled", "host")


def test_a_code_in_use_is_drawn_again(lobbies, monkeypatch):
    client, _ = lobbies
    from app.features.match_setups import router as match_setups

    codes = iter(["AAAA", "AAAA", "BBBB"])
    monkeypatch.setattr(match_setups, "lobby_code", lambda: next(codes))
    assert hosted(client)["code"] == "AAAA"
    assert hosted(client, host="other")["code"] == "BBBB"


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
    assert (setup["phase"], setup["steam_link"], setup["settings"]) == (
        "setup",
        None,
        [],
    )
    url = f"{LOBBIES}/{setup['id']}"
    assert client.post(f"{url}/join", headers=as_player("a")).status_code == 409
    listed = client.get(LOBBIES, params={"guild_id": "g"}, headers=as_player("a"))
    assert listed.json() == []
    mine = {"guild_id": "g", "host_id": "host"}
    found = client.get(LOBBIES, params=mine, headers=as_player("host")).json()
    assert [lobby["id"] for lobby in found] == [setup["id"]]
    live = client.post(f"{url}/live", json={"message_id": "m"}, headers=as_player("a"))
    assert live.status_code == 403
    blank = client.post(
        f"{url}/live", json={"message_id": "m"}, headers=as_player("host")
    )
    assert blank.json()["detail"]["error"]["message"].startswith(
        "set every setting first"
    )


def test_an_edit_is_checked_whole_against_civ_data_and_the_catalogue(lobbies):
    client, _ = lobbies
    lobby = hosted(client)
    assert edit(client, lobby, bans=["LEADER_NOBODY"]).status_code == 400
    assert edit(client, lobby, settings={"huts": "x9"}).status_code == 409
    assert edit(client, lobby, shape="duel").status_code == 409
    partial = {"shape": "ffa-8", "settings": {"huts": "x1"}, "bans": []}
    url = f"{LOBBIES}/{lobby['id']}/edit"
    assert client.put(url, json=partial, headers=as_player("host")).status_code == 409
    saved = edit(client, lobby, shape="ffa-10", settings={"huts": "x1_5"}).json()
    assert {"id": "huts", "value": "x1_5", "by": "host"} in saved["settings"]
    assert len(saved["settings"]) == len(lobby["settings"])
    assert saved["seats"] == 10
    questions = client.get(
        "/api/v2/match-setups/settings/civ6/ffa", headers=as_player("host")
    ).json()
    assert [q["id"] for q in questions][-1] == "draft_mode"
    assert [q["id"] for q in questions] == [s["id"] for s in lobby["settings"]]


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


def test_idle_lobbies_close(lobbies):
    client, store = lobbies
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


def test_a_lobby_left_in_setup_frees_its_host_sooner(lobbies):
    client, store = lobbies
    left = hosted(client, host="gone", live=False)
    live = hosted(client, host="here")
    for lobby in (left, live):
        store.docs[ObjectId(lobby["id"])]["updated_at"] = datetime.now(UTC) - timedelta(
            minutes=20
        )
    closed = client.post(f"{LOBBIES}/close-idle", headers=as_player("bot")).json()
    assert [c["id"] for c in closed] == [left["id"]]
    assert hosted(client, host="gone", live=False)["status"] == "open"
