from __future__ import annotations

import pytest
from pydantic import SecretStr

from app.features.seasons.repository import SeasonNotSeededError

TOKEN = "mito-test-token"
HEADERS = {"authorization": f"Bearer {TOKEN}"}
MITE = "/api/v2/lobbies/mite"


def client_for(monkeypatch, service):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies
    from app.features.lobbies import router as lobbies

    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(TOKEN))
    monkeypatch.setattr(lobbies, "_service", lambda _db: service)
    app = FastAPI()
    app.include_router(lobbies.mite_router)
    app.dependency_overrides[dependencies.get_database] = lambda: object()
    return TestClient(app, raise_server_exceptions=False)


def test_a_lobby_for_an_edition_with_no_season_is_refused_by_name(monkeypatch):
    class Unseeded:
        async def create(self, body, name):
            raise SeasonNotSeededError("No season is recorded for civ7.")

    reply = client_for(monkeypatch, Unseeded()).post(
        MITE,
        json={
            "guild_id": "g",
            "channel_id": "c",
            "host_discord_id": "alice",
            "edition": "civ7",
            "game_type": "ffa",
            "size": 8,
            "voice_channel_id": "v",
        },
        headers=HEADERS,
    )
    assert reply.status_code == 404
    assert (
        reply.json()["detail"]["error"]["message"] == "No season is recorded for civ7."
    )


def deal_client(monkeypatch, civ_data):
    from app.features.lobbies import router as lobbies

    class FakeCivData:
        def __init__(self, _db):
            pass

        async def fetch(self, edition):
            return civ_data

    monkeypatch.setattr(lobbies, "CivDataRepository", FakeCivData)
    return client_for(monkeypatch, object())


CIV6 = {
    "leader_data_version": 4,
    "leaders": [{"token": f"LEADER_{n}"} for n in range(10)],
    "civs": [],
}
DEAL = {"edition": "civ6", "game_type": "ffa", "drafts": 3, "leaders": 3}


def test_the_draft_settings_are_the_edition_s_questions(monkeypatch):
    reply = client_for(monkeypatch, object()).get(
        f"{MITE}/settings/civ6", headers=HEADERS
    )
    assert reply.status_code == 200
    ids = [q["id"] for q in reply.json()["questions"]]
    assert "huts" in ids and "map" in ids


def test_a_deal_answers_one_pool_per_draft(monkeypatch):
    reply = deal_client(monkeypatch, CIV6).post(
        f"{MITE}/deal", json=DEAL, headers=HEADERS
    )
    assert reply.status_code == 200
    assert [len(p["leaders"]) for p in reply.json()["pools"]] == [3, 3, 3]


def test_a_deal_that_cannot_be_made_says_why(monkeypatch):
    reply = deal_client(monkeypatch, CIV6).post(
        f"{MITE}/deal", json={**DEAL, "leaders": 4}, headers=HEADERS
    )
    assert reply.status_code == 400
    assert "10 remain after bans" in reply.json()["detail"]["error"]["message"]


def test_a_deal_with_no_civ_data_is_unavailable(monkeypatch):
    empty = {"leader_data_version": None, "leaders": [], "civs": []}
    reply = deal_client(monkeypatch, empty).post(
        f"{MITE}/deal", json=DEAL, headers=HEADERS
    )
    assert reply.status_code == 503
