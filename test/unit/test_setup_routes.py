from __future__ import annotations

import pytest
from bson import ObjectId
from pydantic import SecretStr

from app.features.seasons.repository import SeasonNotSeededError

TOKEN = "mito-test-token"
HEADERS = {"authorization": f"Bearer {TOKEN}"}
DRAFT = "/api/v2/match-setups/draft"
SEASON = ObjectId()
CIV6 = {
    "leader_data_version": 4,
    "leaders": [{"token": f"LEADER_{n}"} for n in range(10)],
    "civs": [],
}
ASK = {"host_id": "100", "edition": "civ6", "game_type": "ffa", "drafts": 3}


def setup_client(monkeypatch, civ_data, season=SEASON):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies
    from app.features.match_setups import router as match_setups

    recorded: list[dict] = []

    class FakeCivData:
        def __init__(self, _db):
            pass

        async def fetch(self, edition):
            return civ_data

    class FakeSeasons:
        def __init__(self, _db):
            pass

        async def get_current_season(self, edition):
            if season is None:
                raise SeasonNotSeededError(edition)
            return {"_id": season}

    class FakeSetups:
        def __init__(self, _db):
            pass

        async def insert(self, record):
            recorded.append(record)

    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(TOKEN))
    monkeypatch.setattr(match_setups, "CivDataRepository", FakeCivData)
    monkeypatch.setattr(match_setups, "SeasonsRepository", FakeSeasons)
    monkeypatch.setattr(match_setups, "MatchSetupRepository", FakeSetups)
    app = FastAPI()
    app.include_router(match_setups.router)
    app.dependency_overrides[dependencies.get_database] = lambda: object()
    return TestClient(app, raise_server_exceptions=False), recorded


def test_a_draft_is_dealt_and_its_setup_recorded_with_season_and_bans(monkeypatch):
    client, recorded = setup_client(monkeypatch, CIV6)
    ask = {**ASK, "leaders": 2, "leader_bans": ["LEADER_0", "LEADER_1"]}
    reply = client.post(DRAFT, json=ask, headers=HEADERS)
    assert reply.status_code == 200
    assert [len(p["leaders"]) for p in reply.json()["pools"]] == [2, 2, 2]
    [record] = recorded
    assert record["command"] == "draft" and record["mode"] == "ffa"
    assert record["season_id"] == SEASON and record["host_id"] == "100"
    assert record["bans"] == [
        {"token": "LEADER_0", "kind": "leader", "by": "host"},
        {"token": "LEADER_1", "kind": "leader", "by": "host"},
    ]


def test_a_draft_before_any_season_is_still_recorded(monkeypatch):
    client, recorded = setup_client(monkeypatch, CIV6, season=None)
    reply = client.post(DRAFT, json={**ASK, "leaders": 3}, headers=HEADERS)
    assert reply.status_code == 200
    assert recorded[0]["season_id"] is None


def test_a_deal_that_cannot_be_made_says_why_and_records_nothing(monkeypatch):
    client, recorded = setup_client(monkeypatch, CIV6)
    reply = client.post(DRAFT, json={**ASK, "leaders": 4}, headers=HEADERS)
    assert reply.status_code == 400
    assert "10 remain after bans" in reply.json()["detail"]["error"]["message"]
    assert recorded == []


def test_a_draft_with_no_civ_data_is_unavailable(monkeypatch):
    empty = {"leader_data_version": None, "leaders": [], "civs": []}
    client, recorded = setup_client(monkeypatch, empty)
    reply = client.post(DRAFT, json={**ASK, "leaders": 3}, headers=HEADERS)
    assert reply.status_code == 503
    assert recorded == []
