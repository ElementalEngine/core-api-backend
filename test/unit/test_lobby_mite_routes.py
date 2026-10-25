from __future__ import annotations

import pytest
from pydantic import SecretStr

TOKEN = "mito-test-token"
HEADERS = {"authorization": f"Bearer {TOKEN}"}
MITE = "/api/v2/lobbies/mite"


def deal_client(monkeypatch, civ_data):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies
    from app.features.lobbies import router as lobbies

    class FakeCivData:
        def __init__(self, _db):
            pass

        async def fetch(self, edition):
            return civ_data

    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(TOKEN))
    monkeypatch.setattr(lobbies, "CivDataRepository", FakeCivData)
    app = FastAPI()
    app.include_router(lobbies.mite_router)
    app.dependency_overrides[dependencies.get_database] = lambda: object()
    return TestClient(app, raise_server_exceptions=False)


CIV6 = {
    "leader_data_version": 4,
    "leaders": [{"token": f"LEADER_{n}"} for n in range(10)],
    "civs": [],
}
DEAL = {"edition": "civ6", "game_type": "ffa", "drafts": 3, "leaders": 3}


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
