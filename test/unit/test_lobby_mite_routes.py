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
