"""The start route over HTTP: the host's success path and its refusals.

The service has its own tests; this file proves the route hands the request
through, returns what the service returned, and turns each refusal into its
envelope. Needs httpx (FastAPI's TestClient transport) and skips without it.

Governed by D169, D220.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

TOKEN = "activity-test-token"
LOBBY = "507f1f77bcf86cd799439011"
HEADERS = {"authorization": f"Bearer {TOKEN}", "x-actor-discord-id": "alice"}


def client_for(monkeypatch, start):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies
    from app.features.lobbies import router as lobbies

    monkeypatch.setattr(
        dependencies.settings, "activity_service_token", SecretStr(TOKEN)
    )

    class FakeService:
        async def start(self, lobby_id, actor, expected_revision):
            return await start(lobby_id, actor, expected_revision)

    monkeypatch.setattr(lobbies, "_service", lambda _db: FakeService())
    app = FastAPI()
    app.include_router(lobbies.activity_router)
    app.dependency_overrides[dependencies.get_database] = lambda: object()
    return TestClient(app, raise_server_exceptions=False)


def test_the_host_starting_gets_the_settings_phase_back(monkeypatch):
    seen = []

    async def start(lobby_id, actor, expected_revision):
        seen.append((lobby_id, actor, expected_revision))
        return {"_id": lobby_id, "phase": "settings", "revision": 5}

    client = client_for(monkeypatch, start)
    reply = client.post(
        f"/api/v2/lobbies/{LOBBY}/start",
        json={"expected_revision": 4},
        headers=HEADERS,
    )
    assert reply.status_code == 200
    assert reply.json() == {"_id": LOBBY, "phase": "settings", "revision": 5}
    assert seen == [(LOBBY, "alice", 4)]


def test_a_player_who_is_not_the_host_is_told_so(monkeypatch):
    from app.features.lobbies.service import NotTheHost

    async def start(*_args):
        raise NotTheHost("Only the host can start the lobby")

    client = client_for(monkeypatch, start)
    reply = client.post(
        f"/api/v2/lobbies/{LOBBY}/start",
        json={"expected_revision": 4},
        headers=HEADERS,
    )
    assert reply.status_code == 403
    error = reply.json()["detail"]["error"]
    assert error["code"] == "FORBIDDEN"
    assert error["retryable"] is False


def test_a_stale_revision_is_a_conflict_naming_both_revisions(monkeypatch):
    from app.features.lobbies.service import SeatChangeRefused

    async def start(*_args):
        raise SeatChangeRefused("The lobby moved on", 4, 6)

    client = client_for(monkeypatch, start)
    reply = client.post(
        f"/api/v2/lobbies/{LOBBY}/start",
        json={"expected_revision": 4},
        headers=HEADERS,
    )
    assert reply.status_code == 409
    error = reply.json()["detail"]["error"]
    assert error["code"] == "CONFLICT"
    assert error["details"] == {"expected_revision": 4, "current_revision": 6}
