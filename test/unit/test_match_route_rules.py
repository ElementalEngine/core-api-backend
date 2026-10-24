"""Who may act on a report, and what the bot is told when it must wait."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from app.features.matches.errors import ApprovingError
from app.features.matches.service import MatchService

TOKEN = "mito-test-token"
OID = "64b7f0c2a1b2c3d4e5f60718"
HEADERS = {"authorization": f"Bearer {TOKEN}", "x-actor-discord-id": "2"}


def _client(monkeypatch):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies
    from app.core.dependencies import get_database
    from app.features.matches.router_v2 import router

    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(TOKEN))
    monkeypatch.setattr(MatchService, "__init__", lambda self, db: None)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_database] = lambda: None
    return TestClient(app, raise_server_exceptions=False)


def _held(*args, **kwargs):
    raise ApprovingError("This match is being approved.")


def test_an_approve_in_progress_answers_409_retryable(monkeypatch):
    monkeypatch.setattr(MatchService, "approve_match", _held)
    res = _client(monkeypatch).post(f"/api/v2/matches/{OID}/approve", headers=HEADERS)
    assert res.status_code == 409
    assert res.json()["detail"]["error"]["retryable"] is True


def test_staff_who_did_not_play_cannot_contest(monkeypatch):
    async def match(self, match_id):
        return {"players": [{"discord_id": "1"}]}

    monkeypatch.setattr(MatchService, "get_match", match)
    res = _client(monkeypatch).post(
        f"/api/v2/matches/{OID}/contest",
        json={"reason": "wrong order"},
        headers={**HEADERS, "x-actor-is-staff": "true"},
    )
    assert res.status_code == 403
