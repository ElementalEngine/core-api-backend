"""Who may act on a report, and what the bot is told when it must wait."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import SecretStr

from app.features.matches.errors import ApprovingError
from app.features.matches.service import MatchService
from app.features.seasons.repository import SeasonNotSeededError

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


def test_an_edition_with_no_season_is_refused_by_name(monkeypatch):
    async def unseeded(*args, **kwargs):
        raise SeasonNotSeededError("No season is recorded for civ7.")

    monkeypatch.setattr(MatchService, "approve_match", unseeded)
    res = _client(monkeypatch).post(f"/api/v2/matches/{OID}/approve", headers=HEADERS)
    assert res.status_code == 404
    assert res.json()["detail"]["error"]["message"] == "No season is recorded for civ7."


def test_form_and_query_values_the_schema_cannot_bound_are_refused():
    from fastapi import HTTPException

    from app.features.matches import router_v2

    with pytest.raises(HTTPException) as quota:
        asyncio.run(router_v2.get_sub_quota(discord_id="99999999999999999999", db=None))
    assert quota.value.status_code == 400
    with pytest.raises(HTTPException) as upload:
        asyncio.run(
            router_v2.upload_match(
                file=None, is_cloud="true", discord_message_id="1", actor="1", db=None
            )
        )
    assert upload.value.status_code == 400
