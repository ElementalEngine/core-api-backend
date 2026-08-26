"""D83 Hardening 2 — upload_game_report read the whole body with no cap, on a
host with no swap and no MemoryMax (D79, verified absent on the dev unit).

The limit mirrors Mite's CIV_SAVE.MAX_BYTES (constants.ts:73) so the server is
never stricter than the client: a file Mite accepts must not 400 server-side.

400, not 413: the route's two existing raises are bare-string 400s and there is
no generic HTTPException handler, so a 413 would be equally unstructured with a
status Mite has never handled. The size code, envelope and 413 belong together
in D92 -- §4 item 62, S7.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

TOKEN = "mito-test-token"


def _client(monkeypatch):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies

    # Patch the settings object the dependency closes over, not the module --
    # test_config.py reloads it. Same reason as test_mito_gate.py.
    from app.core.dependencies import get_database
    from app.features.matches.router import upload_router

    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(TOKEN))
    app = FastAPI()
    app.include_router(upload_router)
    # get_database raises AppDependencyError during dependency resolution --
    # before the handler body -- so the cap is unreachable without this. D60
    # names dependency_overrides as the route-test mechanism. The override is
    # never used: an oversized body is rejected before MatchService is built,
    # and an at-limit body fails parsing first.
    app.dependency_overrides[get_database] = lambda: None
    return TestClient(app, raise_server_exceptions=False)


def _post(client, payload):
    return client.post(
        "/api/v1/upload-game-report/",
        files={"file": ("save.Civ6Save", payload, "application/octet-stream")},
        data={
            "reporter_discord_id": "1",
            "is_cloud": "0",
            "discord_message_id": "1",
        },
        headers={"authorization": f"Bearer {TOKEN}"},
    )


def test_oversized_upload_is_rejected_for_size(monkeypatch):
    from app.features.matches.router import MAX_SAVE_BYTES

    res = _post(_client(monkeypatch), b"X" * (MAX_SAVE_BYTES + 1))
    assert res.status_code == 400
    assert "too large" in str(res.json()).lower()


def test_body_at_the_limit_is_not_rejected_for_size(monkeypatch):
    from app.features.matches.router import MAX_SAVE_BYTES

    # Not a real save, so it fails downstream -- but never on size. Asserting
    # on res.text rather than res.json(): past the cap the request reaches
    # MatchService with the overridden (None) db and fails there, and a bare
    # FastAPI() has no handler for that, so the body is not JSON.
    res = _post(_client(monkeypatch), b"X" * MAX_SAVE_BYTES)
    assert "too large" not in res.text.lower()
