"""The upload route caps the request body before reading it.

Reading an unbounded body on a host with no swap and no memory limit lets one
request take the process down. The cap is enforced in the handler, and this
proves a body over it is refused rather than buffered.

Governed by D79, D83, D92.
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

    res = _post(_client(monkeypatch), b"X" * MAX_SAVE_BYTES)
    assert "too large" not in res.text.lower()
