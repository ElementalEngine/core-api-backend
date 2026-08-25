"""The mito gate, end to end: 401 without the header, admitted with it.

The structural half -- "every router carries a gate" -- moved to
test_route_gates.py in S8 CP3. It looped a hand-maintained tuple that had
already fallen behind by one router (Correction 65, D169); the replacement
derives from the app's own route table.

Needs httpx (FastAPI's TestClient transport) and skips cleanly without it.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from app.features.stats.router import router as stats_router

TOKEN = "mito-test-token"


def test_gate_returns_401_without_header_and_admits_with_it(monkeypatch):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.core import dependencies

    # Patch the exact settings object the dependency closes over — the config
    # module gets reloaded by test_config.py/test_auth_config.py, so importing
    # app.core.config.settings here could yield a different instance.
    monkeypatch.setattr(dependencies.settings, "mito_service_token", SecretStr(TOKEN))

    app = FastAPI()
    app.include_router(stats_router)
    client = TestClient(app, raise_server_exceptions=False)

    denied = client.get(
        "/api/v1/stats/user",
        params={"civ_version": "civ6", "game_type": "realtime", "discord_id": "1"},
    )
    assert denied.status_code == 401
    assert denied.json()["detail"]["error"]["code"] == "UNAUTHORIZED"

    # With the header the gate admits the request; it then fails on the missing
    # Mongo dependency (503 via AppDependencyError) — which proves it got past 401.
    admitted = client.get(
        "/api/v1/stats/user",
        params={"civ_version": "civ6", "game_type": "realtime", "discord_id": "1"},
        headers={"authorization": f"Bearer {TOKEN}"},
    )
    assert admitted.status_code != 401
