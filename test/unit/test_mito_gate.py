"""Batch 8.3 — the matches/stats routers are gated behind require_mito_token.

The structural test always runs; the functional test needs httpx (FastAPI's
TestClient transport) and skips cleanly where it isn't installed.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from app.core.dependencies import require_mito_token
from app.features.matches.router import matches_router, upload_router
from app.features.stats.router import legacy_router as stats_legacy_router
from app.features.stats.router import router as stats_router

TOKEN = "mito-test-token"


def test_all_mito_facing_routers_carry_the_gate():
    for router in (matches_router, upload_router, stats_router, stats_legacy_router):
        assert any(
            dependency.dependency is require_mito_token
            for dependency in router.dependencies
        ), f"router {router.prefix!r} is missing require_mito_token"


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
