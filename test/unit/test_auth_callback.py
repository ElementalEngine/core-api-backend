import asyncio
import re
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.core import db as core_db, dependencies
from app.features.auth import router_v2
from app.features.auth.errors import (
    AuthConfigurationError,
    SignInCancelledError,
    SteamProfilePrivateError,
)
from app.main import app


def call_back(monkeypatch, outcome=None, **params):
    """The callback as a browser hits it, with the flow's answer stubbed."""

    class Flow:
        def __init__(self, db):
            pass

        async def callback(self, *, code, state, error):
            if outcome is not None:
                raise outcome

    monkeypatch.setattr(router_v2, "RegistrationFlow", Flow)
    app.dependency_overrides[dependencies.get_database] = lambda: None
    try:
        client = TestClient(app, raise_server_exceptions=False)
        return client.get(
            "/oauth/discord/callback", params=params or {"code": "c", "state": "s"}
        )
    finally:
        app.dependency_overrides.clear()


def test_no_v1_auth_route_is_served_or_published():
    paths = [getattr(route, "path", "") for route in app.routes]
    assert not [p for p in paths if p.startswith("/api/v1/auth")]
    assert not [p for p in app.openapi()["paths"] if p.startswith("/api/v1/auth")]
    assert "/oauth/discord/callback" in app.openapi()["paths"]


def test_a_finished_sign_in_answers_a_page_sending_the_player_back(monkeypatch):
    answer = call_back(monkeypatch)
    assert answer.status_code == 200
    assert answer.headers["content-type"].startswith("text/html")
    assert "go back to Discord" in answer.text


def test_the_page_loads_nothing_cannot_be_framed_and_is_not_kept(monkeypatch):
    for outcome in (None, SteamProfilePrivateError(game="civ6"), RuntimeError()):
        headers = call_back(monkeypatch, outcome).headers
        assert headers["content-security-policy"] == (
            "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'"
        )
        assert headers["cache-control"] == "no-store"
        assert headers["referrer-policy"] == "no-referrer"


def test_a_cancelled_sign_in_is_a_plain_page_without_a_reference(monkeypatch):
    answer = call_back(monkeypatch, SignInCancelledError())
    assert answer.status_code == 400
    assert "cancelled" in answer.text
    assert "Reference:" not in answer.text


def test_a_refused_sign_in_shows_its_reason_escaped(monkeypatch):
    refusal = SteamProfilePrivateError(game="civ6")
    refusal.message = "<script>alert(1)</script> & hidden"
    answer = call_back(monkeypatch, refusal)
    assert answer.status_code == 400
    assert answer.headers["content-type"].startswith("text/html")
    assert "<script>" not in answer.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt; &amp; hidden" in answer.text
    assert "Reference:" not in answer.text


def test_a_server_set_up_wrong_hides_the_reason_and_gives_a_reference(monkeypatch):
    answer = call_back(monkeypatch, AuthConfigurationError("client secret unset"))
    assert answer.status_code == 503
    assert "client secret unset" not in answer.text
    assert re.search(r"Reference: [0-9a-f]{12}<", answer.text)


def test_a_bug_answers_a_page_not_json(monkeypatch):
    answer = call_back(monkeypatch, RuntimeError("bug"))
    assert answer.status_code == 500
    assert answer.headers["content-type"].startswith("text/html")
    assert "bug" not in answer.text


def test_a_callback_without_a_state_is_an_invalid_link(monkeypatch):
    answer = call_back(monkeypatch, code="c")
    assert answer.status_code == 400
    assert "invalid or has already been used" in answer.text


def test_startup_never_touches_the_retired_auth_database(monkeypatch):
    touched = set()

    class Collection:
        def __getattr__(self, name):
            async def call(*args, **kwargs):
                return "index" if name == "create_index" else {}

            return call

    class Database:
        def __getitem__(self, name):
            return Collection()

    class Client:
        def __init__(self, *args, **kwargs):
            self.admin = SimpleNamespace(command=self._ping)

        async def _ping(self, name):
            return {"ok": 1}

        def __getitem__(self, name):
            touched.add(name)
            return Database()

        async def close(self):
            pass

    monkeypatch.setattr(core_db, "AsyncMongoClient", Client)

    async def start_and_stop():
        async with core_db.db_lifespan(SimpleNamespace(state=SimpleNamespace())):
            pass

    asyncio.run(start_and_stop())
    assert "server_members" in touched
    assert "auth" not in touched
