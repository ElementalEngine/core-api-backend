from __future__ import annotations

import re

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core import dependencies
from app.main import app

# The gates are checked by what they do, not by how FastAPI stores routes: each
# operation in the app's own spec is called with no token and with each bot's
# token. Without a database the lifespan never connects, so a request the gate
# admits fails on the missing client and nothing is read or written.

TOKENS = {"mite": "mite-test-token", "lj": "lj-test-token", "auth": "auth-test-token"}

PUBLIC_PATHS = frozenset(
    {
        "/",
        "/healthz",
        "/readyz",
        "/oauth/discord/callback",
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
    }
)

# Which bots each prefix admits. An operation under no prefix here fails the
# check, so a new router has to say whose token it takes.
ADMITS = (
    ("/api/v2/auth", {"auth"}),
    ("/api/v2/infractions", {"lj"}),
    ("/api/v2/civ-data", {"mite", "lj", "auth"}),
    ("/api/v2/matches", {"mite"}),
    ("/api/v2/stats", {"mite"}),
    ("/api/v2/match-setups", {"mite"}),
    ("/api/v2/seasons", {"mite"}),
)

METHODS = ("get", "post", "put", "patch", "delete")


@pytest.fixture(autouse=True)
def _tokens(monkeypatch):
    settings = dependencies.settings
    monkeypatch.setattr(settings, "mito_service_token", SecretStr(TOKENS["mite"]))
    monkeypatch.setattr(settings, "lj_service_token", SecretStr(TOKENS["lj"]))
    monkeypatch.setattr(settings, "auth_service_token", SecretStr(TOKENS["auth"]))


def operations(application: FastAPI = app) -> list[tuple[str, str]]:
    """Every method and path the app publishes, from its own spec."""
    paths = application.openapi()["paths"]
    return sorted(
        (method.upper(), path)
        for path, item in paths.items()
        for method in item
        if method in METHODS
    )


def concrete(path: str) -> str:
    # The gate runs before path parameters are validated, so any value will do.
    return re.sub(r"\{[^}]+\}", "x1", path)


def status(client: TestClient, method: str, path: str, token: str | None = None) -> int:
    headers = {"authorization": f"Bearer {token}"} if token else {}
    return client.request(method, concrete(path), headers=headers).status_code


def admitted_by(client: TestClient, method: str, path: str) -> set[str]:
    return {
        bot
        for bot, token in TOKENS.items()
        if status(client, method, path, token) != 401
    }


def expected(path: str) -> set[str] | None:
    return next((bots for prefix, bots in ADMITS if path.startswith(prefix)), None)


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


def test_the_spec_lists_the_operations():
    # Guards the checks below from passing on an empty list.
    ops = operations()
    assert ops
    assert ("GET", "/api/v2/matches/leaderboard") in ops


def test_every_operation_without_a_token_answers_401(client):
    answers = {
        f"{method} {path}": status(client, method, path)
        for method, path in operations()
        if path not in PUBLIC_PATHS
    }
    open_ = {op: code for op, code in answers.items() if code != 401}
    assert not open_, f"operations not refused without a token: {open_}"


def test_every_operation_admits_only_its_bots(client):
    wrong = []
    for method, path in operations():
        if path in PUBLIC_PATHS:
            continue
        want = expected(path)
        got = admitted_by(client, method, path)
        if want is None or got != want:
            wrong.append(f"{method} {path}: admits {sorted(got)}, expected {want}")
    assert not wrong, "\n".join(wrong)


def test_no_operation_is_left_on_v1():
    v1 = [path for _, path in operations() if path.startswith("/api/v1")]
    assert not v1, f"v1 routes still mounted: {v1}"


def test_no_public_path_is_stale(client):
    # A path removed from the app but left in PUBLIC_PATHS would silently
    # widen the exemption for whatever later takes that path.
    gone = [
        path for path in sorted(PUBLIC_PATHS) if client.get(path).status_code == 404
    ]
    assert not gone, f"not mounted: {gone}"


def test_the_check_fails_on_a_router_that_forgot_its_gate():
    # The case it can fail on: without this the checks above pass equally well
    # on a checker that never flags anything.
    probe = FastAPI()
    gateless = APIRouter(prefix="/api/v2/forgot")

    @gateless.get("/thing")
    async def _thing() -> dict[str, str]:
        return {}

    probe.include_router(gateless)
    probe_client = TestClient(probe, raise_server_exceptions=False)
    assert operations(probe) == [("GET", "/api/v2/forgot/thing")]
    assert probe_client.get("/api/v2/forgot/thing").status_code != 401
    assert expected("/api/v2/forgot/thing") is None


def resolved(path: str) -> str:
    """The endpoint the router picks for a GET, read from the request's scope."""
    seen: dict[str, object] = {}

    async def spy(scope, receive, send):
        await app(scope, receive, send)
        seen.update(scope)

    TestClient(spy, raise_server_exceptions=False).get(path)
    endpoint = seen.get("endpoint")
    return getattr(endpoint, "__name__", "")


def test_a_literal_path_is_not_captured_by_its_parameterised_sibling():
    # `leaderboard` is a literal where {match_id} also lives: if the parameter
    # won, the bot would read a match whose id is the word "leaderboard".
    assert resolved("/api/v2/matches/leaderboard") == "get_leaderboard"
    assert resolved("/api/v2/matches/sub-quota") == "get_sub_quota"


def test_the_parameterised_sibling_still_resolves():
    # Proving a literal wins is worthless if /{match_id} matches nothing.
    assert resolved("/api/v2/matches/652f1a2b3c4d5e6f7a8b9c0d") == "get_match"
