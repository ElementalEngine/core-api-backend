"""Every mounted route carries a service-token gate, or is public on purpose.

D169. This replaces the structural half of test_mito_gate.py, which looped a
hand-maintained tuple of routers and had already fallen behind: it omitted
`matches_v2_router`, so eight live routes were unasserted (Correction 65). A
tuple can only ever check what somebody remembered to add to it.

Derived from `app.routes` instead, so a new router without a gate fails here
by construction rather than by recollection. Adding a route to PUBLIC_PATHS
is the only way to exempt one, and that is a visible, reviewable act.

⚠ C5 puts two routers on ONE prefix under DIFFERENT tokens, which is exactly
what a per-router tuple cannot express. That is also why route RESOLUTION
lives here rather than in its own file: section 6b's hazard is a gate
hazard -- a path resolving to the wrong route is checked against the wrong
token, and no handler test can see it because the wrong handler runs fine.
"""

from __future__ import annotations

from fastapi.routing import APIRoute
from starlette.routing import Match

from app.core import dependencies
from app.core.dependencies import (
    require_activity_token,
    require_any_service_token,
    require_mito_token,
)
from app.main import app

# Derived, not listed. CP3 wrote this as a frozenset of four callables and
# `require_activity_token` was invisible to it the moment CP4b added it --
# the same enumeration problem D169 removed one level up, where a hardcoded
# tuple of ROUTERS had already fallen behind by one (Correction 65).
GATES = frozenset(
    value
    for name, value in vars(dependencies).items()
    if name.startswith("require_") and callable(value)
)

# Public by design. `/oauth/discord/callback` is auth's `public_router` --
# Discord calls it, so it cannot carry a service token. The rest are health
# and FastAPI's own documentation endpoints (§4 item 33).
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


def gate_callables(route) -> set:
    """Router-level dependencies reach a route by two paths depending on the
    FastAPI version; read both so this cannot silently return nothing."""
    found = set()
    for dep in getattr(route, "dependencies", ()) or ():
        call = getattr(dep, "dependency", None)
        if call is not None:
            found.add(call)
    dependant = getattr(route, "dependant", None)
    for sub in getattr(dependant, "dependencies", ()) or ():
        call = getattr(sub, "call", None)
        if call is not None:
            found.add(call)
    return found


def api_routes(application=app):
    return [r for r in application.routes if isinstance(r, APIRoute)]


def resolve(method: str, path: str):
    """The route the router would pick, in table order, by its own matching.

    `route.matches` is the function Starlette itself calls, so this cannot
    drift from the real algorithm the way a hand-rolled regex walk would.
    Only a FULL match counts; a path match with the wrong method is PARTIAL
    and the real router keeps looking too.
    """
    scope = {"type": "http", "method": method, "path": path, "root_path": ""}
    for route in app.routes:
        match, _ = route.matches(scope)
        if match is Match.FULL:
            return route
    return None


def test_the_extraction_finds_a_gate_on_a_route_known_to_have_one():
    # ⚠ Without this, an extraction that returned nothing would make the test
    # below report "no ungated routes" while checking nothing at all -- green
    # for a reason unrelated to what it claims.
    by_path = {r.path: gate_callables(r) for r in api_routes()}
    assert require_mito_token in by_path["/api/v2/matches/leaderboard"]
    assert require_any_service_token in by_path["/api/v2/civ-data/{edition}"]


def test_every_mounted_route_is_gated_or_explicitly_public():
    ungated = sorted(
        f"{sorted(r.methods)} {r.path}"
        for r in api_routes()
        if r.path not in PUBLIC_PATHS and not (gate_callables(r) & GATES)
    )
    assert not ungated, f"routes with no service-token gate: {ungated}"


def test_no_public_path_is_stale():
    # A path removed from the app but left in PUBLIC_PATHS would silently
    # widen the exemption for whatever later takes that path.
    mounted = {r.path for r in app.routes if hasattr(r, "path")}
    assert PUBLIC_PATHS <= mounted, f"not mounted: {sorted(PUBLIC_PATHS - mounted)}"


def test_the_check_fails_on_a_router_that_forgot_its_gate():
    # The case it can fail on. Without this the two tests above pass equally
    # well on a checker that never flags anything.
    from fastapi import APIRouter, FastAPI

    probe = FastAPI()
    gateless = APIRouter(prefix="/api/v2/forgot")

    @gateless.get("/thing")
    async def _thing() -> dict:
        return {}

    probe.include_router(gateless)
    offenders = [
        r.path
        for r in api_routes(probe)
        if r.path not in PUBLIC_PATHS and not (gate_callables(r) & GATES)
    ]
    assert "/api/v2/forgot/thing" in offenders


# --- route resolution (C5 section 6b, section 4 items 112 and 113) ------

LOBBY_PATH = "/api/v2/lobbies/652f1a2b3c4d5e6f7a8b9c0d"


def test_resolution_finds_a_route_for_an_unambiguous_path():
    # ⚠ Without this, a wrong scope dict would resolve nothing at all and
    # every assertion below -- each of the form "did NOT resolve to {id}" --
    # would pass while checking nothing.
    route = resolve("POST", "/api/v2/lobbies")
    assert route is not None and route.path == "/api/v2/lobbies"


def test_a_literal_path_is_not_captured_by_its_parameterised_sibling():
    assert resolve("GET", "/api/v2/lobbies/active").path == "/api/v2/lobbies/active"
    assert (
        resolve("GET", "/api/v2/matches/leaderboard").path
        == "/api/v2/matches/leaderboard"
    )


def test_the_parameterised_sibling_still_resolves():
    # Proving /active wins is worthless if /{lobby_id} matches nothing.
    assert resolve("GET", LOBBY_PATH).path == "/api/v2/lobbies/{lobby_id}"


def test_each_lobby_route_carries_its_own_gate_not_merely_a_gate():
    # The suite above asserts SOME gate on every route; if both C5 routers
    # had taken the same dependency it would still pass. Section 6b makes
    # that gate-affecting, because the two share a prefix and resolve as one
    # ordered table.
    assert require_mito_token in gate_callables(resolve("POST", "/api/v2/lobbies"))
    for method, path in (
        ("GET", "/api/v2/lobbies"),
        ("GET", "/api/v2/lobbies/active"),
        ("GET", LOBBY_PATH),
    ):
        gates = gate_callables(resolve(method, path))
        assert require_activity_token in gates, f"{method} {path}"
        assert require_mito_token not in gates, f"{method} {path}"
