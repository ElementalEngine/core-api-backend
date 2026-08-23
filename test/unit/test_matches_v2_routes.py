"""The v2 route table (C1, contract-set §6b).

A literal path and a parameterised path at the same depth are resolved in
registration order, so `/matches/leaderboard` must be declared before
`/matches/{match_id}` or it is captured as an id and answers 404 -- or a
malformed-ObjectId 500. This is a property of the assembled table, not of
any handler, so no handler test can catch it.

What should break this file: reordering the decorators in router_v2.py, or
adding a ninth route without amending C1.

The filter is scoped to /api/v2/matches: the v2 prefix already carries
civ-data from S5, and a bare /api/v2 filter would count it.
"""

from starlette.routing import Match

from app.main import app

EXPECTED = {
    ("POST", "/api/v2/matches"),
    ("GET", "/api/v2/matches/leaderboard"),
    ("GET", "/api/v2/matches/{match_id}"),
    ("PATCH", "/api/v2/matches/{match_id}/players"),
    ("POST", "/api/v2/matches/{match_id}/approve"),
    ("POST", "/api/v2/matches/{match_id}/contest"),
    ("POST", "/api/v2/matches/{match_id}/revert"),
    ("DELETE", "/api/v2/matches/{match_id}"),
}


def _v2_routes():
    out = []
    for route in app.routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", None) or set()
        if path.startswith("/api/v2/matches") and methods:
            verb = sorted(methods - {"HEAD", "OPTIONS"})[0]
            out.append((verb, path))
    return out


def _scope(method, path):
    return {
        "type": "http",
        "method": method,
        "path": path,
        "path_params": {},
        "headers": [],
        "query_string": b"",
        "root_path": "",
    }


def test_c1_declares_exactly_eight_v2_routes():
    assert set(_v2_routes()) == EXPECTED


def test_leaderboard_is_declared_before_the_id_route():
    paths = [p for _, p in _v2_routes()]
    assert paths.index("/api/v2/matches/leaderboard") < paths.index(
        "/api/v2/matches/{match_id}"
    )


def test_leaderboard_resolves_to_its_own_handler():
    # Declaration order is the mechanism; resolution is the property. This
    # is the assertion contract-set §6b actually asks for.
    scope = _scope("GET", "/api/v2/matches/leaderboard")
    matched = [
        route
        for route in app.routes
        if getattr(route, "path", "").startswith("/api/v2/matches")
        and route.matches(scope)[0] == Match.FULL
    ]
    assert matched, "no v2 route matched /api/v2/matches/leaderboard"
    assert matched[0].name == "get_leaderboard", matched[0].name


def test_an_id_still_resolves_to_the_id_handler():
    # The canary: if leaderboard were greedy, this would break instead.
    scope = _scope("GET", "/api/v2/matches/652f1a2b3c4d5e6f7a8b9c0d")
    matched = [
        route
        for route in app.routes
        if getattr(route, "path", "").startswith("/api/v2/matches")
        and route.matches(scope)[0] == Match.FULL
    ]
    assert matched, "no v2 route matched a match id"
    assert matched[0].name == "get_match", matched[0].name


def test_v1_matches_surface_is_untouched():
    # C10: new routes are additive. v1 keeps its sixteen.
    v1 = [
        p
        for p in (getattr(r, "path", "") for r in app.routes)
        if p.startswith("/api/v1") and "match" in p
    ]
    assert v1, "the v1 matches surface disappeared"
