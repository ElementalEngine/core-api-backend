"""The v2 route table (C1, contract-set §6b).

A literal path and a parameterised path at the same depth are resolved in
registration order, so `/matches/leaderboard` must be declared before
`/matches/{match_id}` or it is captured as an id and answers 404 -- or a
malformed-ObjectId 500. This is a property of the assembled table, not of
any handler, so no handler test can catch it.

What should break this file: reordering the decorators in router_v2.py, or
adding a route without amending C1.

The filter is scoped to /api/v2/matches: the v2 prefix already carries
civ-data from S5, and a bare /api/v2 filter would count it.
"""

from starlette.routing import Match

from app.features.matches.schemas_v2 import PlayersPatch, SeatPatchIn, UploadResponse
from app.main import app

EXPECTED = {
    ("POST", "/api/v2/matches"),
    ("GET", "/api/v2/matches/leaderboard"),
    ("GET", "/api/v2/matches/sub-quota"),
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


def test_c1_declares_exactly_its_v2_routes():
    assert set(_v2_routes()) == EXPECTED


def test_leaderboard_is_declared_before_the_id_route():
    paths = [p for _, p in _v2_routes()]
    assert paths.index("/api/v2/matches/leaderboard") < paths.index(
        "/api/v2/matches/{match_id}"
    )


def test_sub_quota_is_declared_before_the_id_route():
    paths = [p for _, p in _v2_routes()]
    assert paths.index("/api/v2/matches/sub-quota") < paths.index(
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


def test_sub_quota_resolves_to_its_own_handler():
    scope = _scope("GET", "/api/v2/matches/sub-quota")
    matched = [
        route
        for route in app.routes
        if getattr(route, "path", "").startswith("/api/v2/matches")
        and route.matches(scope)[0] == Match.FULL
    ]
    assert matched, "no v2 route matched /api/v2/matches/sub-quota"
    assert matched[0].name == "get_sub_quota", matched[0].name


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


def test_the_upload_answer_keeps_its_duplicate_flags():
    # Mite words a duplicate upload by these two fields; a response model
    # that does not declare them strips them silently.
    upload = next(
        route
        for route in app.routes
        if getattr(route, "path", "") == "/api/v2/matches"
        and "POST" in (getattr(route, "methods", None) or set())
    )
    assert upload.response_model is UploadResponse
    assert {"repeated", "repeated_by"} <= set(UploadResponse.model_fields)


def test_a_leader_set_without_a_civ_brings_its_own():
    body = PlayersPatch(
        players=[
            SeatPatchIn(seat=0, leader="LEADER_A"),
            SeatPatchIn(seat=1, leader="LEADER_B", civ="CIVILIZATION_KEPT"),
            SeatPatchIn(seat=2, leader="LEADER_UNMAPPED"),
        ]
    )
    body.fill_civs({"LEADER_A": "CIVILIZATION_A", "LEADER_UNMAPPED": None})
    civs = [p.civ for p in body.to_seat_patches()]
    assert civs[:2] == ["CIVILIZATION_A", "CIVILIZATION_KEPT"]
    assert str(civs[2]) == "UNSET"


class Seasons:
    """The seasons collection: one open season, and one older one by id."""

    async def find_one(self, flt, projection=None, sort=None):
        if "_id" in flt:
            return {"_id": "s5", "label": "Season 5"} if flt["_id"] == "s5" else None
        return {"_id": "s6", "edition": flt["edition"], "label": "Season 6"}


def test_a_match_answer_names_the_season_it_was_approved_in_else_the_open_one():
    import asyncio

    from app.core.constants import COL_SEASONS, GAMES_DB
    from app.features.matches.router_v2 import _named
    from app.features.seasons.repository import clear_cache

    clear_cache()
    db = {GAMES_DB: {COL_SEASONS: Seasons()}}
    approved = asyncio.run(_named(db, {"game": "civ6", "season_id": "s5"}))
    pending = asyncio.run(_named(db, {"game": "civ6"}))
    clear_cache()
    assert approved["season_label"] == "Season 5"
    assert pending["season_label"] == "Season 6"
