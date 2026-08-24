"""C2's four stats routes, and the one field that differs from v1.

The route table has no path parameters, so it carries none of C1's
ordering hazard -- what it can get wrong instead is the pair of legacy
PUTs surviving the cutover, or `wins` leaking onto a surface that C2
requires to say which counter it shows.

What should break this file: adding a fifth v2 stats route without
amending C2, resurrecting either legacy PUT, or renaming rating_gains.
"""

from app.features.stats.schemas import StatRow
from app.features.stats.schemas_v2 import StatRowV2, UserStatsResponseV2
from app.main import app

EXPECTED = {
    ("GET", "/api/v2/stats/user"),
    ("POST", "/api/v2/stats/batch"),
    ("POST", "/api/v2/stats/team-gen"),
    ("PUT", "/api/v2/stats/reset/user"),
}

GONE = {"/api/v1/get-user-stats/", "/api/v1/get-user-stats-batch/"}


def _routes(prefix):
    out = set()
    for route in app.routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", None) or set()
        if path.startswith(prefix) and methods:
            for verb in methods - {"HEAD", "OPTIONS"}:
                out.add((verb, path))
    return out


def test_the_four_v2_stats_routes_are_registered():
    assert _routes("/api/v2/stats") == EXPECTED


def test_the_legacy_put_pair_is_gone():
    assert {p for _, p in _routes("/api/v1")} & GONE == set()


def test_the_v1_stats_routes_survive_until_cutover():
    # C2 is "Replace / hard cutover": Mite v1 still calls these.
    assert ("GET", "/api/v1/stats/user") in _routes("/api/v1/stats")
    assert ("POST", "/api/v1/stats/batch") in _routes("/api/v1/stats")


def test_rating_gains_replaces_wins_and_nothing_else_moves():
    row = StatRow(
        mu=1250,
        sigma=8.25,
        games=41,
        wins=20,
        first=7,
        subbedIn=2,
        subbedOut=1,
    )
    v2 = StatRowV2.from_v1(row)

    assert v2.rating_gains == 20
    assert v2.first == 7
    assert not hasattr(v2, "wins")

    shared = row.model_dump(exclude={"wins"})
    assert v2.model_dump(exclude={"rating_gains"}) == shared


def test_the_v2_response_schema_never_says_wins():
    assert "wins" not in str(UserStatsResponseV2.model_json_schema())
    assert "rating_gains" in str(UserStatsResponseV2.model_json_schema())
