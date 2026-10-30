from app.features.matches.schemas_v2 import PlayersPatch, SeatPatchIn, UploadResponse
from app.main import app


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
