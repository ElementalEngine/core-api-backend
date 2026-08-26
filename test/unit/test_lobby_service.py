"""Creation: what the document carries, and who gets a seat.

Pure -- a fake repository and a fake seasons cache, no database (D59, D60).
What should break these: seating an over-large roster, seating duplicates,
assigning teams at creation, forgetting the season stamp, writing a field
the lobby has not decided yet, or resolving the shape after the season
lookup so a malformed request reaches the database.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.features.lobbies.modes import InvalidLobbyShape, resolve_shape
from app.features.lobbies.schemas import CreateLobbyRequest
from app.features.lobbies.service import (
    LobbyService,
    build_lobby_document,
    for_the_wire,
    seat_the_roster,
)

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
SEASON = {"_id": "S1", "edition": "civ6", "label": "Season 6", "started_at": NOW}


def request(**overrides):
    body = {
        "guild_id": "g1",
        "channel_id": "c1",
        "host_discord_id": "alice",
        "edition": "civ6",
        "game_type": "ffa",
    }
    body.update(overrides)
    return CreateLobbyRequest(**body)


class FakeSeasons:
    def __init__(self):
        self.asked = []

    async def get_current_season(self, edition):
        self.asked.append(edition)
        return dict(SEASON)


class FakeRepo:
    def __init__(self, open_lobbies=None):
        self.inserted = None
        self.queries = []
        self._open = open_lobbies or []

    async def insert_lobby(self, document):
        self.inserted = document
        return {**document, "_id": "L1"}

    async def find_open(self, guild_id, channel_id=None, edition=None, game_type=None):
        self.queries.append((guild_id, channel_id, edition, game_type))
        return list(self._open)


# --- seating ------------------------------------------------------------


@pytest.mark.parametrize(
    "roster,seats",
    [
        (["a", "b", "c"], 3),
        ([], 0),
        (["a"] * 3, 1),  # deduplicated
        (["a", "b", "a"], 2),
        (["", "a"], 1),  # empty ids dropped
    ],
)
def test_seating_dedupes_and_drops_blanks(roster, seats):
    shape = resolve_shape("ffa")
    assert len(seat_the_roster(roster, shape)) == seats


def test_a_roster_that_does_not_fit_seats_nobody():
    # Seven in voice, a 3v3. Seating an arbitrary first six would exclude
    # people by list order.
    shape = resolve_shape("teamer", 2, 3)
    assert seat_the_roster([f"p{i}" for i in range(7)], shape) == []
    assert len(seat_the_roster([f"p{i}" for i in range(6)], shape)) == 6


def test_seats_are_indexed_from_zero_with_no_team():
    shape = resolve_shape("teamer", 3, 3)
    seats = seat_the_roster(["a", "b", "c"], shape)
    assert [s["seat_index"] for s in seats] == [0, 1, 2]
    # Seating means "you are in this lobby", never "you are on red".
    assert all(s["team"] is None for s in seats)
    # D71: a seat exists only when occupied; discord_id never null or absent.
    assert all(s["discord_id"] for s in seats)


def test_duplicates_never_produce_two_seats_for_one_player():
    # Neither index catches this: multikey keys are de-duplicated per
    # document (D176), and the $ne write guard does not apply to an insert.
    shape = resolve_shape("ffa")
    seats = seat_the_roster(["a", "b", "a", "b", "c"], shape)
    ids = [s["discord_id"] for s in seats]
    assert ids == ["a", "b", "c"]
    assert len(ids) == len(set(ids))


# --- the document -------------------------------------------------------


def test_document_stamps_the_season_and_derived_shape():
    shape = resolve_shape("teamer", 3, 3)
    doc = build_lobby_document(request(game_type="teamer"), shape, SEASON, NOW)
    assert (doc["season_id"], doc["season_label"]) == ("S1", "Season 6")
    assert (doc["number_teams"], doc["team_size"]) == (3, 3)
    assert (doc["seat_count"], doc["min_seats"]) == (9, 9)
    assert (doc["phase"], doc["revision"]) == ("lobby", 1)
    assert doc["created_at"] == doc["updated_at"] == NOW


def test_undecided_fields_are_absent_not_null():
    # closed_at absent is what the partial filters select (D175). The rest
    # belong to phases that have not run; writing them as null would claim
    # a decision nobody made.
    doc = build_lobby_document(request(), resolve_shape("ffa"), SEASON, NOW)
    for field in (
        "closed_at",
        "draft_mode",
        "map_type",
        "starting_age",
        "pool_size",
        "pool_appearances",
        "host_bans",
        "majority_bans",
        "bans_by_seat",
        "turn_expires_at",
        "posted_at",
        "revealed_at",
        "cancel_reason",
    ):
        assert field not in doc, f"{field} should be absent at creation"


def test_instance_id_is_omitted_when_absent_and_kept_when_given():
    shape = resolve_shape("ffa")
    assert "instance_id" not in build_lobby_document(request(), shape, SEASON, NOW)
    with_id = build_lobby_document(request(instance_id="i9"), shape, SEASON, NOW)
    assert with_id["instance_id"] == "i9"


# --- the service --------------------------------------------------------


def test_create_resolves_the_shape_and_asks_for_the_right_season():
    repo, seasons = FakeRepo(), FakeSeasons()
    result = asyncio.run(
        LobbyService(repo, seasons).create(
            request(edition="civ7", game_type="teamer", number_teams=2, team_size=3)
        )
    )
    assert seasons.asked == ["civ7"]
    assert repo.inserted["seat_count"] == 6
    assert result["_id"] == "L1"


def test_create_refuses_a_bad_shape_before_touching_the_database():
    repo, seasons = FakeRepo(), FakeSeasons()
    with pytest.raises(InvalidLobbyShape):
        asyncio.run(LobbyService(repo, seasons).create(request(game_type="teamer")))
    assert repo.inserted is None
    assert seasons.asked == []


def test_resolve_active_passes_the_channel_and_returns_one_or_none():
    repo = FakeRepo(open_lobbies=[{"_id": "L1"}])
    assert asyncio.run(LobbyService(repo, FakeSeasons()).resolve_active("g1", "c1"))
    assert repo.queries == [("g1", "c1", None, None)]
    empty = LobbyService(FakeRepo(), FakeSeasons())
    assert asyncio.run(empty.resolve_active("g1", "c1")) is None


class FakeObjectId:
    """Stands in for bson.ObjectId: str()s to a hex string and is otherwise
    not JSON-serialisable, which is exactly how the real one behaves."""

    def __init__(self, hex_value):
        self._hex = hex_value

    def __str__(self):
        return self._hex


def test_the_wire_form_stringifies_both_object_ids():
    # ⚠ FastAPI raised "'ObjectId' object is not iterable" on the first
    # live create -- AFTER the write landed, so the caller saw a 500 for a
    # lobby that exists. Nothing typed this boundary: lobbies is the only v2
    # feature returning a raw document rather than a response model (D179).
    stored = {
        "_id": FakeObjectId("aaa"),
        "season_id": FakeObjectId("bbb"),
        "guild_id": "g1",
        "seats": [{"seat_index": 0, "discord_id": "alice", "team": None}],
    }
    wire = for_the_wire(stored)
    assert wire["_id"] == "aaa"
    assert wire["season_id"] == "bbb"
    assert isinstance(wire["_id"], str) and isinstance(wire["season_id"], str)
    # Everything else passes through untouched.
    assert wire["guild_id"] == "g1"
    assert wire["seats"] == stored["seats"]


def test_the_wire_form_never_mutates_the_stored_document():
    stored = {"_id": FakeObjectId("aaa"), "season_id": FakeObjectId("bbb")}
    for_the_wire(stored)
    assert not isinstance(stored["_id"], str)


def test_a_missing_object_id_is_left_alone_rather_than_stringified():
    # "None" as a string would be worse than a null.
    assert for_the_wire({"_id": None, "season_id": None}) == {
        "_id": None,
        "season_id": None,
    }


def test_create_returns_the_wire_form():
    repo, seasons = FakeRepo(), FakeSeasons()
    repo.insert_lobby = lambda doc: _returns({**doc, "_id": FakeObjectId("L1")})
    result = asyncio.run(LobbyService(repo, seasons).create(request()))
    assert result["_id"] == "L1"


async def _returns(value):
    return value


def test_browse_passes_its_filters_and_never_a_channel():
    repo = FakeRepo()
    asyncio.run(LobbyService(repo, FakeSeasons()).browse("g1", "civ6", "ffa"))
    assert repo.queries == [("g1", None, "civ6", "ffa")]
