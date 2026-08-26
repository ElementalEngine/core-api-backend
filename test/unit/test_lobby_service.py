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
    InvalidLobbyId,
    LobbyNotFound,
    LobbyService,
    as_lobby_id,
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
    def __init__(self, open_lobbies=None, lobby=None):
        self.inserted = None
        self.queries = []
        self.asked_for = []
        self._open = open_lobbies or []
        self._lobby = lobby

    async def insert_lobby(self, document):
        self.inserted = document
        return {**document, "_id": "L1"}

    async def find_open(self, guild_id, channel_id=None, edition=None, game_type=None):
        self.queries.append((guild_id, channel_id, edition, game_type))
        return list(self._open)

    async def find_by_id(self, lobby_id):
        self.asked_for.append(str(lobby_id))
        return self._lobby


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
    assert asyncio.run(
        LobbyService(repo, FakeSeasons()).resolve_active("g1", "c1", "alice")
    )
    assert repo.queries == [("g1", "c1", None, None)]
    empty = LobbyService(FakeRepo(), FakeSeasons())
    assert asyncio.run(empty.resolve_active("g1", "c1", "alice")) is None


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
    wire = for_the_wire(stored, None)
    assert wire["_id"] == "aaa"
    assert wire["season_id"] == "bbb"
    assert isinstance(wire["_id"], str) and isinstance(wire["season_id"], str)
    # Everything else passes through untouched.
    assert wire["guild_id"] == "g1"
    assert wire["seats"] == stored["seats"]


def test_the_wire_form_never_mutates_the_stored_document():
    stored = {"_id": FakeObjectId("aaa"), "season_id": FakeObjectId("bbb")}
    for_the_wire(stored, None)
    assert not isinstance(stored["_id"], str)


def test_a_missing_object_id_is_left_alone_rather_than_stringified():
    # "None" as a string would be worse than a null.
    assert for_the_wire({"_id": None, "season_id": None}, None) == {
        "_id": None,
        "season_id": None,
        # project_lobby normalises a missing seats array; every stored lobby
        # carries one, so this only shows up on a hand-built fragment.
        "seats": [],
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
    asyncio.run(LobbyService(repo, FakeSeasons()).browse("g1", "alice", "civ6", "ffa"))
    assert repo.queries == [("g1", None, "civ6", "ffa")]


# --- censoring at the wire boundary (D186) ------------------------------

SETTINGS_LOBBY = {
    "_id": "L1",
    "phase": "settings",
    "seats": [
        {"seat_index": 0, "discord_id": "alice", "ballot": {"map": "pangaea"}},
        {"seat_index": 1, "discord_id": "bob", "ballot": {"map": "continents"}},
    ],
}


def seats_by_id(lobby):
    return {seat["discord_id"]: seat for seat in lobby["seats"]}


def test_the_wire_form_censors_before_it_stringifies():
    # The defect this exists for: CP4b's three routes returned the stored
    # document raw, so project_lobby was on no application path at all.
    # alice keeps hers and bob loses his in one assertion pair, so this
    # cannot pass on a boundary that strips every ballot.
    seats = seats_by_id(for_the_wire(SETTINGS_LOBBY, "alice"))
    assert seats["alice"]["ballot"] == {"map": "pangaea"}
    assert "ballot" not in seats["bob"]


def test_an_observer_sees_participation_but_no_ballot():
    wire = for_the_wire(SETTINGS_LOBBY, None)
    assert all("ballot" not in seat for seat in wire["seats"])
    # Counted before the ballots are removed, or it would always be 0 or 1.
    assert wire["ballots_submitted"] == 2


def test_resolve_active_censors_the_lobby_it_returns():
    repo = FakeRepo(open_lobbies=[SETTINGS_LOBBY])
    lobby = asyncio.run(
        LobbyService(repo, FakeSeasons()).resolve_active("g1", "c1", "bob")
    )
    seats = seats_by_id(lobby)
    assert seats["bob"]["ballot"] == {"map": "continents"}
    assert "ballot" not in seats["alice"]


def test_browse_censors_every_lobby_not_only_the_first():
    repo = FakeRepo(open_lobbies=[SETTINGS_LOBBY, SETTINGS_LOBBY])
    lobbies = asyncio.run(LobbyService(repo, FakeSeasons()).browse("g1", "alice"))
    assert len(lobbies) == 2
    assert all("ballot" not in seats_by_id(lobby)["bob"] for lobby in lobbies)


def test_the_create_response_is_unchanged_by_the_projection():
    # CP4b measured this response on the wire, and D186 put the projection on
    # the path: a `lobby`-phase document has neither censored surface, so the
    # projection must neither strip a field, add one, nor alter a value.
    #
    # The timestamps are excluded because the boundary re-encodes them by
    # design (Correction 90) and a test below pins that format exactly. The
    # last assertion is what keeps the exclusion honest -- without it,
    # dropping both timestamps entirely would pass.
    repo, seasons = FakeRepo(), FakeSeasons()
    result = asyncio.run(LobbyService(repo, seasons).create(request()))
    timestamps = {"created_at", "updated_at"}
    stored = {**repo.inserted, "_id": "L1"}
    assert set(result) == set(stored)
    assert {k: v for k, v in result.items() if k not in timestamps} == {
        k: v for k, v in stored.items() if k not in timestamps
    }
    assert all(isinstance(result[key], str) for key in timestamps)


# --- reading one lobby (C5 GET /{id}, D77) ------------------------------

HEX_ID = "652f1a2b3c4d5e6f7a8b9c0d"
READ_LOBBY = {**SETTINGS_LOBBY, "revision": 4}


def read(repo, **kwargs):
    return asyncio.run(LobbyService(repo, FakeSeasons()).read(**kwargs))


def test_read_censors_for_the_caller():
    repo = FakeRepo(lobby=READ_LOBBY)
    seats = seats_by_id(read(repo, lobby_id=HEX_ID, viewer_discord_id="bob"))
    assert repo.asked_for == [HEX_ID]
    assert seats["bob"]["ballot"] == {"map": "continents"}
    assert "ballot" not in seats["alice"]


def test_read_withholds_only_when_the_revision_has_not_moved():
    # Both legs together (D77). The first alone would pass on a read that
    # returns None unconditionally.
    repo = FakeRepo(lobby=READ_LOBBY)
    assert read(repo, lobby_id=HEX_ID, viewer_discord_id="bob", since=4) is None
    moved = read(repo, lobby_id=HEX_ID, viewer_discord_id="bob", since=3)
    assert moved["revision"] == 4
    # No `since` is an unconditional read and can never answer 204.
    assert read(repo, lobby_id=HEX_ID, viewer_discord_id="bob")["revision"] == 4


def test_a_since_ahead_of_the_document_still_serves_the_truth():
    # Only reachable from a client that invented a revision. Answering 204
    # would freeze it on a lobby it has never actually seen.
    repo = FakeRepo(lobby=READ_LOBBY)
    lobby = read(repo, lobby_id=HEX_ID, viewer_discord_id="bob", since=9)
    assert lobby["revision"] == 4


def test_a_malformed_id_is_refused_before_the_database():
    repo = FakeRepo(lobby=READ_LOBBY)
    with pytest.raises(InvalidLobbyId):
        read(repo, lobby_id="nope", viewer_discord_id="bob")
    # C5 section 6b names the malformed-ObjectId 500. The check sits before
    # the round trip, not around it.
    assert repo.asked_for == []


def test_a_missing_lobby_raises_rather_than_returning_none():
    # None means 204 on this path. A miss returning None would tell a poller
    # "nothing has changed" about a lobby that does not exist.
    repo = FakeRepo(lobby=None)
    with pytest.raises(LobbyNotFound):
        read(repo, lobby_id=HEX_ID, viewer_discord_id="bob")


def test_a_well_formed_id_round_trips_to_the_same_hex():
    assert str(as_lobby_id(HEX_ID)) == HEX_ID


# --- one datetime encoding for every route (Correction 90) --------------


def has_datetime(value):
    if isinstance(value, datetime):
        return True
    if isinstance(value, dict):
        return any(has_datetime(item) for item in value.values())
    if isinstance(value, list):
        return any(has_datetime(item) for item in value)
    return False


def test_a_datetime_leaves_the_boundary_as_rfc3339_with_z():
    # Measured on the wire: /active emitted ...237000Z through Pydantic while
    # GET /{id}, which needs response_model=None for its 204, emitted
    # ...237000+00:00 through jsonable_encoder. Z is the one three routes
    # already spoke, so this moves the fourth into line rather than the
    # other three out of it.
    stored = {
        "_id": "L1",
        "created_at": datetime(2026, 8, 26, 10, 31, 13, 237000, tzinfo=UTC),
    }
    assert for_the_wire(stored, None)["created_at"] == "2026-08-26T10:31:13.237000Z"


def test_no_datetime_survives_the_boundary_at_any_depth():
    # Deliberately broader than the converter, which handles the top level --
    # the document's actual shape today. If a later phase nests a datetime
    # inside seats, this fails and the converter grows for a stated reason,
    # rather than one route quietly disagreeing with three again.
    stored = {
        "_id": "L1",
        "created_at": datetime(2026, 8, 26, tzinfo=UTC),
        "updated_at": datetime(2026, 8, 26, tzinfo=UTC),
        "seats": [{"seat_index": 0, "discord_id": "alice"}],
    }
    assert has_datetime(stored), "fixture must contain what the test looks for"
    assert not has_datetime(for_the_wire(stored, None))
