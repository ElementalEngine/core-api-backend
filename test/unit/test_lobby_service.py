"""Creation: what the document carries, and who gets a seat.

Pure -- a fake repository and a fake seasons cache, no database (D59, D60).
What should break these: seating an over-large roster, seating duplicates,
assigning teams at creation, forgetting the season stamp, writing a field
the lobby has not decided yet, or resolving the shape after the season
lookup so a malformed request reaches the database.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from app.features.lobbies.modes import (
    InvalidLobbyShape,
    InvalidSeating,
    resolve_shape,
)
from app.features.lobbies.projection import BALLOT, PICK, POOL, POOL_APPEARANCES
from app.features.lobbies.schemas import (
    ChangeSeatRequest,
    CreateLobbyRequest,
    SubmitBallotRequest,
    SubmitBansRequest,
    SubmitPickRequest,
)
from app.features.lobbies.service import (
    STALE_AFTER,
    InvalidLobbyId,
    LobbyNotFound,
    LobbyService,
    NotSeated,
    NotTheHost,
    NotYourTurn,
    PickIsFinal,
    SeatChangeRefused,
    as_lobby_id,
    build_lobby_document,
    for_the_wire,
    rearranged,
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
    def __init__(
        self,
        open_lobbies=None,
        lobby=None,
        written=None,
        reread=None,
        stale=None,
        applied=None,
    ):
        self.inserted = None
        self.queries = []
        self.asked_for = []
        self.writes = []
        self.evictions = []
        self.changes = []
        self._open = open_lobbies or []
        self._lobby = lobby
        self._written = written
        self._reread = reread
        self._stale = stale or []
        self._applied = applied

    async def insert_lobby(self, document):
        self.inserted = document
        return {**document, "_id": "L1"}

    async def evict_stale(self, players, cutoff, now):
        self.evictions.append((sorted(players), cutoff))
        return self._stale

    async def find_open(self, guild_id, channel_id=None, edition=None, game_type=None):
        self.queries.append((guild_id, channel_id, edition, game_type))
        return list(self._open)

    async def find_by_id(self, lobby_id):
        self.asked_for.append(str(lobby_id))
        # A refused write re-reads to say WHY (spec section 9), so the second
        # look can legitimately see a different document.
        if self._reread is not None and len(self.asked_for) > 1:
            return self._reread
        return self._lobby

    async def replace_seats(
        self, lobby_id, expected_revision, seats, now, *, absent_player=None
    ):
        self.writes.append((expected_revision, seats, absent_player))
        # ⚠ The real one uses ReturnDocument.AFTER, so it returns the document
        # INCLUDING the seats just written. Returning the pre-write document
        # made every caller of this return value test against a lie -- it is
        # what `_resolve_settings` reads to decide whether everyone has voted.
        if self._written is None:
            return None
        return {**self._written, "seats": seats}

    async def apply_changes(self, lobby_id, expected_revision, changes, now):
        self.changes.append((expected_revision, changes))
        if self._applied is None:
            return None
        return {**self._applied, **changes}


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


# --- D179's classification guard ----------------------------------------
#
# D179 refused the built-view shape in the projection: enumerating 26 public
# fields to protect one conditional one is ceremony, and an allowlist catches
# a DERIVED leak like pool_appearances no better than a denylist. What it
# accepted instead was this -- the enumeration lives in the check, where its
# only job is to make adding a field a decision.
#
# The censored names are imported from projection.py rather than retyped, so
# a new censored surface cannot be added there and forgotten here.

CENSORED_LOBBY_FIELDS = {POOL_APPEARANCES}
CENSORED_SEAT_FIELDS = {BALLOT, POOL, PICK}

PUBLIC_LOBBY_FIELDS = {
    "guild_id",
    "channel_id",
    "host_discord_id",
    "instance_id",
    "starting_age",
    "source",
    "season_id",
    "season_label",
    "edition",
    "game_type",
    "number_teams",
    "team_size",
    "seat_count",
    "min_seats",
    "seats",
    "phase",
    "revision",
    "created_at",
    "updated_at",
}
PUBLIC_SEAT_FIELDS = {"seat_index", "discord_id", "team"}


def built_document():
    return build_lobby_document(
        request(
            game_type="teamer",
            number_teams=3,
            team_size=3,
            instance_id="i9",
            roster=["alice", "bob", "carol"],
        ),
        resolve_shape("teamer", 3, 3),
        SEASON,
        NOW,
    )


def unclassified(document):
    stray = set(document) - PUBLIC_LOBBY_FIELDS - CENSORED_LOBBY_FIELDS
    for seat in document.get("seats") or []:
        stray |= set(seat) - PUBLIC_SEAT_FIELDS - CENSORED_SEAT_FIELDS
    return stray


def test_every_field_the_builder_writes_is_classified():
    # ⚠ The projection STRIPS rather than builds, so a field added to the
    # document is exposed by default. That is how pool_appearances was nearly
    # missed -- found by reading section 8 against section 6, not because
    # anything forced the question. This forces it.
    document = built_document()
    assert document["seats"], "the fixture must seat someone or the seat leg is vacuous"
    assert not unclassified(document), sorted(unclassified(document))


def test_the_guard_reports_a_field_that_is_neither_public_nor_censored():
    # D86 Rule 1: the check has to be able to fail for the reason it exists.
    # Without this, the test above passes on a guard that returns nothing.
    smuggled = {**built_document(), "unlisted_field": "x"}
    assert unclassified(smuggled) == {"unlisted_field"}
    seated = built_document()
    seated["seats"][0]["unlisted_seat_field"] = "x"
    assert unclassified(seated) == {"unlisted_seat_field"}


# --- changing a seat (C5 PATCH /{id}/seats) -----------------------------

OPEN_LOBBY = {
    "_id": "L1",
    "phase": "lobby",
    "revision": 3,
    "game_type": "ffa",
    "number_teams": None,
    "team_size": None,
    "host_discord_id": "alice",
    "seats": [{"seat_index": 0, "discord_id": "alice", "team": None}],
}


def change(repo, actor="alice", **body):
    body.setdefault("action", "place")
    body.setdefault("expected_revision", 3)
    return asyncio.run(
        LobbyService(repo, FakeSeasons()).change_seat(
            HEX_ID, actor, ChangeSeatRequest(**body)
        )
    )


def test_rearranging_keeps_the_seat_a_move_carries():
    # A ballot -- later a pool and a pick -- belongs to the player, not the
    # position. Rebuilding the seat would silently drop it.
    seats = [{"seat_index": 0, "discord_id": "alice", "team": 0, "ballot": {"m": "p"}}]
    moved = rearranged(
        seats,
        "alice",
        ChangeSeatRequest(expected_revision=3, action="place", seat_index=7, team=1),
    )
    assert moved == [
        {"seat_index": 7, "discord_id": "alice", "team": 1, "ballot": {"m": "p"}}
    ]


def test_rearranging_never_closes_a_gap():
    # ⚠ O-19b: civup compacts before chunking, which moves a player across a
    # team boundary that nobody asked to cross.
    seats = [
        {"seat_index": 0, "discord_id": "a"},
        {"seat_index": 5, "discord_id": "b"},
    ]
    placed = rearranged(
        seats, "c", ChangeSeatRequest(expected_revision=3, action="place", seat_index=2)
    )
    assert [seat["seat_index"] for seat in placed] == [0, 2, 5]


def test_leaving_removes_only_the_target():
    seats = [
        {"seat_index": 0, "discord_id": "a"},
        {"seat_index": 1, "discord_id": "b"},
    ]
    left = rearranged(
        seats, "a", ChangeSeatRequest(expected_revision=3, action="leave")
    )
    assert [seat["discord_id"] for seat in left] == ["b"]


def test_a_place_writes_the_array_it_validated():
    repo = FakeRepo(lobby=OPEN_LOBBY, written={**OPEN_LOBBY, "revision": 4})
    change(repo, actor="bob", seat_index=4)
    expected_revision, seats, absent = repo.writes[0]
    assert expected_revision == 3
    assert [(s["discord_id"], s["seat_index"]) for s in seats] == [
        ("alice", 0),
        ("bob", 4),
    ]
    # ⚠ D176's clause, and only where D176 measured it: bob was not seated.
    assert absent == "bob"


def test_moving_a_seated_player_carries_no_absent_clause():
    # The $ne would refuse the player their own seat.
    repo = FakeRepo(lobby=OPEN_LOBBY, written={**OPEN_LOBBY, "revision": 4})
    change(repo, actor="alice", seat_index=6)
    assert repo.writes[0][2] is None


def test_only_the_host_may_move_somebody_else():
    repo = FakeRepo(lobby=OPEN_LOBBY, written=OPEN_LOBBY)
    with pytest.raises(NotTheHost):
        change(repo, actor="bob", seat_index=4, discord_id="carol")
    assert repo.writes == []
    # alice IS the host.
    change(repo, actor="alice", seat_index=4, discord_id="carol")
    assert repo.writes[0][1][-1]["discord_id"] == "carol"


def test_an_illegal_arrangement_never_reaches_the_write():
    repo = FakeRepo(lobby=OPEN_LOBBY, written=OPEN_LOBBY)
    with pytest.raises(InvalidSeating):
        change(repo, actor="bob", seat_index=0)  # alice holds seat 0
    assert repo.writes == []


def test_seats_are_settled_once_the_lobby_leaves_lobby_phase():
    # D189. Ballots are per seat and turn_index points into the seating, so a
    # move from `settings` on corrupts state no validator reads.
    repo = FakeRepo(lobby={**OPEN_LOBBY, "phase": "settings"}, written=OPEN_LOBBY)
    with pytest.raises(SeatChangeRefused) as exc:
        change(repo, actor="bob", seat_index=4)
    assert repo.writes == []
    assert (exc.value.expected, exc.value.current) == (3, 3)


def test_a_stale_revision_reports_both_numbers():
    repo = FakeRepo(
        lobby=OPEN_LOBBY, written=None, reread={**OPEN_LOBBY, "revision": 9}
    )
    with pytest.raises(SeatChangeRefused) as exc:
        change(repo, actor="bob", seat_index=4)
    assert (exc.value.expected, exc.value.current) == (3, 9)
    assert "moved on" in str(exc.value)


def test_a_lost_race_at_the_same_revision_names_the_seat_not_the_revision():
    # ⚠ Spec section 9: matched-count zero is stale revision OR already
    # seated, and only the re-read tells them apart. Same revision means
    # D176's $ne refused, not that the caller is behind.
    repo = FakeRepo(lobby=OPEN_LOBBY, written=None, reread=OPEN_LOBBY)
    with pytest.raises(SeatChangeRefused) as exc:
        change(repo, actor="bob", seat_index=4)
    assert (exc.value.expected, exc.value.current) == (3, 3)
    assert "already holds a seat" in str(exc.value)


# --- D177's eviction ----------------------------------------------------


def test_creation_evicts_stale_lobbies_holding_the_roster():
    # ⚠ D74's timers are lazy and an abandoned lobby gets no read to
    # evaluate them, so one_active_seat_per_player holds the seat forever
    # and the only symptom is a bare E11000. The read that triggers
    # evaluation has to be the NEW lobby's creation.
    repo = FakeRepo(stale=[{"_id": "OLD", "channel_id": "c9", "updated_at": NOW}])
    asyncio.run(
        LobbyService(repo, FakeSeasons()).create(request(roster=["alice", "bob"]))
    )
    assert repo.evictions[0][0] == ["alice", "bob"]
    assert repo.inserted is not None, "creation proceeds after the eviction"


def test_the_cutoff_is_an_hour_behind_the_creation():
    repo = FakeRepo()
    asyncio.run(LobbyService(repo, FakeSeasons()).create(request(roster=["alice"])))
    _, cutoff = repo.evictions[0]
    assert repo.inserted["created_at"] - cutoff == STALE_AFTER


def test_an_empty_roster_evicts_nothing():
    # Nobody is being seated, so no seat can be held elsewhere -- and a
    # query per empty create would be a round trip for no reason.
    repo = FakeRepo()
    asyncio.run(LobbyService(repo, FakeSeasons()).create(request()))
    assert repo.evictions == []


def test_eviction_asks_about_deduplicated_players_only():
    repo = FakeRepo()
    asyncio.run(
        LobbyService(repo, FakeSeasons()).create(request(roster=["a", "a", "", "b"]))
    )
    assert repo.evictions[0][0] == ["a", "b"]


# --- starting the vote and resolving it (D190, D191, D194) --------------

VOTING = {
    **OPEN_LOBBY,
    "phase": "settings",
    "edition": "civ6",
    "min_seats": 2,
    "turn_expires_at": datetime.now(UTC) + timedelta(minutes=5),
    "seats": [
        {"seat_index": 0, "discord_id": "alice", "team": None},
        {"seat_index": 1, "discord_id": "bob", "team": None},
    ],
}
IN_LOBBY = {**VOTING, "phase": "lobby", "turn_expires_at": None}


def start(repo, actor="alice", revision=3):
    return asyncio.run(LobbyService(repo, FakeSeasons()).start(HEX_ID, actor, revision))


def vote(repo, actor="alice", **selections):
    return asyncio.run(
        LobbyService(repo, FakeSeasons()).submit_ballot(
            HEX_ID,
            actor,
            SubmitBallotRequest(expected_revision=3, selections=selections),
        )
    )


def test_starting_opens_the_settings_vote_with_a_deadline():
    repo = FakeRepo(lobby=IN_LOBBY, applied=IN_LOBBY)
    start(repo)
    _, changes = repo.changes[0]
    assert changes["phase"] == "settings"
    assert changes["turn_expires_at"] > datetime.now(UTC)


def test_only_the_host_starts():
    repo = FakeRepo(lobby=IN_LOBBY, applied=IN_LOBBY)
    with pytest.raises(NotTheHost):
        start(repo, actor="bob")
    assert repo.changes == []


def test_starting_below_min_seats_is_refused():
    # ⚠ Not automatic at min_seats either (D190): FFA seats twelve and needs
    # six, so the sixth arrival does not mean nobody else is coming.
    thin = {**IN_LOBBY, "min_seats": 6}
    repo = FakeRepo(lobby=thin, applied=thin)
    with pytest.raises(SeatChangeRefused):
        start(repo)
    assert repo.changes == []


def test_starting_twice_is_refused():
    repo = FakeRepo(lobby=VOTING, applied=VOTING)
    with pytest.raises(SeatChangeRefused):
        start(repo)


def test_an_unseated_player_cannot_vote():
    repo = FakeRepo(lobby=VOTING, written=VOTING, applied=VOTING)
    with pytest.raises(NotSeated):
        vote(repo, actor="mallory", map="pangaea")
    assert repo.writes == []


@pytest.mark.parametrize(
    "selections",
    [{"no_such_question": "a"}, {"map": "no_such_option"}, {"map": ""}],
)
def test_a_ballot_the_catalogue_does_not_recognise_is_refused(selections):
    repo = FakeRepo(lobby=VOTING, written=VOTING, applied=VOTING)
    with pytest.raises(InvalidSeating):
        vote(repo, **selections)
    assert repo.writes == []


def test_a_ballot_is_stored_on_the_voting_seat_only():
    repo = FakeRepo(lobby=VOTING, written=VOTING, applied=VOTING)
    vote(repo, actor="alice", duration="unlimited")
    _, seats, _ = repo.writes[0]
    by_id = {seat["discord_id"]: seat for seat in seats}
    assert by_id["alice"]["ballot"] == {"duration": "unlimited"}
    assert not by_id["bob"].get("ballot")


def test_the_last_ballot_tallies_and_moves_to_bans():
    # ⚠ Both seats have voted, so the phase resolves in the same call the
    # final ballot arrives in -- the caller never waits for a poll tick.
    voted = {
        **VOTING,
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "ballot": {"duration": "unlimited"},
            },
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=voted, written=voted, applied=voted)
    vote(repo, actor="bob", duration="unlimited")
    _, changes = repo.changes[0]
    assert changes["phase"] == "bans"
    assert changes["settings"]["duration"] == "unlimited"


def test_a_ballot_short_of_everyone_does_not_advance():
    repo = FakeRepo(lobby=VOTING, written=VOTING, applied=VOTING)
    vote(repo, actor="alice", duration="unlimited")
    assert repo.changes == []


def test_a_read_past_the_deadline_advances_the_phase():
    # ⚠ D74/D194: nothing sweeps timers, so the POLL has to advance them. A
    # settings phase in a lobby nobody writes to would otherwise never expire.
    expired = {**VOTING, "turn_expires_at": datetime.now(UTC) - timedelta(minutes=1)}
    repo = FakeRepo(lobby=expired, applied=expired)
    asyncio.run(LobbyService(repo, FakeSeasons()).read(HEX_ID, "alice"))
    _, changes = repo.changes[0]
    assert changes["phase"] == "bans"
    # Nobody voted, so every question locks to its default (D191) -- which is
    # what makes an expired vote safe to advance rather than stall.
    assert changes["settings"]["duration"]


def test_a_read_before_the_deadline_changes_nothing():
    repo = FakeRepo(lobby=VOTING, applied=VOTING)
    asyncio.run(LobbyService(repo, FakeSeasons()).read(HEX_ID, "alice"))
    assert repo.changes == []


# --- banning (D195, D196) ----------------------------------------------


class FakeCivData:
    """Two leaders and three civs spread across two ages."""

    def __init__(self):
        self.asked = []

    async def fetch(self, edition):
        self.asked.append(edition)
        return {
            "edition": edition,
            "leader_data_version": 1,
            "leaders": [
                {"token": "LEADER_TRAJAN", "name": "Trajan"},
                {"token": "LEADER_HATSHEPSUT", "name": "Hatshepsut"},
                {"token": "LEADER_AUGUSTUS", "name": "Augustus"},
                {"token": "LEADER_AMINA", "name": "Amina"},
                {"token": "LEADER_XERXES", "name": "Xerxes"},
            ],
            "civs": [
                {"token": "CIVILIZATION_ROME", "age_pool": "AGE_ANTIQUITY"},
                {"token": "CIVILIZATION_EGYPT", "age_pool": "AGE_ANTIQUITY"},
                {"token": "CIVILIZATION_SPAIN", "age_pool": "AGE_EXPLORATION"},
            ],
        }


BANNING = {
    **VOTING,
    "phase": "bans",
    "edition": "civ7",
    "seats": [
        {"seat_index": 0, "discord_id": "alice", "team": None},
        {"seat_index": 1, "discord_id": "bob", "team": None},
    ],
}


def ban(repo, actor="alice", civ_data=None, revision=3, **keys):
    return asyncio.run(
        LobbyService(repo, FakeSeasons(), civ_data or FakeCivData()).submit_bans(
            HEX_ID, actor, SubmitBansRequest(expected_revision=revision, **keys)
        )
    )


def test_bans_are_stored_on_the_submitting_seat_only():
    repo = FakeRepo(lobby=BANNING, written=BANNING, applied=BANNING)
    ban(repo, actor="alice", leader_keys=["LEADER_TRAJAN"])
    _, seats, _ = repo.writes[0]
    by_id = {seat["discord_id"]: seat for seat in seats}
    assert by_id["alice"]["bans"]["leader_keys"] == ["LEADER_TRAJAN"]
    assert by_id["bob"].get("bans") is None


def test_banning_nothing_still_counts_as_submitting():
    # ⚠ `bans is not None` is the submitted test, not "banned something" --
    # otherwise a seat that wants no bans would stall the phase until the
    # timer expired.
    voted = {
        **BANNING,
        "seats": [
            {"seat_index": 0, "discord_id": "alice", "bans": {"leader_keys": []}},
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=voted, written=voted, applied=voted)
    ban(repo, actor="bob")
    assert repo.changes, "the empty submission completed the phase"


def test_an_unseated_player_cannot_ban():
    repo = FakeRepo(lobby=BANNING, written=BANNING, applied=BANNING)
    with pytest.raises(NotSeated):
        ban(repo, actor="mallory", leader_keys=["LEADER_TRAJAN"])
    assert repo.writes == []


def test_a_leader_the_edition_does_not_have_is_refused():
    repo = FakeRepo(lobby=BANNING, written=BANNING, applied=BANNING)
    with pytest.raises(InvalidSeating):
        ban(repo, leader_keys=["LEADER_NOBODY"])
    assert repo.writes == []


def test_a_civ_outside_the_starting_age_is_refused():
    # ⚠ The case civ-data is on the service for. CIVILIZATION_SPAIN is a real
    # token for a civ that is not in an antiquity game, and banning it would
    # burn one of only three slots -- a client bug, not collusion.
    antiquity = {**BANNING, "starting_age": "AGE_ANTIQUITY"}
    repo = FakeRepo(lobby=antiquity, written=antiquity, applied=antiquity)
    with pytest.raises(InvalidSeating):
        ban(repo, civ_keys=["CIVILIZATION_SPAIN"])
    assert repo.writes == []
    repo = FakeRepo(lobby=BANNING, written=BANNING, applied=BANNING)
    ban(repo, civ_keys=["CIVILIZATION_SPAIN"])
    assert repo.writes


def test_bans_cannot_be_submitted_outside_the_ban_phase():
    repo = FakeRepo(lobby=VOTING, written=VOTING, applied=VOTING)
    with pytest.raises(SeatChangeRefused):
        ban(repo, leader_keys=["LEADER_TRAJAN"])
    assert repo.writes == []


def test_the_last_submission_tallies_and_moves_to_draft():
    both = {
        **BANNING,
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "bans": {"leader_keys": ["LEADER_TRAJAN"], "civ_keys": []},
            },
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=both, written=both, applied=both)
    ban(repo, actor="bob", leader_keys=["LEADER_TRAJAN"])
    _, changes = repo.changes[0]
    assert changes["phase"] == "draft"
    assert changes["bans"]["leader"] == ["LEADER_TRAJAN"]


def test_an_expired_ban_phase_advances_from_a_read():
    # ⚠ The lazy timer generalising to a second phase (D74, D194): nothing
    # sweeps it, so the poll is what moves an abandoned ban phase on. Built
    # with NO civ-data on purpose -- the advance must not need it.
    expired = {**BANNING, "turn_expires_at": datetime.now(UTC) - timedelta(minutes=1)}
    repo = FakeRepo(lobby=expired, applied=expired)
    asyncio.run(LobbyService(repo, FakeSeasons(), FakeCivData()).read(HEX_ID, "alice"))
    _, changes = repo.changes[0]
    assert changes["phase"] == "draft"
    assert all(len(seat["pool"]) == 2 for seat in changes["seats"])
    assert changes["bans"] == {"leader": [], "civ": []}


def test_a_lobby_that_bans_itself_out_is_cancelled():
    # ⚠ D198, and it fired for real before this test existed: two leaders,
    # one banned, two players needing one each. The advance CANCELS rather
    # than raising -- a poll can trigger it, so raising would make every read
    # a 500 with no route out.
    keys = [
        "LEADER_TRAJAN",
        "LEADER_HATSHEPSUT",
        "LEADER_AUGUSTUS",
        "LEADER_AMINA",
    ]
    both = {
        **BANNING,
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "bans": {"leader_keys": keys, "civ_keys": []},
            },
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=both, written=both, applied=both)
    ban(repo, actor="bob", leader_keys=keys)
    _, changes = repo.changes[0]
    assert changes["phase"] == "cancelled"
    assert changes["cancel_reason"] == "no_pool"
    assert changes["closed_at"] is not None


# --- turn-ordered drafts (D199, D200) ------------------------------------

DRAFTING = {
    **BANNING,
    "phase": "draft",
    "edition": "civ6",
    "settings": {"draft_mode": "snake"},
    "pick_order": ["alice", "bob"],
    "turn_index": 0,
    "seats": [
        {
            "seat_index": 0,
            "discord_id": "alice",
            "team": 0,
            "pool": ["LEADER_TRAJAN"],
        },
        {
            "seat_index": 1,
            "discord_id": "bob",
            "team": 1,
            "pool": ["LEADER_AMINA"],
        },
    ],
}


def pick(repo, actor, token=None, revision=3, civ_token=None):
    body = {"expected_revision": revision}
    if token is not None:
        body["token"] = token
    if civ_token is not None:
        body["civ_token"] = civ_token
    return asyncio.run(
        LobbyService(repo, FakeSeasons(), FakeCivData()).submit_pick(
            HEX_ID, actor, SubmitPickRequest(**body)
        )
    )


def test_picking_out_of_turn_is_refused():
    # ⚠ The whole gap CP6d closed: turn_index was written and read by nothing,
    # so any seat could pick at any moment. Silent -- the draft simply stopped
    # being a draft.
    repo = FakeRepo(lobby=DRAFTING, applied=DRAFTING)
    with pytest.raises(NotYourTurn):
        pick(repo, "bob", "LEADER_AMINA")
    assert repo.changes == []


def test_the_seat_whose_turn_it_is_may_pick_and_the_index_advances():
    repo = FakeRepo(lobby=DRAFTING, applied=DRAFTING)
    pick(repo, "alice", "LEADER_TRAJAN")
    _, changes = repo.changes[0]
    assert changes["turn_index"] == 1
    by_id = {s["discord_id"]: s for s in changes["seats"]}
    assert by_id["alice"]["pick"] == "LEADER_TRAJAN"


def test_a_token_outside_your_own_pool_is_refused():
    repo = FakeRepo(lobby=DRAFTING, applied=DRAFTING)
    with pytest.raises(InvalidSeating):
        pick(repo, "alice", "LEADER_AMINA")
    assert repo.changes == []


def test_a_second_pick_of_the_same_field_is_refused():
    # O-34, per field.
    already = {
        **DRAFTING,
        "seats": [
            {**DRAFTING["seats"][0], "pick": "LEADER_TRAJAN"},
            DRAFTING["seats"][1],
        ],
    }
    repo = FakeRepo(lobby=already, applied=already)
    with pytest.raises(PickIsFinal):
        pick(repo, "alice", "LEADER_TRAJAN")


def test_a_civ_pick_is_allowed_after_a_leader_pick():
    # ⚠ Why the lock is PER FIELD: snake on civ7 runs a leader round then a
    # civ round, so a seat picks twice. Locking on `pick` alone would refuse
    # the second and stall every civ7 snake draft.
    civ7 = {
        **DRAFTING,
        "edition": "civ7",
        "pick_order": ["alice", "bob", "bob", "alice"],
        "turn_index": 3,
        "seats": [
            {
                **DRAFTING["seats"][0],
                "pick": "LEADER_TRAJAN",
                "civ_pool": ["CIVILIZATION_ROME"],
            },
            DRAFTING["seats"][1],
        ],
    }
    repo = FakeRepo(lobby=civ7, applied=civ7)
    # The civ round sends civ_token ALONE -- the leader is already locked.
    pick(repo, "alice", civ_token="CIVILIZATION_ROME")
    _, changes = repo.changes[0]
    by_id = {s["discord_id"]: s for s in changes["seats"]}
    assert by_id["alice"]["civ_pick"] == "CIVILIZATION_ROME"


def test_cwc_writes_the_team_and_never_the_seat():
    # ⚠ D199, the one place the seat is not the unit of ownership. A captain
    # drafts for the team; nobody is assigned a leader until the players
    # divide them.
    cwc = {
        **DRAFTING,
        "settings": {"draft_mode": "cwc"},
        "pick_order": ["alice", "bob"],
        # ⚠ D201: one shared pool on the lobby, not per-seat pools.
        "pool": ["LEADER_TRAJAN", "LEADER_AMINA"],
        "teams": [
            {"team_index": 0, "leaders": [], "civs": []},
            {"team_index": 1, "leaders": [], "civs": []},
        ],
    }
    repo = FakeRepo(lobby=cwc, applied=cwc)
    pick(repo, "alice", "LEADER_TRAJAN")
    _, changes = repo.changes[0]
    assert "seats" not in changes
    assert changes["teams"][0]["leaders"] == ["LEADER_TRAJAN"]
    assert changes["teams"][1]["leaders"] == []


def test_a_turn_ordered_draft_completes_when_the_order_is_spent():
    # ⚠ Two completion tests, one phase. A CWC captain picks for the team, so
    # no seat ever holds a pick and "every seat has picked" would never fire.
    cwc = {
        **DRAFTING,
        "settings": {"draft_mode": "cwc"},
        "pick_order": ["alice"],
        "turn_index": 0,
        "pool": ["LEADER_TRAJAN"],
        "teams": [{"team_index": 0, "leaders": [], "civs": []}],
    }
    repo = FakeRepo(lobby=cwc, applied=cwc)
    pick(repo, "alice", "LEADER_TRAJAN")
    assert repo.changes[-1][1]["phase"] == "complete"


def test_a_pick_with_neither_token_is_refused():
    with pytest.raises(ValueError):
        SubmitPickRequest(expected_revision=3)


def test_cwc_cannot_take_a_leader_another_team_already_took():
    # ⚠ D201: one shared pool, so "already taken" is the only thing stopping
    # two teams drafting the same leader. Per-seat pools made that impossible
    # by construction; a shared pool has to check.
    cwc = {
        **DRAFTING,
        "settings": {"draft_mode": "cwc"},
        "pick_order": ["alice", "bob"],
        "turn_index": 1,
        "pool": ["LEADER_TRAJAN", "LEADER_AMINA"],
        "teams": [
            {"team_index": 0, "leaders": ["LEADER_TRAJAN"], "civs": []},
            {"team_index": 1, "leaders": [], "civs": []},
        ],
    }
    repo = FakeRepo(lobby=cwc, applied=cwc)
    with pytest.raises(InvalidSeating):
        pick(repo, "bob", "LEADER_TRAJAN")
    assert repo.changes == []
    pick(repo, "bob", "LEADER_AMINA")
    assert repo.changes[0][1]["teams"][1]["leaders"] == ["LEADER_AMINA"]
