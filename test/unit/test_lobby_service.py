"""Creation: what the document carries, and who gets a seat.

Pure -- a fake repository and a fake seasons cache, no database.
What should break these: seating an over-large roster, seating duplicates,
assigning teams at creation, forgetting the season stamp, writing a field
the lobby has not decided yet, or resolving the shape after the season
lookup so a malformed request reaches the database.

Governed by D59, D60.
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
    MarkReadyRequest,
    SubmitBallotRequest,
    SubmitBansRequest,
    SubmitPickRequest,
)
from app.features.lobbies.service import (
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
    seat_the_host,
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
    body.setdefault("voice_channel_id", "v1")
    # A teamer chooses its draft mode at creation and derives its own seat
    # count; an ffa is sized by the host.
    if body["game_type"] == "teamer":
        body.setdefault("draft_mode", "standard")
    elif body["game_type"] == "ffa":
        body.setdefault("size", 8)
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
        self.claims = []
        self.counted = []
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
        if self._written is None:
            return None
        self._lobby = {**(self._lobby or self._written), "seats": seats}
        return {**self._written, "seats": seats}

    async def is_playing_a_started_lobby(self, discord_id):
        return False

    async def claim_for_stats(self, lobby_id, now):
        self.claims.append(lobby_id)
        if len(self.claims) > 1 or self._applied is None:
            return None
        latest = dict(self._applied)
        for _, changes in self.changes:
            latest.update(changes)
        return {**latest, "phase": "complete"}

    async def add_contributions(self, key, counts):
        self.counted.append((key, counts))
        return len(counts)

    async def apply_changes(self, lobby_id, expected_revision, changes, now):
        self.changes.append((expected_revision, changes))
        if self._applied is None:
            return None
        return {**self._applied, **changes}


# --- seating ------------------------------------------------------------


def test_document_stamps_the_season_and_derived_shape():
    shape = resolve_shape("teamer", 3, 3)
    doc = build_lobby_document(request(game_type="teamer"), shape, SEASON, NOW)
    assert (doc["season_id"], doc["season_label"]) == ("S1", "Season 6")
    assert (doc["number_teams"], doc["team_size"]) == (3, 3)
    assert (doc["seat_count"], doc["min_seats"]) == (9, 9)
    assert (doc["phase"], doc["revision"]) == ("lobby", 1)
    assert doc["created_at"] == doc["updated_at"] == NOW


def test_undecided_fields_are_absent_not_null():
    doc = build_lobby_document(request(), resolve_shape("ffa", size=8), SEASON, NOW)
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
    shape = resolve_shape("ffa", size=8)
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
    """
    Stands in for bson.ObjectId: str()s to a hex string and is otherwise not
    JSON-serialisable, which is exactly how the real one behaves.
    """

    def __init__(self, hex_value):
        self._hex = hex_value

    def __str__(self):
        return self._hex


def test_the_wire_form_stringifies_both_object_ids():
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
    repo, seasons = FakeRepo(), FakeSeasons()
    result = asyncio.run(LobbyService(repo, seasons).create(request()))
    timestamps = {"created_at", "updated_at"}
    stored = {**repo.inserted, "_id": "L1"}
    assert set(result) == set(stored)
    assert {k: v for k, v in result.items() if k not in timestamps} == {
        k: v for k, v in stored.items() if k not in timestamps
    }
    assert all(isinstance(result[key], str) for key in timestamps)


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


def has_datetime(value):
    if isinstance(value, datetime):
        return True
    if isinstance(value, dict):
        return any(has_datetime(item) for item in value.values())
    if isinstance(value, list):
        return any(has_datetime(item) for item in value)
    return False


def test_a_datetime_leaves_the_boundary_as_rfc3339_with_z():
    stored = {
        "_id": "L1",
        "created_at": datetime(2026, 8, 26, 10, 31, 13, 237000, tzinfo=UTC),
    }
    assert for_the_wire(stored, None)["created_at"] == "2026-08-26T10:31:13.237000Z"


def test_no_datetime_survives_the_boundary_at_any_depth():
    stored = {
        "_id": "L1",
        "created_at": datetime(2026, 8, 26, tzinfo=UTC),
        "updated_at": datetime(2026, 8, 26, tzinfo=UTC),
        "seats": [{"seat_index": 0, "discord_id": "alice"}],
    }
    assert has_datetime(stored), "fixture must contain what the test looks for"
    assert not has_datetime(for_the_wire(stored, None))


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
    "settings",
    "voice_channel_id",
}
PUBLIC_SEAT_FIELDS = {"seat_index", "discord_id", "team", "ready"}


def built_document():
    return build_lobby_document(
        request(
            game_type="teamer",
            number_teams=3,
            team_size=3,
            instance_id="i9",
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
    document = built_document()
    assert document["seats"], "the fixture must seat someone or the seat leg is vacuous"
    assert not unclassified(document), sorted(unclassified(document))


def test_the_guard_reports_a_field_that_is_neither_public_nor_censored():
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
    repo = FakeRepo(lobby=OPEN_LOBBY, written=None, reread=OPEN_LOBBY)
    with pytest.raises(SeatChangeRefused) as exc:
        change(repo, actor="bob", seat_index=4)
    assert (exc.value.expected, exc.value.current) == (3, 3)
    assert "already holds a seat" in str(exc.value)


# --- starting the vote and resolving it ---------------------------------

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
    # A seat finishing is what ends the phase, not a seat submitting. The
    # resolution happens in the same call, so the caller never waits for a
    # poll tick.
    voted = {
        **VOTING,
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "ballot": {"duration": "unlimited"},
                "ready": True,
            },
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=voted, written=voted, applied=voted)
    vote(repo, actor="bob", duration="unlimited")
    ready(repo, actor="bob")
    _, changes = repo.changes[0]
    assert changes["phase"] == "bans"
    assert changes["settings"]["duration"] == "unlimited"


def test_one_seat_finishing_does_not_advance_the_phase():
    # The property readiness introduces: alice is done, bob is not, so the
    # lobby waits for him or for the timer.
    repo = FakeRepo(lobby=VOTING, written=VOTING, applied=VOTING)
    vote(repo, actor="alice", duration="unlimited")
    ready(repo, actor="alice")
    assert repo.changes == []


def test_submitting_a_ballot_is_not_finishing_with_it():
    # A seat may answer and keep thinking. Nothing advances until it says so.
    repo = FakeRepo(lobby=VOTING, written=VOTING, applied=VOTING)
    vote(repo, actor="alice", duration="unlimited")
    vote(repo, actor="bob", duration="unlimited")
    assert repo.changes == []


def test_a_read_past_the_deadline_advances_the_phase():
    expired = {**VOTING, "turn_expires_at": datetime.now(UTC) - timedelta(minutes=1)}
    repo = FakeRepo(lobby=expired, applied=expired)
    asyncio.run(LobbyService(repo, FakeSeasons()).read(HEX_ID, "alice"))
    _, changes = repo.changes[0]
    assert changes["phase"] == "bans"
    assert changes["settings"]["duration"]


def test_a_read_before_the_deadline_changes_nothing():
    repo = FakeRepo(lobby=VOTING, applied=VOTING)
    asyncio.run(LobbyService(repo, FakeSeasons()).read(HEX_ID, "alice"))
    assert repo.changes == []


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
    voted = {
        **BANNING,
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "bans": {"leader_keys": []},
                "ready": True,
            },
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=voted, written=voted, applied=voted)
    ban(repo, actor="bob")
    ready(repo, actor="bob")
    assert repo.changes, "an empty submission still counts as having banned"


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
                "ready": True,
            },
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=both, written=both, applied=both)
    ban(repo, actor="bob", leader_keys=["LEADER_TRAJAN"])
    assert repo.changes == [], "submitting bans is not finishing"
    ready(repo, actor="bob")
    _, changes = repo.changes[0]
    assert changes["phase"] == "draft"
    assert changes["bans"]["leader"] == ["LEADER_TRAJAN"]


def test_an_expired_ban_phase_advances_from_a_read():
    expired = {**BANNING, "turn_expires_at": datetime.now(UTC) - timedelta(minutes=1)}
    repo = FakeRepo(lobby=expired, applied=expired)
    asyncio.run(LobbyService(repo, FakeSeasons(), FakeCivData()).read(HEX_ID, "alice"))
    _, changes = repo.changes[0]
    assert changes["phase"] == "draft"
    assert all(len(seat["pool"]) == 2 for seat in changes["seats"])
    assert changes["bans"] == {"leader": [], "civ": []}


def test_a_lobby_that_bans_itself_out_is_cancelled():
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
                "ready": True,
            },
            {"seat_index": 1, "discord_id": "bob", "team": None},
        ],
    }
    repo = FakeRepo(lobby=both, written=both, applied=both)
    ban(repo, actor="bob", leader_keys=keys)
    ready(repo, actor="bob")
    _, changes = repo.changes[0]
    assert changes["phase"] == "cancelled"
    assert changes["cancel_reason"] == "no_pool"
    assert changes["closed_at"] is not None


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


def test_cwc_writes_the_team_and_never_the_seat():
    cwc = {
        **DRAFTING,
        "settings": {"draft_mode": "cwc"},
        "pick_order": ["alice", "bob"],
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
    # Two completion tests, one phase. A CWC captain picks for the team, so
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


def test_a_finished_lobby_is_counted_once_and_only_once():
    done = {
        **DRAFTING,
        "settings": {"draft_mode": "standard"},
        "pick_order": [],
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "pool": ["LEADER_TRAJAN"],
            }
        ],
    }
    repo = FakeRepo(lobby=done, applied=done)
    pick(repo, "alice", "LEADER_TRAJAN")
    assert len(repo.counted) == 1
    key, counts = repo.counted[0]
    assert key["edition"] == "civ6"
    assert counts["LEADER_TRAJAN"]["picks"] == 1

    # A second completion claims nothing, so nothing is counted again.
    asyncio.run(LobbyService(repo, FakeSeasons(), FakeCivData())._count_the_lobby("L1"))
    assert len(repo.counted) == 1


def test_a_counter_failure_never_fails_the_pick():
    class Exploding(FakeRepo):
        async def claim_for_stats(self, lobby_id, now):
            raise RuntimeError("mongo is having a day")

    done = {
        **DRAFTING,
        "settings": {"draft_mode": "standard"},
        "pick_order": [],
        "seats": [{"seat_index": 0, "discord_id": "alice", "pool": ["LEADER_TRAJAN"]}],
    }
    repo = Exploding(lobby=done, applied=done)
    lobby = pick(repo, "alice", "LEADER_TRAJAN")
    assert lobby["phase"] == "complete"


def test_civ7_picks_a_leader_and_a_civ_in_one_submission():
    civ7 = {
        **DRAFTING,
        "edition": "civ7",
        "pick_order": [],
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "pool": ["LEADER_TRAJAN"],
                "civ_pool": ["CIVILIZATION_ROME"],
            }
        ],
    }
    repo = FakeRepo(lobby=civ7, applied=civ7)
    pick(repo, "alice", "LEADER_TRAJAN", civ_token="CIVILIZATION_ROME")
    _, changes = repo.changes[0]
    seat = changes["seats"][0]
    assert seat["pick"] == "LEADER_TRAJAN"
    assert seat["civ_pick"] == "CIVILIZATION_ROME"


def test_a_seat_that_already_picked_is_refused_outright():
    # One lock, one field. Snake was the only reason it was ever per-field.
    already = {
        **DRAFTING,
        "pick_order": [],
        "seats": [
            {
                "seat_index": 0,
                "discord_id": "alice",
                "pool": ["LEADER_TRAJAN"],
                "pick": "LEADER_TRAJAN",
            }
        ],
    }
    repo = FakeRepo(lobby=already, applied=already)
    with pytest.raises(PickIsFinal):
        pick(repo, "alice", "LEADER_TRAJAN")
    assert repo.changes == []


def test_the_host_takes_the_first_seat_and_nobody_else_is_seated():
    # A lobby opens with one seat filled. Everyone else presses join, so the
    # lobby fills by self-selection rather than by who happened to be in
    # voice when the command ran.
    seats = seat_the_host("alice")
    assert seats == [{"seat_index": 0, "discord_id": "alice", "team": None}]


def test_creation_evicts_a_stale_lobby_holding_the_host():
    # The host is the only player being seated, so they are the only one who
    # can be holding a seat somewhere else.
    repo = FakeRepo(stale=[{"_id": "OLD", "channel_id": "c9", "updated_at": NOW}])
    asyncio.run(LobbyService(repo, FakeSeasons()).create(request()))
    assert repo.evictions
    players, _ = repo.evictions[0]
    assert players == ["alice"]


def test_the_cutoff_is_an_hour_behind_the_creation():
    repo = FakeRepo()
    asyncio.run(LobbyService(repo, FakeSeasons()).create(request()))
    _, cutoff = repo.evictions[0]
    assert timedelta(minutes=59) < datetime.now(UTC) - cutoff < timedelta(minutes=61)


def ready(repo, actor="alice", revision=3):
    return asyncio.run(
        LobbyService(repo, FakeSeasons(), FakeCivData()).mark_ready(
            HEX_ID, actor, MarkReadyRequest(expected_revision=revision)
        )
    )


# --- cwc captains ban in turn ------------------------------------------

CWC = {
    **BANNING,
    "edition": "civ6",
    "settings": {"draft_mode": "cwc"},
    "team_size": 2,
    "number_teams": 2,
    "ban_order": ["alice", "bob", "bob", "alice"],
    "turn_index": 0,
    "bans": {"leader": [], "civ": []},
    "seats": [
        {"seat_index": 0, "discord_id": "alice", "team": 0},
        {"seat_index": 1, "discord_id": "bob", "team": 1},
    ],
}


def test_a_captain_cannot_ban_out_of_turn():
    repo = FakeRepo(lobby=CWC, written=CWC, applied=CWC)
    with pytest.raises(NotYourTurn):
        ban(repo, actor="bob", leader_keys=["LEADER_TRAJAN"])
    assert repo.changes == []


def test_a_turn_bans_one_leader_and_advances_the_order():
    repo = FakeRepo(lobby=CWC, written=CWC, applied=CWC)
    ban(repo, actor="alice", leader_keys=["LEADER_TRAJAN"])
    _, changes = repo.changes[0]
    assert changes["bans"]["leader"] == ["LEADER_TRAJAN"]
    assert changes["turn_index"] == 1


def test_a_civ6_turn_banning_two_leaders_is_refused():
    # A turn is one action. Letting a captain send several would make the
    # order meaningless.
    repo = FakeRepo(lobby=CWC, written=CWC, applied=CWC)
    with pytest.raises(InvalidSeating):
        ban(repo, actor="alice", leader_keys=["LEADER_TRAJAN", "LEADER_AMINA"])
    assert repo.changes == []


def test_a_civ7_turn_bans_a_leader_and_a_civ_together():
    civ7 = {**CWC, "edition": "civ7"}
    repo = FakeRepo(lobby=civ7, written=civ7, applied=civ7)
    ban(
        repo,
        actor="alice",
        leader_keys=["LEADER_TRAJAN"],
        civ_keys=["CIVILIZATION_ROME"],
    )
    _, changes = repo.changes[0]
    assert changes["bans"] == {
        "leader": ["LEADER_TRAJAN"],
        "civ": ["CIVILIZATION_ROME"],
    }


def test_a_civ7_turn_without_a_civ_is_refused():
    civ7 = {**CWC, "edition": "civ7"}
    repo = FakeRepo(lobby=civ7, written=civ7, applied=civ7)
    with pytest.raises(InvalidSeating):
        ban(repo, actor="alice", leader_keys=["LEADER_TRAJAN"])


def test_a_key_already_banned_cannot_be_banned_again():
    taken = {**CWC, "bans": {"leader": ["LEADER_TRAJAN"], "civ": []}}
    repo = FakeRepo(lobby=taken, written=taken, applied=taken)
    with pytest.raises(InvalidSeating):
        ban(repo, actor="alice", leader_keys=["LEADER_TRAJAN"])


def test_the_last_ban_turn_deals_the_draft():
    spent = {**CWC, "turn_index": 3, "bans": {"leader": ["A", "B", "C"], "civ": []}}
    repo = FakeRepo(lobby=spent, written=spent, applied=spent)
    ban(repo, actor="alice", leader_keys=["LEADER_TRAJAN"])
    assert len(repo.changes) == 2, "the ban, then the deal"
    assert repo.changes[-1][1]["phase"] == "draft"


def test_a_ban_turn_that_expires_is_skipped_not_cancelled():
    # A captain who runs out of time loses the ban, not the lobby.
    expired = {**CWC, "turn_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    repo = FakeRepo(lobby=expired, applied=expired)
    asyncio.run(LobbyService(repo, FakeSeasons(), FakeCivData()).read(HEX_ID, "alice"))
    _, changes = repo.changes[0]
    assert changes["turn_index"] == 1
    assert changes["turn_expires_at"] > datetime.now(UTC)


# --- staff observers ----------------------------------------------------

HIDDEN = {
    **DRAFTING,
    "settings": {"draft_mode": "blind"},
    "pick_order": [],
    "revealed_at": None,
    "seats": [
        {
            "seat_index": 0,
            "discord_id": "alice",
            "pool": ["LEADER_TRAJAN"],
            "pick": "LEADER_TRAJAN",
        }
    ],
}


TWO_HIDDEN = {
    **HIDDEN,
    "seats": [
        *HIDDEN["seats"],
        {
            "seat_index": 1,
            "discord_id": "bob",
            "pool": ["LEADER_AMINA"],
            "pick": "LEADER_AMINA",
        },
    ],
}


def watch(repo, actor, is_staff=True):
    return asyncio.run(
        LobbyService(repo, FakeSeasons(), FakeCivData()).read(
            HEX_ID, actor, is_staff=is_staff
        )
    )


def test_a_player_never_sees_another_blind_pick():
    repo = FakeRepo(lobby=HIDDEN, applied=HIDDEN)
    seen = watch(repo, "bob", is_staff=False)
    assert seen["seats"][0].get("pick") is None


def test_staff_watching_a_lobby_see_every_blind_pick():
    # Adjudication needs them. The secrecy is between players, and a censored
    # view never shows two seats' picks at once.
    repo = FakeRepo(lobby=TWO_HIDDEN, applied=TWO_HIDDEN)
    seen = watch(repo, "mod")
    assert [seat["pick"] for seat in seen["seats"]] == [
        "LEADER_TRAJAN",
        "LEADER_AMINA",
    ]


def test_staff_playing_in_the_lobby_are_players_not_observers():
    # Holding a seat makes you a participant whatever your role is: alice
    # sees her own pick because it is hers, and bob's stays hidden.
    repo = FakeRepo(lobby=TWO_HIDDEN, applied=TWO_HIDDEN)
    seen = watch(repo, "alice")
    by_id = {seat["discord_id"]: seat for seat in seen["seats"]}
    assert by_id["alice"]["pick"] == "LEADER_TRAJAN"
    assert by_id["bob"].get("pick") is None


def test_staff_mid_game_elsewhere_cannot_observe():
    class Busy(FakeRepo):
        async def is_playing_a_started_lobby(self, discord_id):
            return True

    repo = Busy(lobby=HIDDEN, applied=HIDDEN)
    seen = watch(repo, "mod")
    assert seen["seats"][0].get("pick") is None
