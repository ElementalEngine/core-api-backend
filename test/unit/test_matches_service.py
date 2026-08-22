"""Unit tests for the matches service defect fixes (batch 6.1b).

Covers:
- stored subbedIn/subbedOut counters are read (not clobbered to 0) and accumulate on approve;
- revert_match tolerates legacy docs with missing season/combined deltas and never writes
  negative counters;
- client-supplied numeric fields are validated (MatchServiceError -> 400, not ValueError -> 500);
- update-match only writes whitelisted fields (match_id never enters the $set).

Scope: logic only. FakeRepo and FakeSession stand in for the driver, so these
tests assert that approve and revert reach commit -- not that a transaction
provides atomicity. They do not discharge the Motor -> PyMongo transactional
risk (D84).
"""

from __future__ import annotations

import asyncio

import pytest

from app.features.matches.models import MatchModel
from app.features.matches.router import MATCH_UPDATE_SETTABLE_FIELDS
from app.features.matches.schemas import MatchUpdate
from app.features.matches.service import MatchService, MatchServiceError, _require_int

OID = "652f1a2b3c4d5e6f7a8b9c0d"


def make_player(**overrides):
    player = {
        "steam_id": "76561190000000001",
        "user_name": "player",
        "civ": "Rome",
        "team": 0,
        "leader": None,
        "player_alive": True,
        "discord_id": "123",
        "placement": 0,
        "quit": False,
        "delta": 5.0,
        "season_delta": 5.0,
        "combined_delta": 5.0,
        "is_sub": False,
        "subbed_out": False,
    }
    player.update(overrides)
    return player


def make_match_doc(players, *, is_cloud=False):
    return {
        "game": "civ6",
        "turn": 100,
        "age": None,
        "map_type": "Pangaea",
        "game_mode": "FFA",
        "is_cloud": is_cloud,
        "players": players,
        "parser_version": "1",
        "discord_messages_id_list": ["m0"],
        "save_file_hash": "hash",
        "reporter_discord_id": "999",
        "contest_report_list": [],
    }


# Tracks the driver's call convention, not its semantics: start_transaction is
# awaited as PyMongo requires, commit is recorded as a flag. A transaction that
# never commits atomically still passes every test below.
class FakeSession:
    def __init__(self):
        self.committed = False
        self.aborted = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def start_transaction(self):
        return self

    async def commit_transaction(self):
        self.committed = True

    async def abort_transaction(self):
        self.aborted = True


class FakeRepo:
    def __init__(self, match_doc=None, stat_doc=None):
        self.match_doc = match_doc
        self.stat_doc = stat_doc
        self.upserts = []
        self.session = FakeSession()
        self.deleted_pending = []
        self.deleted_validated = []
        self.sub_events_written = []
        self.sub_events_removed = []
        self.claims = []
        self.releases = []

    async def find_pending_by_id(self, oid):
        return dict(self.match_doc) if self.match_doc else None

    async def claim_pending_match(self, oid, *, now=None):
        self.claims.append(oid)
        return dict(self.match_doc) if self.match_doc else None

    async def release_pending_claim(self, oid):
        self.releases.append(oid)

    async def find_validated_by_id(self, oid):
        return dict(self.match_doc) if self.match_doc else None

    async def get_player_stat_doc(self, **kwargs):
        return dict(self.stat_doc) if self.stat_doc else None

    async def start_session(self):
        return self.session

    async def upsert_player_stat_doc(self, **kwargs):
        self.upserts.append(kwargs)

    async def insert_validated_match(self, doc, session=None):
        return OID

    async def delete_pending_match(self, oid, session=None):
        self.deleted_pending.append(oid)

    async def delete_validated_match(self, oid, session=None):
        self.deleted_validated.append(oid)

    async def record_sub_in(self, discord_id, match_id, session=None):
        self.sub_events_written.append((discord_id, match_id))

    async def remove_sub_in(self, discord_id, match_id, session=None):
        self.sub_events_removed.append((discord_id, match_id))


class FakeRatings:
    def __init__(self):
        self.events = []

    async def insert_events(self, events, *, session):
        self.events.extend(events)


def make_service(repo, ratings=None) -> MatchService:
    svc = MatchService.__new__(MatchService)
    svc.q = repo
    svc.ratings = ratings if ratings is not None else FakeRatings()
    return svc


# --- stored sub counters are read, not defaulted to 0 ---


def test_get_player_ranking_reads_stored_sub_counters():
    repo = FakeRepo(
        stat_doc={
            "_id": 123,
            "mu": 30.0,
            "sigma": 5.0,
            "games": 10,
            "wins": 4,
            "first": 2,
            "subbedIn": 3,
            "subbedOut": 1,
            "civs": {},
        }
    )
    match = MatchModel(**make_match_doc([make_player()]))

    stat = asyncio.run(make_service(repo).get_player_ranking(match, "123", 0))

    assert stat.games == 10
    assert stat.subbedIn == 3
    assert stat.subbedOut == 1


def test_approve_match_accumulates_counters_from_stored_doc():
    stored = {
        "_id": 123,
        "mu": 30.0,
        "sigma": 5.0,
        "games": 10,
        "wins": 4,
        "first": 2,
        "subbedIn": 3,
        "subbedOut": 1,
        "civs": {},
    }
    players = [
        make_player(discord_id="123", team=0, placement=0),
        make_player(
            discord_id="456", team=1, placement=1, steam_id="76561190000000002"
        ),
    ]
    repo = FakeRepo(match_doc=make_match_doc(players), stat_doc=stored)

    result = asyncio.run(make_service(repo).approve_match(OID, "approver-1"))

    assert result["match_id"] == OID
    assert repo.session.committed
    # 2 players x (lifetime + seasonal + combined)
    assert len(repo.upserts) == 6
    for upsert in repo.upserts:
        doc = upsert["doc"]
        assert doc["games"] == 11  # 10 + 1, not 0 + 1
        assert doc["subbedIn"] == 3  # preserved, not clobbered to 0
        assert doc["subbedOut"] == 1
        assert "civs" in doc and "leaders" in doc


# --- revert_match: legacy None deltas + negative-counter clamp ---


def test_revert_match_tolerates_missing_deltas_and_clamps_counters():
    # Legacy validated doc: no season/combined deltas; player never accumulated stats.
    players = [make_player(season_delta=None, combined_delta=None)]
    repo = FakeRepo(match_doc=make_match_doc(players), stat_doc=None)

    result = asyncio.run(make_service(repo).revert_match(OID))

    assert result["match_id"] == OID
    assert repo.session.committed
    assert repo.deleted_validated
    assert len(repo.upserts) == 3  # lifetime + seasonal + combined
    for upsert in repo.upserts:
        doc = upsert["doc"]
        for counter in ("games", "wins", "first", "subbedIn", "subbedOut"):
            assert doc[counter] >= 0, f"{counter} went negative: {doc[counter]}"


# --- numeric field validation (400, not 500) ---


def test_require_int_parses_and_rejects():
    assert _require_int("3", "player_id") == 3
    assert _require_int(" 4 ", "player_id") == 4
    for bad in ("abc", "", "1.5", None):
        with pytest.raises(MatchServiceError):
            _require_int(bad, "player_id")


def test_remove_sub_rejects_non_numeric_id_as_service_error():
    players = [make_player(), make_player(discord_id="456", subbed_out=True)]
    repo = FakeRepo(match_doc=make_match_doc(players))

    with pytest.raises(MatchServiceError, match="whole number"):
        asyncio.run(make_service(repo).remove_sub(OID, "not-a-number", "m1"))


def test_change_order_rejects_non_numeric_and_unmapped_teams():
    players = [make_player(team=0), make_player(discord_id="456", team=1)]
    repo = FakeRepo(match_doc=make_match_doc(players))
    svc = make_service(repo)

    with pytest.raises(MatchServiceError, match="whole number"):
        asyncio.run(svc.change_order(OID, "abc 2", "m1"))

    # Two distinct teams (0 and 2) pass the length check but team 2 has no list entry.
    gapped = [make_player(team=0), make_player(discord_id="456", team=2)]
    repo_gapped = FakeRepo(match_doc=make_match_doc(gapped))
    with pytest.raises(MatchServiceError, match="no entry"):
        asyncio.run(make_service(repo_gapped).change_order(OID, "1 2", "m1"))


# --- update-match whitelist ---


def test_update_match_whitelist_excludes_match_id():
    payload = MatchUpdate(
        match_id="652f1a2b3c4d5e6f7a8b9c0d", confirmed=True, flagged=True
    )
    update_data = {
        key: value
        for key, value in payload.dict(exclude_unset=True).items()
        if key in MATCH_UPDATE_SETTABLE_FIELDS
    }
    assert update_data == {"confirmed": True, "flagged": True}
    assert "match_id" not in update_data


def test_update_match_rejects_empty_payload():
    repo = FakeRepo(match_doc=make_match_doc([make_player()]))
    with pytest.raises(MatchServiceError, match="Empty update payload"):
        asyncio.run(make_service(repo).update_match(OID, {}))


def test_revert_doc_math_uses_per_scope_deltas():
    # delta stored, season/combined missing (legacy) — lifetime mu subtracts the delta,
    # season/combined subtract 0.0 instead of raising.
    stored = {"_id": 123, "mu": 30.0, "sigma": 5.0, "games": 10, "wins": 4, "first": 2}
    players = [make_player(delta=5.0, season_delta=None, combined_delta=None)]
    repo = FakeRepo(match_doc=make_match_doc(players), stat_doc=stored)

    asyncio.run(make_service(repo).revert_match(OID))

    docs = {(u["is_seasonal"], u["is_combined"]): u["doc"] for u in repo.upserts}
    assert docs[(False, False)]["mu"] == pytest.approx(25.0)  # 30 - 5 (lifetime)
    assert docs[(True, False)]["mu"] == pytest.approx(
        30.0
    )  # 30 - 0 (season, legacy None)
    assert docs[(False, True)]["mu"] == pytest.approx(
        30.0
    )  # 30 - 0 (combined, legacy None)
    for doc in docs.values():
        assert doc["sigma"] == pytest.approx(7.0)  # 5 + 2
        assert doc["games"] == 9
    # wins: lifetime decrements (delta 5 > 0), season/combined don't (coerced 0)
    assert docs[(False, False)]["wins"] == 3
    assert docs[(True, False)]["wins"] == 4
    assert docs[(False, True)]["wins"] == 4


# --- cloud games have no season row (Entry 1) ---


def _approve_and_collect_scopes(is_cloud):
    stored = {
        "_id": 123,
        "mu": 30.0,
        "sigma": 5.0,
        "games": 10,
        "wins": 4,
        "first": 2,
        "subbedIn": 3,
        "subbedOut": 1,
        "civs": {},
    }
    players = [
        make_player(discord_id="123", team=0, placement=0),
        make_player(
            discord_id="456", team=1, placement=1, steam_id="76561190000000002"
        ),
    ]
    repo = FakeRepo(
        match_doc=make_match_doc(players, is_cloud=is_cloud), stat_doc=stored
    )
    asyncio.run(make_service(repo).approve_match(OID, "approver-1"))
    return repo.upserts


def test_realtime_approve_writes_lifetime_season_and_combined():
    upserts = _approve_and_collect_scopes(is_cloud=False)
    legs = {(u["is_seasonal"], u["is_combined"]) for u in upserts}
    assert legs == {(False, False), (True, False), (False, True)}
    assert len(upserts) == 6  # two players, three legs each


def test_cloud_approve_skips_the_season_row():
    upserts = _approve_and_collect_scopes(is_cloud=True)
    legs = {(u["is_seasonal"], u["is_combined"]) for u in upserts}
    assert legs == {(False, False), (False, True)}
    assert not any(u["is_seasonal"] for u in upserts)
    assert len(upserts) == 4  # two players, lifetime and combined only
    # "cloud is never seasonal" must not become "cloud is never written"
    assert all(u["is_cloud"] for u in upserts)


# --- subs are dated rows, not a counter (Entry 4b) ---


def test_approving_a_sub_writes_one_dated_row():
    # Three players, one of them a sub: TrueSkill rates the non-subs
    # separately and needs two groups to do it (see §4 item 76).
    players = [
        make_player(discord_id="123", team=0, placement=0, is_sub=True),
        make_player(
            discord_id="456", team=1, placement=1, steam_id="76561190000000002"
        ),
        make_player(
            discord_id="789", team=2, placement=2, steam_id="76561190000000003"
        ),
    ]
    repo = FakeRepo(match_doc=make_match_doc(players))

    asyncio.run(make_service(repo).approve_match(OID, "approver-1"))

    # Only the sub, and the row carries the match so a revert can find it.
    assert len(repo.sub_events_written) == 1
    discord_id, match_id = repo.sub_events_written[0]
    assert discord_id == "123"
    assert str(match_id) == OID  # the service converts it to an ObjectId
    assert repo.sub_events_removed == []
