from __future__ import annotations

import asyncio

import pytest
from bson import ObjectId

from app.features.matches.models import MatchModel
from app.features.matches.service import MatchService, MatchServiceError
from app.features.seasons.repository import SeasonNotSeededError

OID = "652f1a2b3c4d5e6f7a8b9c0d"
SEASON_ID = ObjectId("652f1a2b3c4d5e6f7a8b9c0e")


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


class FakeSession:
    def __init__(self, commit_error=None):
        self.committed = False
        self.aborted = False
        self.in_transaction = True
        self.commit_error = commit_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def start_transaction(self):
        return self

    async def commit_transaction(self):
        # As pymongo does: a commit ends the transaction, even one that fails.
        self.in_transaction = False
        if self.commit_error:
            raise self.commit_error
        self.committed = True

    async def abort_transaction(self):
        self.aborted = True


class FakeRepo:
    def __init__(self, match_doc=None, stat_doc=None):
        self.match_doc = match_doc
        self.stat_doc = stat_doc
        self.upserts = []
        self.events = []
        self.session = FakeSession()
        self.deleted_pending = []
        self.deleted_validated = []
        self.sub_events_written = []
        self.sub_events_removed = []
        self.claims = []
        self.releases = []
        self.validated = None

    async def find_pending_by_id(self, oid):
        return dict(self.match_doc) if self.match_doc else None

    async def claim_pending_match(self, oid, *, now=None, expect=None):
        self.claims.append(oid)
        return dict(self.match_doc) if self.match_doc else None

    async def release_pending_claim(self, oid, *, claimed_at):
        self.releases.append(oid)

    async def find_validated_by_id(self, oid):
        return dict(self.match_doc) if self.match_doc else None

    async def get_player_stat_doc(self, **kwargs):
        return dict(self.stat_doc) if self.stat_doc else None

    async def get_player_stat_docs_batch(self, *, discord_ids, **kwargs):
        return {d: dict(self.stat_doc) for d in discord_ids} if self.stat_doc else {}

    @property
    def client(self):
        repo = self

        class Client:
            def start_session(self):
                return repo.session

        return Client()

    async def write_player_stat_docs(self, *, legs, **kwargs):
        for (is_seasonal, is_combined), docs in legs.items():
            for did, doc in docs.items():
                self.upserts.append(
                    {
                        **kwargs,
                        "is_seasonal": is_seasonal,
                        "is_combined": is_combined,
                        "discord_id": did,
                        "doc": doc,
                    }
                )

    async def insert_validated_match(self, doc, session=None):
        self.validated = doc
        return OID

    async def delete_pending_match(self, oid, session=None):
        self.deleted_pending.append(oid)

    async def delete_validated_match(self, oid, session=None):
        self.deleted_validated.append(oid)
        return True

    async def record_sub_in(self, discord_id, match_id, session=None):
        self.sub_events_written.append((discord_id, match_id))

    async def remove_sub_in(self, discord_id, match_id, session=None):
        self.sub_events_removed.append((discord_id, match_id))

    async def insert_events(self, events, *, session):
        self.events.extend(events)


class FakeSeasons:
    def __init__(self, seasons=None):
        if seasons is None:
            seasons = {"civ6": {"_id": SEASON_ID, "label": "Season 6"}}
        self.seasons = seasons
        self.asked = []

    async def get_current_season(self, edition):
        self.asked.append(edition)
        if edition not in self.seasons:
            raise SeasonNotSeededError(f"no season row for edition {edition!r}")
        return dict(self.seasons[edition])

    async def read_open(self, edition, *, session=None):
        row = self.seasons.get(edition)
        return dict(row) if row else None


def make_service(repo, ratings=None, seasons=None) -> MatchService:
    svc = MatchService.__new__(MatchService)
    svc.q = repo
    # One fake for both: the stat reads and writes moved to RatingsRepository,
    # so the upsert assertions below still read from the same object.
    svc.ratings = ratings if ratings is not None else repo
    svc.seasons = seasons if seasons is not None else FakeSeasons()
    return svc


# --- stored sub counters are read, not defaulted to 0 ---


def test_a_players_ranking_reads_stored_sub_counters():
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

    stat = asyncio.run(make_service(repo).get_players_ranking(match))[0]

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
    assert result["approved_at"] is not None
    assert result["approver_discord_id"] == "approver-1"
    assert repo.session.committed
    # 2 players x (lifetime + seasonal + combined)
    assert len(repo.upserts) == 6
    for upsert in repo.upserts:
        doc = upsert["doc"]
        assert doc["games"] == 11  # 10 + 1, not 0 + 1
        assert doc["subbedIn"] == 3  # preserved, not clobbered to 0
        assert doc["subbedOut"] == 1
        assert "civs" in doc and "leaders" in doc


# --- a validated match carries the season it was approved in ---


def _two_player_match():
    return make_match_doc(
        [
            make_player(discord_id="123", team=0, placement=0),
            make_player(
                discord_id="456", team=1, placement=1, steam_id="76561190000000002"
            ),
        ]
    )


def test_approve_stamps_the_current_season_of_the_matchs_edition():
    repo = FakeRepo(match_doc=_two_player_match())
    seasons = FakeSeasons()

    asyncio.run(make_service(repo, seasons=seasons).approve_match(OID, "a-1"))

    assert seasons.asked == ["civ6"]
    assert repo.validated["season_id"] == SEASON_ID


def test_approve_without_a_season_writes_nothing_and_releases_the_claim():
    repo = FakeRepo(match_doc=_two_player_match())
    svc = make_service(repo, seasons=FakeSeasons({}))

    with pytest.raises(SeasonNotSeededError):
        asyncio.run(svc.approve_match(OID, "a-1"))

    assert repo.upserts == []
    assert repo.events == []
    assert repo.validated is None
    assert not repo.session.committed
    assert len(repo.releases) == 1


def test_approve_refuses_a_seat_without_a_player_and_writes_nothing():
    doc = make_match_doc(
        [
            make_player(discord_id="1", placement=0, team=0),
            make_player(discord_id="-2", placement=1, team=1),
            make_player(discord_id=None, placement=2, team=2),
        ]
    )
    repo = FakeRepo(match_doc=doc)

    with pytest.raises(
        MatchServiceError, match="Assign seats 2 and 3 before approving."
    ):
        asyncio.run(make_service(repo).approve_match(OID, "a-1"))

    assert repo.validated is None
    assert repo.events == []
    assert len(repo.releases) == 1


def test_a_leaver_row_is_not_a_seat_to_assign():
    from app.features.matches.validation import approval_refusal

    sub = make_player(discord_id="1", is_sub=True)
    leaver = make_player(discord_id="-9", subbed_out=True)
    other = make_player(discord_id="-3", placement=1, team=1)
    assert approval_refusal(MatchModel(**make_match_doc([sub, leaver]))) is None
    assert (
        approval_refusal(MatchModel(**make_match_doc([sub, leaver, other])))
        == "Assign seat 2 before approving."
    )


# --- revert_match: legacy None deltas + negative-counter clamp ---


def test_revert_match_tolerates_missing_deltas_and_clamps_counters():
    # Legacy validated doc: no season/combined deltas; player never accumulated stats.
    players = [make_player(season_delta=None, combined_delta=None)]
    repo = FakeRepo(match_doc=make_match_doc(players), stat_doc=None)

    result = asyncio.run(make_service(repo).revert_match(OID, "staff-1"))

    assert result["match_id"] == OID
    assert repo.session.committed
    assert repo.deleted_validated
    # The ledger names who reverted, on every event the revert wrote.
    assert repo.events and all(e["actor_id"] == "staff-1" for e in repo.events)
    assert len(repo.upserts) == 3  # lifetime + seasonal + combined
    for upsert in repo.upserts:
        doc = upsert["doc"]
        for counter in ("games", "wins", "first", "subbedIn", "subbedOut"):
            assert doc[counter] >= 0, f"{counter} went negative: {doc[counter]}"


# --- numeric field validation (400, not 500) ---


# --- update-match whitelist ---


def test_revert_doc_math_uses_per_scope_deltas():
    # delta stored, season/combined missing (legacy) — lifetime mu subtracts the delta,
    # season/combined subtract 0.0 instead of raising.
    stored = {"_id": 123, "mu": 30.0, "sigma": 5.0, "games": 10, "wins": 4, "first": 2}
    players = [make_player(delta=5.0, season_delta=None, combined_delta=None)]
    repo = FakeRepo(match_doc=make_match_doc(players), stat_doc=stored)

    asyncio.run(make_service(repo).revert_match(OID, "staff-1"))

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


# --- cloud games have no season row ---


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


# --- subs are dated rows, not a counter ---


def test_approving_a_sub_writes_one_dated_row():
    # Three players, one of them a sub: TrueSkill rates the non-subs
    # separately and needs two groups to do it.
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


def test_paired_duel_with_one_sub_approves():
    players = [
        make_player(discord_id="111", team=0, placement=0, is_sub=True),
        make_player(
            discord_id="222",
            team=0,
            placement=0,
            steam_id="76561190000000009",
            subbed_out=True,
        ),
        make_player(
            discord_id="333", team=1, placement=1, steam_id="76561190000000002"
        ),
    ]
    repo = FakeRepo(match_doc=make_match_doc(players))
    result = asyncio.run(make_service(repo).approve_match(OID, "approver-1"))
    assert result["match_id"]
    assert [d for d, _ in repo.sub_events_written] == ["111"]


def test_approve_refuses_a_seat_without_a_leader_or_civ():
    doc = make_match_doc(
        [
            make_player(discord_id="1", placement=0, team=0),
            make_player(discord_id="2", placement=1, team=1, leader="LEADER_NONE"),
        ]
    )
    repo = FakeRepo(match_doc=doc)
    with pytest.raises(MatchServiceError, match="leader and civ of seat 2"):
        asyncio.run(make_service(repo).approve_match(OID, "a-1"))
    assert repo.validated is None


def test_reverting_an_earlier_seasons_match_leaves_this_season_alone():
    stored = {"_id": 123, "mu": 30.0, "sigma": 5.0, "games": 10, "wins": 4, "first": 2}
    doc = make_match_doc([make_player(delta=5.0, season_delta=3.0)])
    earlier = ObjectId()
    repo = FakeRepo(match_doc={**doc, "season_id": earlier}, stat_doc=stored)

    out = asyncio.run(make_service(repo).revert_match(OID, "staff-1"))

    assert out["season_id"] == earlier

    assert {(u["is_seasonal"], u["is_combined"]) for u in repo.upserts} == {
        (False, False),
        (False, True),
    }


def test_a_player_linked_to_another_platform_can_take_a_seat():
    class Users:
        def __init__(self, user):
            self.user = user

        async def get_user_by_discord_id(self, discord_id):
            return self.user

    svc = MatchService.__new__(MatchService)
    svc.q = Users({"discord_id": "1", "linked_platform": "epic"})
    assert asyncio.run(svc.seat_steam_id("1")) is None
    svc.q = Users(None)
    with pytest.raises(MatchServiceError, match="not found"):
        asyncio.run(svc.seat_steam_id("1"))


def test_a_commit_that_clashes_is_retryable_and_is_not_aborted_twice():
    from pymongo.errors import OperationFailure

    from app.features.matches.errors import RetryLaterError

    repo = FakeRepo(match_doc=_two_player_match())
    repo.session = FakeSession(
        OperationFailure(
            "NoSuchTransaction", 251, {"errorLabels": ["TransientTransactionError"]}
        )
    )
    with pytest.raises(RetryLaterError):
        asyncio.run(make_service(repo).approve_match(OID, "a-1"))
    assert not repo.session.aborted
    assert len(repo.releases) == 1


def test_a_v1_match_without_a_contest_list_or_created_at_can_be_reverted():
    stored = {"_id": 123, "mu": 30.0, "sigma": 5.0, "games": 10, "wins": 4, "first": 2}
    doc = {**make_match_doc([make_player(delta=5.0)]), "_id": ObjectId(OID)}
    doc["created_at"] = None
    del doc["contest_report_list"]
    repo = FakeRepo(match_doc=doc, stat_doc=stored)

    result = asyncio.run(make_service(repo).revert_match(OID, "staff-1"))

    assert result["contest_report_list"] == []
    assert result["created_at"] == ObjectId(OID).generation_time
    # The lifetime FFA rating the rank role follows, after the revert.
    assert result["affected_players"] == [{"discord_id": "123", "rating_mu": 25.0}]


def test_rank_roles_follow_ffa_and_cloud_games_only():
    from app.features.ratings.scope import drives_rank_roles

    assert drives_rank_roles(game_mode="FFA", is_cloud=False)
    assert drives_rank_roles(game_mode="teamer", is_cloud=True)
    assert not drives_rank_roles(game_mode="teamer", is_cloud=False)
    assert not drives_rank_roles(game_mode="duel", is_cloud=False)

    stored = {"_id": 123, "mu": 30.0, "sigma": 5.0, "games": 10, "wins": 4, "first": 2}
    teamer = {**make_match_doc([make_player()]), "game_mode": "Teamer"}
    repo = FakeRepo(match_doc=teamer, stat_doc=stored)
    assert (
        asyncio.run(make_service(repo).revert_match(OID, "staff-1"))["affected_players"]
        == []
    )


def test_approve_stamps_the_season_open_inside_its_transaction():
    class Changed(FakeSeasons):
        async def read_open(self, edition, *, session=None):
            return {"_id": "s7", "label": "Season 7"}

    repo = FakeRepo(match_doc=_two_player_match())
    out = asyncio.run(make_service(repo, seasons=Changed()).approve_match(OID, "a-1"))
    assert repo.validated["season_id"] == "s7"
    assert out["season_id"] == "s7"


def test_a_repeated_approve_answers_with_the_season_it_was_approved_in():
    class Approved(FakeRepo):
        async def claim_pending_match(self, oid, *, now=None, expect=None):
            return None

    stored = {"_id": 123, "mu": 30.0, "sigma": 5.0, "games": 10, "wins": 4, "first": 2}
    repo = Approved(
        match_doc={**_two_player_match(), "season_id": "s5"}, stat_doc=stored
    )
    assert (
        asyncio.run(make_service(repo).approve_match(OID, "a-1"))["season_id"] == "s5"
    )
