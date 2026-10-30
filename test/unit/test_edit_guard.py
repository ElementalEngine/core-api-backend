from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from bson import ObjectId
from pymongo.errors import OperationFailure

from app.features.matches.editing import EditingService
from app.features.matches.errors import ApprovingError, NotFoundError
from app.features.matches.repository import CLAIM_TTL, MatchRepository, _editable
from app.features.matches.service import MatchService
from app.features.matches.validation import SeatPatch

OID = "652f1a2b3c4d5e6f7a8b9c0d"


def _doc():
    player = {
        "steam_id": "1",
        "user_name": "a",
        "civ": "CIVILIZATION_ROME",
        "team": 0,
        "leader": "LEADER_TRAJAN",
        "discord_id": "100000000000000001",
        "placement": 0,
        "quit": False,
        "is_sub": False,
        "subbed_out": False,
    }
    return {
        "_id": ObjectId(OID),
        "game": "civ6",
        "turn": 100,
        "map_type": "Pangaea",
        "game_mode": "ffa",
        "is_cloud": False,
        "players": [
            player,
            {**player, "discord_id": "100000000000000002", "team": 1, "placement": 1},
        ],
        "parser_version": "1",
        "discord_messages_id_list": ["m0"],
        "save_file_hash": "h",
        "reporter_discord_id": "100000000000000001",
        "contest_report_list": [],
    }


class _Q:
    """A pending store whose guarded writes miss when told to."""

    def __init__(self, doc, *, held=False):
        self.doc = doc
        self.held = held
        self.players = None
        self.pushed = None

    async def find_pending_by_id(self, oid):
        return dict(self.doc) if self.doc else None

    async def set_pending_players(self, oid, players, *, now):
        if self.held or not self.doc:
            return False
        self.players = players
        return True

    async def push_contest(self, oid, entry, *, now):
        if self.held or not self.doc:
            return False
        self.pushed = entry
        return True

    async def take_pending_match(self, oid, *, now):
        return None if self.held else (dict(self.doc) if self.doc else None)

    async def claim_pending_match(self, oid, *, now, expect=None):
        return None

    async def find_validated_by_id(self, oid):
        return getattr(self, "validated", None)


def _svc(q, monkeypatch):
    svc = MatchService.__new__(MatchService)
    svc.q = q

    async def _same(match):
        return match

    async def _steam(discord_id):
        return "1"

    monkeypatch.setattr(svc, "_recompute_deltas", _same)
    monkeypatch.setattr(svc, "seat_steam_id", _steam)
    return svc


def _patch(svc):
    edit = EditingService(svc).patch_players(
        OID,
        [SeatPatch(seat=0, placement=1), SeatPatch(seat=1, placement=0)],
        actor_is_staff=False,
    )
    return asyncio.run(edit)


def test_a_patch_writes_the_players_alone(monkeypatch):
    q = _Q(_doc())
    _patch(_svc(q, monkeypatch))
    assert [p["placement"] for p in q.players] == [1, 0]


def test_a_patch_during_an_approve_is_refused(monkeypatch):
    with pytest.raises(ApprovingError, match="being approved"):
        _patch(_svc(_Q(_doc(), held=True), monkeypatch))


def test_every_unregistered_player_is_named_and_nothing_is_written(monkeypatch):
    from app.features.matches.errors import MatchServiceError

    q = _Q(_doc())
    svc = _svc(q, monkeypatch)

    async def _unlinked(discord_id):
        raise MatchServiceError(f"User {discord_id} not found")

    monkeypatch.setattr(svc, "seat_steam_id", _unlinked)
    a, b = "100000000000000003", "100000000000000004"
    edit = EditingService(svc).patch_players(
        OID,
        [SeatPatch(seat=0, discord_id=a), SeatPatch(seat=1, discord_id=b)],
        actor_is_staff=False,
    )
    with pytest.raises(
        MatchServiceError, match=f"<@{a}>, <@{b}> are not registered yet"
    ):
        asyncio.run(edit)
    assert q.players is None


def test_a_contest_is_pushed_not_rewritten(monkeypatch):
    q = _Q(_doc())
    asyncio.run(
        EditingService(_svc(q, monkeypatch)).contest_report(
            OID, "100000000000000002", "wrong order"
        )
    )
    assert q.pushed == {
        "contestor_discord_id": "100000000000000002",
        "reason": "wrong order",
    }


def test_a_contest_on_a_vanished_match_is_404(monkeypatch):
    with pytest.raises(NotFoundError):
        asyncio.run(
            EditingService(_svc(_Q(None), monkeypatch)).contest_report(OID, "1", "x")
        )


def test_a_delete_during_an_approve_is_refused(monkeypatch):
    with pytest.raises(ApprovingError):
        asyncio.run(
            EditingService(
                _svc(_Q(_doc(), held=True), monkeypatch)
            ).delete_pending_match(OID)
        )


def test_a_second_approve_says_it_is_being_approved(monkeypatch):
    with pytest.raises(ApprovingError):
        asyncio.run(_svc(_Q(_doc()), monkeypatch).approve_match(OID, "1"))


def test_an_approve_repeated_after_it_landed_gets_the_same_answer(monkeypatch):
    q = _Q(None)
    when = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
    q.validated = {**_doc(), "approved_at": when, "approver_discord_id": "7"}
    svc = _svc(q, monkeypatch)

    async def _ratings(match, **_):
        return [SimpleNamespace(mu=31.0), SimpleNamespace(mu=19.0)]

    monkeypatch.setattr(svc, "get_players_ranking", _ratings)
    out = asyncio.run(svc.approve_match(OID, "1"))
    assert (out["match_id"], out["approved_at"], out["approver_discord_id"]) == (
        OID,
        when,
        "7",
    )
    # Rank roles follow a repeat too, from the ratings as they stand.
    assert out["affected_players"] == [
        {"discord_id": "100000000000000001", "rating_mu": 31.0},
        {"discord_id": "100000000000000002", "rating_mu": 19.0},
    ]


def test_a_claim_older_than_the_ttl_no_longer_holds():
    now = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
    assert _editable(now) == {
        "$or": [
            {"approving_at": {"$exists": False}},
            {"approving_at": {"$lt": now - CLAIM_TTL}},
        ]
    }
    assert timedelta(minutes=1) < CLAIM_TTL <= timedelta(minutes=15)


class _Col:
    def __init__(self, *, repeated):
        self.repeated = repeated
        self.indexes = {"_id_": {}, "pending_matches_save_file_hash_idx": {}}

    async def index_information(self):
        return dict(self.indexes)

    async def drop_index(self, name):
        if name not in self.indexes:
            raise OperationFailure("index not found", code=27)
        del self.indexes[name]

    async def create_index(self, keys, *, name, unique=False, **_):
        if unique and self.repeated:
            raise OperationFailure("E11000 duplicate key", code=11000)
        self.indexes[name] = {"unique": unique}


def _ensure(col):
    repo = MatchRepository.__new__(MatchRepository)
    repo._pending = col
    asyncio.run(repo._ensure_composition_index())
    return col.indexes


def test_the_composition_index_becomes_unique():
    indexes = _ensure(_Col(repeated=False))
    assert "pending_matches_save_file_hash_idx" not in indexes
    assert indexes["pending_matches_save_file_hash_uq"] == {"unique": True}


def test_repeated_compositions_keep_the_plain_index_and_startup_goes_on():
    indexes = _ensure(_Col(repeated=True))
    assert "pending_matches_save_file_hash_uq" not in indexes
    assert indexes["pending_matches_save_file_hash_idx"] == {"unique": False}


def test_a_boot_with_the_unique_index_changes_nothing():
    col = _Col(repeated=True)
    col.indexes = {"_id_": {}, "pending_matches_save_file_hash_uq": {"unique": True}}
    assert _ensure(col) == {
        "_id_": {},
        "pending_matches_save_file_hash_uq": {"unique": True},
    }


def test_a_plain_index_dropped_by_another_process_is_not_an_error():
    col = _Col(repeated=False)

    async def _gone():
        return {"_id_": {}, "pending_matches_save_file_hash_idx": {}}

    col.index_information = _gone
    col.indexes = {"_id_": {}}
    assert _ensure(col)["pending_matches_save_file_hash_uq"] == {"unique": True}


def test_a_clashing_rating_write_can_be_retried_and_nothing_leaks():
    from app.features.matches.approval import _write_failure
    from app.features.matches.errors import MatchServiceError, RetryLaterError

    clash = OperationFailure(
        "WriteConflict",
        code=112,
        details={"errorLabels": ["TransientTransactionError"]},
    )
    assert isinstance(_write_failure("approval", clash), RetryLaterError)
    other = _write_failure("revert", RuntimeError("secret internals"))
    assert type(other) is MatchServiceError
    assert "secret" not in str(other)


def test_form_and_query_values_the_schema_cannot_bound_are_refused():
    from fastapi import HTTPException

    from app.features.matches import router_v2

    with pytest.raises(HTTPException) as quota:
        asyncio.run(router_v2.get_sub_quota(discord_id="99999999999999999999", db=None))
    assert quota.value.status_code == 400
    with pytest.raises(HTTPException) as upload:
        asyncio.run(
            router_v2.upload_match(
                file=None, is_cloud="true", discord_message_id="1", actor="1", db=None
            )
        )
    assert upload.value.status_code == 400
