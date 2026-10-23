"""Edits and approval do not overlap.

An approve reads the match once, then moves it. A patch, contest or delete
landing in between would be lost or would wipe the claim, so each write is
guarded and a refused one says why.

What should break this file: a write path that replaces the whole pending
document, or a guarded write whose miss is reported as anything but 404 or
"being approved".
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

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

    async def push_contest(self, oid, entry, message_id, *, now):
        if self.held or not self.doc:
            return False
        self.pushed = (entry, message_id)
        return True

    async def take_pending_match(self, oid, *, now):
        return None if self.held else (dict(self.doc) if self.doc else None)

    async def claim_pending_match(self, oid, *, now):
        return None


def _svc(q, monkeypatch):
    svc = MatchService.__new__(MatchService)
    svc.q = q

    async def _same(match):
        return match

    async def _steam(discord_id):
        return "1"

    monkeypatch.setattr(svc, "_recompute_deltas", _same)
    monkeypatch.setattr(svc, "discord_to_steam_id", _steam)
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


def test_a_contest_is_pushed_not_rewritten(monkeypatch):
    q = _Q(_doc())
    asyncio.run(
        EditingService(_svc(q, monkeypatch)).contest_report(
            OID, "100000000000000002", "wrong order"
        )
    )
    entry, message_id = q.pushed
    assert entry["contestor_discord_id"] == "100000000000000002"
    assert entry["reason"] == "wrong order"
    assert message_id is None


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
