from __future__ import annotations

import asyncio
import hashlib

import pytest
from pymongo.errors import DuplicateKeyError

from app.features.matches.ingest import IngestService
from app.features.matches.service import MatchService

RAW = b"CIV6" + b"\x00" * 64
BYTES_HASH = hashlib.sha256(RAW).hexdigest()

PARSED = {
    "game": "civ6",
    "turn": 1,
    "map_type": "Pangaea",
    "game_mode": "ffa",
    "parser_version": "1.5",
    "players": [
        {
            "steam_id": "1",
            "user_name": "a",
            "civ": "LEADER_DIDO",
            "team": 0,
            "placement": 0,
        },
        {
            "steam_id": "2",
            "user_name": "b",
            "civ": "LEADER_SHAKA",
            "team": 1,
            "placement": 1,
        },
    ],
}


class _Q:
    """Records calls; each find_* returns whatever the test seeded."""

    def __init__(
        self,
        *,
        pending_bytes=None,
        validated_bytes=None,
        pending_hash=None,
        validated_hash=(),
        insert_raises=False,
    ):
        self._pending_bytes = pending_bytes
        self._validated_bytes = validated_bytes
        self._pending_hash = pending_hash
        self._validated_hash = list(validated_hash)
        self._insert_raises = insert_raises
        self.inserted = None

    async def find_pending_by_bytes(self, h):
        return self._pending_bytes

    async def find_validated_by_bytes(self, h):
        return self._validated_bytes

    async def find_pending_by_hash(self, h):
        return self._pending_hash

    async def find_validated_by_hash(self, h):
        return self._validated_hash

    async def insert_pending_match(self, doc, *, session=None):
        self.inserted = doc
        if self._insert_raises:
            raise DuplicateKeyError("dup")
        from bson import ObjectId

        return ObjectId()


def _svc(q, monkeypatch):
    svc = MatchService.__new__(MatchService)
    svc.q = q
    monkeypatch.setattr(svc, "_parse_save", lambda b: dict(PARSED))

    async def _identity(match):
        return match

    monkeypatch.setattr(svc, "match_id_to_discord", _identity)
    monkeypatch.setattr(svc, "_recompute_deltas", _identity)
    return svc


async def _upload(svc):
    return await IngestService(svc).create_from_save(RAW, "1", False, "1")


def test_same_bytes_in_pending_is_repeated_by_file(monkeypatch):
    from bson import ObjectId

    q = _Q(pending_bytes={"_id": ObjectId(), "save_bytes_sha256": BYTES_HASH})
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is True
    assert res["repeated_by"] == "file"


def test_approved_match_blocks_reupload(monkeypatch):
    from bson import ObjectId

    q = _Q(validated_bytes={"_id": ObjectId(), "save_bytes_sha256": BYTES_HASH})
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is True
    assert res["repeated_by"] == "file"


def test_same_lineup_different_file_is_repeated_by_composition(monkeypatch):
    from bson import ObjectId

    q = _Q(pending_hash={"_id": ObjectId(), "save_file_hash": "x"})
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is True
    assert res["repeated_by"] == "composition"


def _registered(svc, monkeypatch):
    async def _map(match):
        for p in match.players:
            p.discord_id = {"1": "11", "2": "22"}[p.steam_id]
        return match

    monkeypatch.setattr(svc, "match_id_to_discord", _map)
    return svc


def test_another_save_of_an_approved_game_points_to_it(monkeypatch):
    from bson import ObjectId

    approved = {
        "_id": ObjectId(),
        "players": [{"discord_id": "11"}, {"discord_id": "22"}],
    }
    q = _Q(validated_hash=[approved])
    res = asyncio.run(_upload(_registered(_svc(q, monkeypatch), monkeypatch)))
    assert res["repeated"] is True
    assert res["repeated_by"] == "approved"
    assert res["match_id"] == str(approved["_id"])
    assert q.inserted is None


def test_a_seat_staff_assigned_still_counts_as_the_same_game(monkeypatch):
    from bson import ObjectId

    # The save maps one seat to a registered player; staff assigned the other.
    approved = {
        "_id": ObjectId(),
        "players": [{"discord_id": "11"}, {"discord_id": "77"}],
    }
    q = _Q(validated_hash=[approved])
    svc = _svc(q, monkeypatch)

    async def _one_registered(match):
        match.players[0].discord_id = "11"
        match.players[1].discord_id = "-2"
        return match

    monkeypatch.setattr(svc, "match_id_to_discord", _one_registered)
    res = asyncio.run(_upload(svc))
    assert res["repeated_by"] == "approved"


def test_the_same_picks_by_other_players_are_a_new_game(monkeypatch):
    from bson import ObjectId

    approved = {
        "_id": ObjectId(),
        "players": [{"discord_id": "11"}, {"discord_id": "33"}],
    }
    q = _Q(validated_hash=[approved])
    res = asyncio.run(_upload(_registered(_svc(q, monkeypatch), monkeypatch)))
    assert res["repeated"] is False
    assert q.inserted is not None


def test_insert_race_returns_repeated_not_500(monkeypatch):
    from bson import ObjectId

    q = _Q(insert_raises=True)
    winner = {"_id": ObjectId(), "save_bytes_sha256": BYTES_HASH}

    calls = {"n": 0}

    async def _find(h):
        calls["n"] += 1
        return winner if calls["n"] > 1 else None

    q.find_pending_by_bytes = _find
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is True
    assert res["repeated_by"] == "file"


def test_a_composition_race_returns_repeated_not_500(monkeypatch):
    """Two different saves of one game, uploaded at once: the unique
    composition index refuses the second insert."""
    from bson import ObjectId

    q = _Q(insert_raises=True)
    winner = {"_id": ObjectId(), "save_file_hash": "x"}
    calls = {"n": 0}

    async def _find(h):
        calls["n"] += 1
        return winner if calls["n"] > 1 else None

    q.find_pending_by_hash = _find
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is True
    assert res["repeated_by"] == "composition"


def test_a_save_with_one_side_is_refused_before_rating(monkeypatch):
    from app.features.matches.errors import MatchServiceError

    q = _Q()
    svc = _svc(q, monkeypatch)
    one = {**PARSED, "players": PARSED["players"][:1]}
    monkeypatch.setattr(svc, "_parse_save", lambda b: dict(one))
    with pytest.raises(MatchServiceError, match="at least two"):
        asyncio.run(_upload(svc))
    assert q.inserted is None


def test_fresh_upload_stores_a_non_empty_byte_hash(monkeypatch):
    q = _Q()
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is False
    assert q.inserted["save_bytes_sha256"] == BYTES_HASH
    # Never empty: "" and None both satisfy {$exists: true} and would collide.
    assert q.inserted["save_bytes_sha256"]
