"""Playbook Entry 12 Half B — two hashes at two strengths (D83).

save_bytes_sha256 answers "is this the same file?" and hard-blocks in BOTH
collections; save_file_hash stays the composition hash and answers "is this the
same game?" (D133 -- no rename in Wave 1).

The tests that matter: check 6 (an approved match's save cannot be re-uploaded
into a fresh approvable pending match -- the live double-rating path) and check
9 (a concurrent double upload returns repeated, not a 500).

Also asserted: the field is never written empty. The partial index filters on
{$exists: true}, so a "" or None default IS indexed, and two such documents
collide on E11000 -- which would break the second approval of any pre-C10
match.
"""

from __future__ import annotations

import asyncio
import hashlib

from pymongo.errors import DuplicateKeyError

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
        }
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
        insert_raises=False,
    ):
        self._pending_bytes = pending_bytes
        self._validated_bytes = validated_bytes
        self._pending_hash = pending_hash
        self._insert_raises = insert_raises
        self.inserted = None

    async def find_pending_by_bytes(self, h):
        return self._pending_bytes

    async def find_validated_by_bytes(self, h):
        return self._validated_bytes

    async def find_pending_by_hash(self, h):
        return self._pending_hash

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
    return await svc.create_from_save(RAW, "1", False, "1")


def test_same_bytes_in_pending_is_repeated_by_file(monkeypatch):
    from bson import ObjectId

    q = _Q(pending_bytes={"_id": ObjectId(), "save_bytes_sha256": BYTES_HASH})
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is True
    assert res["repeated_by"] == "file"


def test_approved_match_blocks_reupload(monkeypatch):
    """Entry 12 check 6 -- the live double-rating path."""
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


def test_insert_race_returns_repeated_not_500(monkeypatch):
    """Entry 12 check 9 -- E11000 becomes an answer, not an error."""
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


def test_fresh_upload_stores_a_non_empty_byte_hash(monkeypatch):
    q = _Q()
    res = asyncio.run(_upload(_svc(q, monkeypatch)))
    assert res["repeated"] is False
    assert q.inserted["save_bytes_sha256"] == BYTES_HASH
    # Never empty: "" and None both satisfy {$exists: true} and would collide.
    assert q.inserted["save_bytes_sha256"]
