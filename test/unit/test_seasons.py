"""Entry 11's cache and seed shape, without a database (D59, D60).

What should break these: changing a seed label, dropping `started_at`,
reintroducing `ended_at`, binding the wrong database or collection, making
the cache one slot instead of one per edition, removing the cache so every
call hits Mongo, handing out the cached document by reference, or dropping
either index declaration. Mongo actually BUILDING and enforcing them is
Entry 11's dev dry-run -- D60 keeps it out of here.

`asyncio.run` rather than a pytest-asyncio marker: the plugin is not a
dependency, and an unrecognised marker leaves the coroutine un-awaited and
the test green having run nothing (D86 Rule 1).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from pymongo.errors import BulkWriteError

from app.core.constants import COL_SEASONS, GAMES_DB
from app.features.seasons.repository import (
    SeasonNotSeededError,
    SeasonsAlreadySeededError,
    SeasonsRepository,
    clear_cache,
    seed_documents,
)

NOW = datetime(2026, 8, 25, 12, 0, 0, tzinfo=UTC)


class FakeCollection:
    """Counts reads, so 'served from the cache' is asserted rather than hoped."""

    def __init__(self, docs):
        self._docs = docs
        self.find_one_calls = 0

    async def find_one(self, flt, sort=None):
        self.find_one_calls += 1
        matches = [d for d in self._docs if d["edition"] == flt["edition"]]
        if not matches:
            return None
        # Honour `sort` the way Mongo does -- return the first row after
        # sorting -- so the repository's sort DIRECTION is under test. A fake
        # that picked the max itself would pass whichever way it was written.
        for field, direction in reversed(sort or []):
            matches.sort(key=lambda d: d[field], reverse=direction < 0)
        return matches[0]


def _repo(docs):
    """Built through __init__ on a nested-dict client, so a wrong GAMES_DB or
    COL_SEASONS raises KeyError here instead of passing silently."""
    collection = FakeCollection(docs)
    return SeasonsRepository({GAMES_DB: {COL_SEASONS: collection}}), collection


@pytest.fixture(autouse=True)
def _clean_cache():
    clear_cache()
    yield
    clear_cache()


def test_seed_is_one_row_per_edition_with_no_ended_at():
    docs = seed_documents(NOW)
    assert [d["edition"] for d in docs] == ["civ6", "civ7"]
    assert [d["label"] for d in docs] == ["Season 6", "Season 1"]
    assert all(d["started_at"] == NOW for d in docs)
    # D106 removed ended_at. Nothing is nullable and nothing is absent by
    # convention, so the document carries exactly these three fields.
    assert all(set(d) == {"edition", "label", "started_at"} for d in docs)


def test_second_call_is_served_from_the_cache():
    repo, collection = _repo(seed_documents(NOW))
    first = asyncio.run(repo.get_current_season("civ6"))
    second = asyncio.run(repo.get_current_season("civ6"))
    assert first == second
    assert collection.find_one_calls == 1


def test_a_caller_cannot_poison_the_cache():
    repo, collection = _repo(seed_documents(NOW))
    first = asyncio.run(repo.get_current_season("civ6"))
    first["label"] = "tampered"
    second = asyncio.run(repo.get_current_season("civ6"))
    assert second["label"] == "Season 6"
    assert collection.find_one_calls == 1


def test_cache_holds_one_entry_per_edition():
    repo, collection = _repo(seed_documents(NOW))
    assert asyncio.run(repo.get_current_season("civ6"))["label"] == "Season 6"
    assert asyncio.run(repo.get_current_season("civ7"))["label"] == "Season 1"
    # Two editions, two reads. A one-slot cache makes this three or more; a
    # cache not keyed by edition returns Season 6 for civ7.
    assert collection.find_one_calls == 2


def test_latest_started_at_wins_per_edition():
    later = datetime(2026, 12, 1, tzinfo=UTC)
    docs = seed_documents(NOW) + [
        {"edition": "civ6", "label": "Season 7", "started_at": later}
    ]
    repo, _ = _repo(docs)
    # Entry 11 check 7, the rollover rehearsal, as a permanent test: civ6
    # rolls and civ7 does not (D105).
    assert asyncio.run(repo.get_current_season("civ6"))["label"] == "Season 7"
    assert asyncio.run(repo.get_current_season("civ7"))["label"] == "Season 1"


def test_clear_cache_forces_a_reread():
    repo, collection = _repo(seed_documents(NOW))
    asyncio.run(repo.get_current_season("civ6"))
    clear_cache()
    asyncio.run(repo.get_current_season("civ6"))
    assert collection.find_one_calls == 2


class RecordingCollection(FakeCollection):
    """Captures index declarations. Mongo building them is Entry 11's dev
    dry-run (D60 forbids a DB here); what this pins is that we still ASK for
    them -- D106 records the risk of someone dropping the unique index to
    silence the E11000 that proves the seed cannot double-run."""

    def __init__(self):
        super().__init__([])
        self.indexes = []

    async def create_index(self, keys, name=None, unique=False):
        self.indexes.append({"keys": keys, "name": name, "unique": unique})


def test_ensure_indexes_declares_the_lookup_and_the_unique_label():
    collection = RecordingCollection()
    repo = SeasonsRepository({GAMES_DB: {COL_SEASONS: collection}})
    asyncio.run(repo.ensure_indexes())
    by_name = {i["name"]: i for i in collection.indexes}
    assert set(by_name) == {"current_season_lookup", "unique_label_per_edition"}
    # Descending on started_at is what makes "greatest started_at" an index
    # scan rather than a sort (D106).
    assert by_name["current_season_lookup"]["keys"] == [
        ("edition", 1),
        ("started_at", -1),
    ]
    assert by_name["current_season_lookup"]["unique"] is False
    assert by_name["unique_label_per_edition"]["keys"] == [
        ("edition", 1),
        ("label", 1),
    ]
    assert by_name["unique_label_per_edition"]["unique"] is True


class _InsertResult:
    def __init__(self, ids):
        self.inserted_ids = ids


class SeedingCollection(FakeCollection):
    """insert_many, with the failure mode chosen by the caller."""

    def __init__(self, raise_codes=None):
        super().__init__([])
        self.raise_codes = raise_codes
        self.inserted = None

    async def insert_many(self, documents):
        if self.raise_codes is not None:
            raise BulkWriteError(
                {
                    "writeErrors": [
                        {"index": i, "code": c} for i, c in enumerate(self.raise_codes)
                    ]
                }
            )
        self.inserted = documents
        return _InsertResult(list(range(len(documents))))


def _seed_repo(collection):
    return SeasonsRepository({GAMES_DB: {COL_SEASONS: collection}})


def test_seed_inserts_every_document_and_returns_the_count():
    collection = SeedingCollection()
    docs = seed_documents(NOW)
    assert asyncio.run(_seed_repo(collection).seed(docs)) == 2
    assert collection.inserted == docs


def test_reseeding_raises_already_seeded_not_a_driver_error():
    # insert_many raises BulkWriteError; DuplicateKeyError comes only from
    # insert_one. Catching the wrong one made a re-seed traceback instead of
    # reporting cleanly -- Entry 11 check 6 found it on the cluster.
    collection = SeedingCollection(raise_codes=[11000])
    with pytest.raises(SeasonsAlreadySeededError):
        asyncio.run(_seed_repo(collection).seed(seed_documents(NOW)))


def test_a_non_duplicate_bulk_error_is_not_swallowed():
    # The case that stops the translation becoming a blanket except: a write
    # that failed for any other reason must keep its own type.
    collection = SeedingCollection(raise_codes=[66])
    with pytest.raises(BulkWriteError):
        asyncio.run(_seed_repo(collection).seed(seed_documents(NOW)))


def test_missing_row_raises_rather_than_returning_none():
    repo, _ = _repo([])
    with pytest.raises(SeasonNotSeededError):
        asyncio.run(repo.get_current_season("civ6"))
