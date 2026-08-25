"""The `seasons` collection: playbook Entry 11, contract C6.

No router. `season_id` reaches clients only as a stamped field on lobby
documents, and `season_label` from the cache below -- there is no second
consumer, so there is no route (D95).

The open season for an edition is the row with the greatest `started_at`.
There is no `ended_at` and no open/closed flag: season N's end *is* season
N+1's start, one value rather than two that must agree (D106).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import BulkWriteError

from app.core.constants import COL_SEASONS, GAMES_DB

# Lowercase in the database and on the wire; Mite's CivEdition is CIV6/CIV7
# at the TypeScript layer only (D87). The keys are the editions -- seasons
# needs no EDITIONS tuple of its own beside civdata's.
SEED_LABELS: Dict[str, str] = {"civ6": "Season 6", "civ7": "Season 1"}

DUPLICATE_KEY = 11000


class SeasonNotSeededError(RuntimeError):
    """No season row exists for an edition.

    Returning None instead would surface as an AttributeError where the lobby
    stamps season_id, one layer away from the actual cause.
    """


class SeasonsAlreadySeededError(RuntimeError):
    """A seed run was rejected by unique_label_per_edition.

    Not a fault: it is the protection that stops a seed -- and later the
    rollover script -- running twice (D106), named rather than left as a
    driver traceback.
    """


# Module-level, not per-instance: repositories are constructed per request,
# so an instance cache would never see a second call. Two entries, keyed by
# edition (D95, D105). clear_cache() exists because a process-global dict
# leaks between tests, and Entry 11 check 8 has to be able to fail.
_CACHE: Dict[str, Dict[str, Any]] = {}


def clear_cache() -> None:
    _CACHE.clear()


def seed_documents(now: datetime) -> List[Dict[str, Any]]:
    """One row per edition. `now` is the tracking start, not the true start of
    either season -- the only boundary that will ever be guessed (D104).
    """
    return [
        {"edition": edition, "label": label, "started_at": now}
        for edition, label in SEED_LABELS.items()
    ]


class SeasonsRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._seasons: AsyncCollection = client[GAMES_DB][COL_SEASONS]

    async def ensure_indexes(self) -> None:
        # create_index creates the collection, so Entry 11's createCollection
        # step needs no separate call. Index first, then seed: with the unique
        # index in place a second seed fails with E11000 and changes nothing,
        # which is also what stops the rollover script running twice.
        await self._seasons.create_index(
            [("edition", ASCENDING), ("started_at", DESCENDING)],
            name="current_season_lookup",
        )
        await self._seasons.create_index(
            [("edition", ASCENDING), ("label", ASCENDING)],
            unique=True,
            name="unique_label_per_edition",
        )

    async def seed(self, documents: List[Dict[str, Any]]) -> int:
        """Insert the seed rows. Deliberately not an upsert: a second run must
        be rejected by unique_label_per_edition (Entry 11 checks 5, 6).

        insert_many raises BulkWriteError; DuplicateKeyError comes only from
        insert_one. Translated here rather than in the caller because
        scripts/ sits outside both `mypy files` and `testpaths`, so logic
        left there is checked by nothing (item 87).
        """
        try:
            result = await self._seasons.insert_many(documents)
        except BulkWriteError as exc:
            write_errors = (exc.details or {}).get("writeErrors") or []
            if write_errors and all(
                e.get("code") == DUPLICATE_KEY for e in write_errors
            ):
                raise SeasonsAlreadySeededError(
                    "seasons already holds a row for one of these labels"
                ) from exc
            raise
        return len(result.inserted_ids)

    async def get_current_season(self, edition: str) -> Dict[str, Any]:
        cached = _CACHE.get(edition)
        if cached is None:
            doc = await self._seasons.find_one(
                {"edition": edition}, sort=[("started_at", DESCENDING)]
            )
            if doc is None:
                raise SeasonNotSeededError(f"no season row for edition {edition!r}")
            _CACHE[edition] = doc
            cached = doc
        # Copy on the way out. Every value is immutable, so shallow suffices,
        # and a caller mutating the result cannot poison the entry every later
        # caller reads.
        return dict(cached)


__all__ = [
    "DUPLICATE_KEY",
    "SEED_LABELS",
    "SeasonNotSeededError",
    "SeasonsAlreadySeededError",
    "SeasonsRepository",
    "clear_cache",
    "seed_documents",
]
