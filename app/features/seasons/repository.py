"""The `seasons` collection. The open season for an edition is the row with
the greatest `started_at`: season N's end is season N+1's start."""

from __future__ import annotations

from typing import Any

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_SEASONS, GAMES_DB

# The season a match with no season_id belongs to, or an older one: v1 stamped
# none. The ones v1 approved in Civ 6's Season 7 were stamped at the cutover;
# every Civ 7 match v1 approved is Season 1's.
V1_LABELS: dict[str, str] = {"civ6": "Season 6", "civ7": "Season 1"}


class SeasonNotSeededError(RuntimeError):
    """No season row exists for an edition."""


_CACHE: dict[str, dict[str, Any]] = {}
# Season names by id. A season's name never changes once its row exists.
_LABELS: dict[Any, str] = {}


def clear_cache() -> None:
    _CACHE.clear()
    _LABELS.clear()


class SeasonsRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._seasons: AsyncCollection = client[GAMES_DB][COL_SEASONS]

    async def ensure_indexes(self) -> None:
        await self._seasons.create_index(
            [("edition", ASCENDING), ("started_at", DESCENDING)],
            name="current_season_lookup",
        )
        await self._seasons.create_index(
            [("edition", ASCENDING), ("label", ASCENDING)],
            unique=True,
            name="unique_label_per_edition",
        )

    async def read_open(
        self, edition: str, *, session: AsyncClientSession | None = None
    ) -> dict[str, Any] | None:
        """The open season read now, not from the cache: inside a transaction,
        the one its writes will land in."""
        return await self._seasons.find_one(
            {"edition": edition}, sort=[("started_at", DESCENDING)], session=session
        )

    async def get_current_season(self, edition: str) -> dict[str, Any]:
        cached = _CACHE.get(edition)
        if cached is None:
            doc = await self._seasons.find_one(
                {"edition": edition}, sort=[("started_at", DESCENDING)]
            )
            if doc is None:
                raise SeasonNotSeededError(f"No season is recorded for {edition}.")
            _CACHE[edition] = doc
            cached = doc
        return dict(cached)

    async def label_for(self, edition: str, season_id: Any = None) -> str | None:
        """A season's name, such as "Season 6": the season with this id, or the
        edition's open season; None when there is no such row."""
        if season_id is None:
            try:
                return str((await self.get_current_season(edition))["label"])
            except SeasonNotSeededError:
                return None
        if season_id not in _LABELS:
            doc = await self._seasons.find_one({"_id": season_id}, {"label": 1})
            if doc is None:
                return None
            _LABELS[season_id] = str(doc["label"])
        return _LABELS[season_id]


__all__ = [
    "V1_LABELS",
    "SeasonNotSeededError",
    "SeasonsRepository",
    "clear_cache",
]
