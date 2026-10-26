from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_MATCH_SETUPS, GAMES_DB


class MatchSetupRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._setups: AsyncCollection = client[GAMES_DB][COL_MATCH_SETUPS]

    async def insert(self, setup: dict[str, Any]) -> None:
        await self._setups.insert_one(setup)


__all__ = ["MatchSetupRepository"]
