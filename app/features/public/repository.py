"""The website's reads outside the stat documents: totals, and the names
players registered with."""

from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_USERS, DB_SERVER_MEMBERS, GAMES_DB
from app.features.matches.repository import COL_VALIDATED_MATCHES

NAME_FIELDS = ("display_name", "discord_username", "user_name")


class PublicRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._users: AsyncCollection = client[DB_SERVER_MEMBERS][COL_USERS]
        self._validated: AsyncCollection = client[GAMES_DB][COL_VALIDATED_MATCHES]

    async def registered_players(self) -> int:
        return await self._users.count_documents({})

    async def matches_played(self) -> int:
        return await self._validated.count_documents({})

    async def names_by_discord_id(
        self, discord_ids: list[str]
    ) -> dict[str, dict[str, Any]]:
        """Each registered player's name fields, by Discord id."""
        if not discord_ids:
            return {}
        cursor = self._users.find(
            {"discord_id": {"$in": discord_ids}},
            {"_id": 0, "discord_id": 1, **dict.fromkeys(NAME_FIELDS, 1)},
        )
        return {doc["discord_id"]: doc async for doc in cursor}


__all__ = ["NAME_FIELDS", "PublicRepository"]
