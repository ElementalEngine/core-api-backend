"""The website's reads outside the stat documents: totals, the names players
registered with, and a season's games and draft bans."""

from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import (
    COL_MATCH_SETUPS,
    COL_USERS,
    DB_SERVER_MEMBERS,
    GAMES_DB,
)
from app.features.matches.repository import COL_VALIDATED_MATCHES

NAME_FIELDS = ("display_name", "discord_username", "user_name")


class PublicRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._users: AsyncCollection = client[DB_SERVER_MEMBERS][COL_USERS]
        self._validated: AsyncCollection = client[GAMES_DB][COL_VALIDATED_MATCHES]
        self._setups: AsyncCollection = client[GAMES_DB][COL_MATCH_SETUPS]

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

    async def season_games(self, season_id: Any) -> list[dict[str, Any]]:
        """A season's approved real-time games, with only the fields the stats
        count. Cloud games have no season of their own."""
        cursor = self._validated.find(
            {"season_id": season_id, "is_cloud": {"$ne": True}},
            {
                "_id": 0,
                "game_mode": 1,
                "map_type": 1,
                "players.leader": 1,
                "players.civ": 1,
                "players.placement": 1,
                "players.delta": 1,
                "players.subbed_out": 1,
            },
        )
        return await cursor.to_list()

    async def season_draft_bans(self, season_id: Any) -> list[dict[str, Any]]:
        """Each game set up in a season: its mode and its bans. A setup gets its
        season when its draft is dealt."""
        cursor = self._setups.find(
            {"season_id": season_id}, {"_id": 0, "mode": 1, "bans": 1}
        )
        return await cursor.to_list()


__all__ = ["NAME_FIELDS", "PublicRepository"]
