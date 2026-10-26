from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from app.core.constants import COL_MATCH_SETUPS, GAMES_DB
from app.features.match_setups.lobby import CANCELLED, OPEN

OPEN_LOBBY = {"command": "lobby", "status": OPEN}


class SeatedElsewhere(RuntimeError):
    """A player already sits in another open lobby."""


class MatchSetupRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._setups: AsyncCollection = client[GAMES_DB][COL_MATCH_SETUPS]

    async def ensure_indexes(self) -> None:
        await self._setups.create_index(
            [("players.discord_id", ASCENDING)],
            unique=True,
            partialFilterExpression=OPEN_LOBBY,
            name="one_open_seat_per_player",
        )
        await self._setups.create_index(
            [("guild_id", ASCENDING), ("created_at", DESCENDING)],
            partialFilterExpression=OPEN_LOBBY,
            name="open_lobbies_by_guild",
        )
        await self._setups.create_index(
            [("updated_at", ASCENDING)],
            partialFilterExpression=OPEN_LOBBY,
            name="idle_open_lobbies",
        )

    async def insert(self, setup: dict[str, Any]) -> ObjectId:
        try:
            return (await self._setups.insert_one(setup)).inserted_id
        except DuplicateKeyError as exc:
            raise SeatedElsewhere from exc

    async def lobby(self, lobby_id: str) -> dict[str, Any] | None:
        try:
            oid = ObjectId(lobby_id)
        except InvalidId:
            return None
        return await self._setups.find_one({"_id": oid, "command": "lobby"})

    async def update_lobby(
        self, lobby: dict[str, Any], changes: dict[str, Any], now: datetime
    ) -> dict[str, Any] | None:
        """Apply changes made from this copy of the lobby; None if it changed
        since, so the caller reads it again."""
        try:
            return await self._setups.find_one_and_update(
                {"_id": lobby["_id"], "revision": lobby["revision"]},
                {"$set": {**changes, "updated_at": now}, "$inc": {"revision": 1}},
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError as exc:
            raise SeatedElsewhere from exc

    async def open_lobbies(self, guild_id: str) -> list[dict[str, Any]]:
        cursor = self._setups.find({**OPEN_LOBBY, "guild_id": guild_id})
        return await cursor.sort("created_at", DESCENDING).to_list(None)

    async def close_idle(self, cutoff: datetime, now: datetime) -> list[dict[str, Any]]:
        """Close every open lobby untouched since the cutoff; the closed ones."""
        idle = {**OPEN_LOBBY, "updated_at": {"$lt": cutoff}}
        closed = []
        async for lobby in self._setups.find(idle, {"_id": 1}):
            done = await self._setups.find_one_and_update(
                {"_id": lobby["_id"], **idle},
                {
                    "$set": {
                        "status": CANCELLED,
                        "cancel_reason": "idle",
                        "closed_at": now,
                    },
                    "$inc": {"revision": 1},
                },
                return_document=ReturnDocument.AFTER,
            )
            if done is not None:
                closed.append(done)
        return closed


__all__ = ["MatchSetupRepository", "SeatedElsewhere"]
