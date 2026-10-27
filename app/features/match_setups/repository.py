from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from app.core.constants import COL_MATCH_SETUPS, GAMES_DB
from app.features.match_setups.lobby import CANCELLED, OPEN, SETUP

OPEN_LOBBY = {"command": "lobby", "status": OPEN}


class SeatedElsewhere(RuntimeError):
    """A player already sits in another open lobby."""


class CodeTaken(RuntimeError):
    """Another open lobby in the server has this code."""


def _refused(exc: DuplicateKeyError) -> RuntimeError:
    taken = "code" in ((exc.details or {}).get("keyPattern") or {})
    return CodeTaken() if taken else SeatedElsewhere()


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
            [("guild_id", ASCENDING), ("code", ASCENDING)],
            unique=True,
            partialFilterExpression=OPEN_LOBBY,
            name="one_open_lobby_per_code",
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
        await self._setups.create_index(
            [("deadline", ASCENDING)],
            partialFilterExpression=OPEN_LOBBY,
            name="open_lobbies_by_deadline",
        )

    async def insert(self, setup: dict[str, Any]) -> ObjectId:
        try:
            return (await self._setups.insert_one(setup)).inserted_id
        except DuplicateKeyError as exc:
            raise _refused(exc) from exc

    async def lobby(self, lobby_id: str) -> dict[str, Any] | None:
        try:
            oid = ObjectId(lobby_id)
        except InvalidId:
            return None
        return await self._setups.find_one({"_id": oid, "command": "lobby"})

    async def lobby_by_code(self, guild_id: str, code: str) -> dict[str, Any] | None:
        return await self._setups.find_one(
            {**OPEN_LOBBY, "guild_id": guild_id, "code": code}
        )

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
            raise _refused(exc) from exc

    async def open_lobbies(
        self, guild_id: str, host_id: str | None = None
    ) -> list[dict[str, Any]]:
        """The lobbies players can see; a host's own, in setup or not."""
        mine = {"host_id": host_id} if host_id else {"phase": {"$ne": SETUP}}
        cursor = self._setups.find({**OPEN_LOBBY, **mine, "guild_id": guild_id})
        return await cursor.sort("created_at", DESCENDING).to_list(None)

    async def due(self, now: datetime) -> list[dict[str, Any]]:
        """The open lobbies whose countdown, vote or blind picks are past their time."""
        cursor = self._setups.find({**OPEN_LOBBY, "deadline": {"$lt": now}})
        return await cursor.sort("deadline", ASCENDING).to_list(None)

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
                        "deadline": None,
                        "closed_at": now,
                    },
                    "$inc": {"revision": 1},
                },
                return_document=ReturnDocument.AFTER,
            )
            if done is not None:
                closed.append(done)
        return closed


__all__ = ["CodeTaken", "MatchSetupRepository", "SeatedElsewhere"]
