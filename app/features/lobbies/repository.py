"""The `lobbies` and `lobby_stats` collections: playbook Entry 7, contract C5.

Half A only (D166): the collections and their three indexes. The domain --
creation, seats, phases, aggregation -- is Half B.

One repository for both collections, following the house boundary: matches
binds four, auth three, ratings two. `lobby_stats` is a rebuildable cache
derived from `lobbies` (Entry 7), written in the same operation, with no
second consumer yet (D13) -- so it is not its own feature.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from app.core.constants import COL_LOBBIES, COL_LOBBY_STATS, GAMES_DB
from app.features.lobbies.phases import CANCEL_ABANDONED, CANCELLED, COMPLETE

OPEN_LOBBY = {"closed_at": None}

SEATED_OPEN_LOBBY = {**OPEN_LOBBY, "seats.discord_id": {"$exists": True}}

# Spec section 3: `cancelled` is a phase and the reason is a field. An
# eviction is not a host cancelling, so it carries its own reason.


class LobbyInsertRefused(RuntimeError):
    """A unique index refused an insert. `index` names which one."""

    def __init__(self, index: str) -> None:
        super().__init__(f"refused by {index}")
        self.index = index


def _refusing_index(exc: DuplicateKeyError) -> str:
    """The index Mongo refused on, read from `keyPattern`."""
    pattern = (exc.details or {}).get("keyPattern") or {}
    if "seats.discord_id" in pattern:
        return "one_active_seat_per_player"
    if "channel_id" in pattern:
        return "one_active_lobby_per_channel"
    return "unknown"


class LobbyRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        games = client[GAMES_DB]
        self._lobbies: AsyncCollection = games[COL_LOBBIES]
        self._lobby_stats: AsyncCollection = games[COL_LOBBY_STATS]

    async def ensure_indexes(self) -> None:
        # create_index creates the collection; both are empty at creation, so
        # the builds are free and index-first is trivially safe.
        await self._lobbies.create_index(
            [("guild_id", ASCENDING), ("channel_id", ASCENDING)],
            unique=True,
            partialFilterExpression=OPEN_LOBBY,
            name="one_active_lobby_per_channel",
        )
        await self._lobbies.create_index(
            [("seats.discord_id", ASCENDING)],
            unique=True,
            partialFilterExpression=SEATED_OPEN_LOBBY,
            name="one_active_seat_per_player",
        )
        await self._lobby_stats.create_index(
            [
                ("season_id", ASCENDING),
                ("edition", ASCENDING),
                ("game_type", ASCENDING),
                ("token", ASCENDING),
            ],
            unique=True,
            name="aggregate_key",
        )

    async def insert_lobby(self, document: dict[str, Any]) -> dict[str, Any]:
        """Insert and return the stored document, `_id` included."""
        try:
            result = await self._lobbies.insert_one(document)
        except DuplicateKeyError as exc:
            raise LobbyInsertRefused(_refusing_index(exc)) from exc
        return {**document, "_id": result.inserted_id}

    async def replace_seats(
        self,
        lobby_id: ObjectId,
        expected_revision: int,
        seats: list[dict[str, Any]],
        now: datetime,
        *,
        absent_player: str | None = None,
    ) -> dict[str, Any] | None:
        """Write a validated seat array under's revision gate."""
        query: dict[str, Any] = {"_id": lobby_id, "revision": expected_revision}
        if absent_player is not None:
            query["seats.discord_id"] = {"$ne": absent_player}
        return await self._lobbies.find_one_and_update(
            query,
            {
                "$set": {"seats": seats, "updated_at": now},
                "$inc": {"revision": 1},
            },
            return_document=ReturnDocument.AFTER,
        )

    async def apply_changes(
        self,
        lobby_id: ObjectId,
        expected_revision: int,
        changes: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any] | None:
        """Revision-guarded write of top-level lobby fields."""
        return await self._lobbies.find_one_and_update(
            {"_id": lobby_id, "revision": expected_revision},
            {"$set": {**changes, "updated_at": now}, "$inc": {"revision": 1}},
            return_document=ReturnDocument.AFTER,
        )

    async def claim_for_posting(
        self, guild_id: str, now: datetime
    ) -> dict[str, Any] | None:
        """Claim the oldest unposted finished lobby for this guild, or None."""
        return await self._lobbies.find_one_and_update(
            {"guild_id": guild_id, "phase": COMPLETE, "posted_at": None},
            {"$set": {"posted_at": now}},
            sort=[("closed_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    async def claim_for_stats(
        self, lobby_id: ObjectId, now: datetime
    ) -> dict[str, Any] | None:
        """Claim a completed lobby for counting, once and only once."""
        return await self._lobbies.find_one_and_update(
            {
                "_id": lobby_id,
                "phase": COMPLETE,
                "stats_written_at": {"$exists": False},
            },
            {"$set": {"stats_written_at": now}},
            return_document=ReturnDocument.BEFORE,
        )

    async def add_contributions(
        self, key: dict[str, Any], counts: dict[str, dict[str, int]]
    ) -> int:
        """Upsert-and-$inc one aggregate row per token."""
        written = 0
        for token, fields in counts.items():
            increments = {field: value for field, value in fields.items() if value}
            if not increments:
                continue
            await self._lobby_stats.update_one(
                {**key, "token": token},
                {"$inc": increments, "$setOnInsert": {**key, "token": token}},
                upsert=True,
            )
            written += 1
        return written

    async def find_by_id(self, lobby_id: ObjectId) -> dict[str, Any] | None:
        """One lobby by id, open or closed."""
        return await self._lobbies.find_one({"_id": lobby_id})

    async def evict_stale(
        self, players: list[str], cutoff: datetime, now: datetime
    ) -> list[dict[str, Any]]:
        """
        Close open lobbies holding any of `players` and untouched since `cutoff`.
        Returns the ones closed.
        """
        stale = {**OPEN_LOBBY, "updated_at": {"$lt": cutoff}}
        found = await self._lobbies.find(
            {**stale, "seats.discord_id": {"$in": players}}
        ).to_list(None)
        if not found:
            return []
        await self._lobbies.update_many(
            {**stale, "_id": {"$in": [lobby["_id"] for lobby in found]}},
            {
                "$set": {
                    "closed_at": now,
                    "phase": CANCELLED,
                    "cancel_reason": CANCEL_ABANDONED,
                },
                "$inc": {"revision": 1},
            },
        )
        return found

    async def find_open(
        self,
        guild_id: str,
        channel_id: str | None = None,
        edition: str | None = None,
        game_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Open lobbies for a guild, newest first."""
        query: dict[str, Any] = {"guild_id": guild_id, **OPEN_LOBBY}
        if channel_id is not None:
            query["channel_id"] = channel_id
        if edition is not None:
            query["edition"] = edition
        if game_type is not None:
            query["game_type"] = game_type
        cursor = self._lobbies.find(query).sort("created_at", DESCENDING)
        return await cursor.to_list(None)


__all__ = [
    "OPEN_LOBBY",
    "SEATED_OPEN_LOBBY",
    "LobbyInsertRefused",
    "LobbyRepository",
]
