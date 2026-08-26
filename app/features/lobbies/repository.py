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

# `closed_at` is set on `complete` AND on `cancelled` (spec section 3), so
# testing it alone cannot drift out of step with `phase`.
#
# ⚠ `{"$exists": False}` CANNOT be used: MongoDB rewrites it as $not, which a
# partial index rejects (CannotCreateIndex 67). `{"closed_at": None}` selects
# ABSENT fields as well as explicit nulls -- measured -- so it is a drop-in
# for the intended meaning (D175, Correction 73a).
OPEN_LOBBY = {"closed_at": None}

# ⚠ The seat index needs the second clause. An EMPTY seats array indexes one
# `null` key, so without it only ONE seat-less lobby could be open fleet-wide
# -- the state of every lobby in the instant after creation (Correction 73b).
# Filtering on the indexed path itself is Entry 12's proven idiom.
SEATED_OPEN_LOBBY = {**OPEN_LOBBY, "seats.discord_id": {"$exists": True}}

# Spec section 3: `cancelled` is a phase and the reason is a field. An
# eviction is not a host cancelling, so it carries its own reason.
PHASE_CANCELLED = "cancelled"
CANCEL_ABANDONED = "abandoned"


class LobbyInsertRefused(RuntimeError):
    """A unique index refused an insert. `index` names which one."""

    def __init__(self, index: str) -> None:
        super().__init__(f"refused by {index}")
        self.index = index


def _refusing_index(exc: DuplicateKeyError) -> str:
    """The index Mongo refused on, read from `keyPattern`.

    Not parsed out of the message: the wording is not a contract, the key
    pattern is.
    """
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
        # ⚠ MULTIKEY. Measured: it enforces one active seat per player ACROSS
        # documents -- D71's canary, and the structural fix for O-18. It does
        # NOT enforce uniqueness WITHIN a document, because MongoDB
        # de-duplicates multikey keys per document; that is the `$ne` clause
        # on the seat write instead (D176). Null seats still collide, which
        # is why `discord_id` is never null and never absent (D71).
        await self._lobbies.create_index(
            [("seats.discord_id", ASCENDING)],
            unique=True,
            partialFilterExpression=SEATED_OPEN_LOBBY,
            name="one_active_seat_per_player",
        )
        # The aggregate is maintained by upsert-and-$inc. Without a unique
        # key two concurrent writers both miss and both insert, producing
        # duplicate rows that then diverge silently. No partial filter: every
        # aggregate row carries all four fields.
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
        """Insert and return the stored document, `_id` included.

        A refusal is one of D71's two invariants: the channel already holds an
        open lobby, or a seated player is seated elsewhere. The index name is
        carried out so the caller can say which.
        """
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
        """Write a validated seat array under D77's revision gate.

        The array written is the array `validate_seats` accepted, so no
        arrangement can reach the collection without having been checked --
        a targeted `$push`/`$set` would validate a prospective list and then
        write something else.

        ⚠ `absent_player` carries D176's `$ne` clause, and only when the
        target is not currently seated, which is the case D176 measured at
        `[1, 0]`. The `revision` clause reaches the same outcome on its own
        -- any competing seat write bumps it and this filter stops matching
        -- so the two are not independent guarantees; the measured one stays.

        Returns the updated document, or None when nothing matched: stale
        revision, or the player was seated in between. The caller re-reads to
        say which (spec section 9).
        """
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

    async def find_by_id(self, lobby_id: ObjectId) -> dict[str, Any] | None:
        """One lobby by id, open or closed.

        ⚠ No `closed_at` clause, unlike `find_open`. A completed lobby stays
        readable: D73's `complete` row shows everything, and that is the
        result screen the Activity renders once a draft ends.
        """
        return await self._lobbies.find_one({"_id": lobby_id})

    async def evict_stale(
        self, players: list[str], cutoff: datetime, now: datetime
    ) -> list[dict[str, Any]]:
        """Close open lobbies holding any of `players` and untouched since
        `cutoff`. Returns the ones closed.

        ⚠ `updated_at` repeats on the update filter, not only the find. A
        lobby touched between the two would otherwise be closed on the
        strength of a reading that is no longer true.
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
                    "phase": PHASE_CANCELLED,
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
        """Open lobbies for a guild, newest first.

        One query behind both reads: `/active` passes a channel and takes the
        first, browse passes the optional filters. Two queries would drift --
        civup has six near-identical ones (D180).

        ⚠ `guild_id` is required and never defaulted. A service token is
        per-service, not per-guild, so an unfiltered read would expose every
        lobby on the deployment to any holder of it.

        The predicate repeats OPEN_LOBBY exactly, which is what lets
        `one_active_lobby_per_channel` serve it: a partial index is eligible
        only when the query implies its filter.
        """
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
