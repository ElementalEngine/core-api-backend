"""The `lobbies` and `lobby_stats` collections: playbook Entry 7, contract C5.

Half A only (D166): the collections and their three indexes. The domain --
creation, seats, phases, aggregation -- is Half B.

One repository for both collections, following the house boundary: matches
binds four, auth three, ratings two. `lobby_stats` is a rebuildable cache
derived from `lobbies` (Entry 7), written in the same operation, with no
second consumer yet (D13) -- so it is not its own feature.
"""

from __future__ import annotations

from pymongo import ASCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

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


__all__ = ["OPEN_LOBBY", "SEATED_OPEN_LOBBY", "LobbyRepository"]
