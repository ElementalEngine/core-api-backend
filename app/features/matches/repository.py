from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, Mapping, Optional

from bson import ObjectId
from bson.int64 import Int64
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_SUB_EVENTS, COL_USERS, DB_SERVER_MEMBERS, GAMES_DB

# ---- match collection names (single source of truth, D157) ----

COL_PENDING_MATCHES = "pending_matches"
COL_VALIDATED_MATCHES = "validated_matches"


class MatchRepository:
    """Every read and write against the match collections (D157, S6).

    The three handles live here because nothing else uses them and
    ensure_indexes below already declares their indexes.
    """

    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client
        mr = client[GAMES_DB]
        self._pending: AsyncCollection = mr[COL_PENDING_MATCHES]
        self._validated: AsyncCollection = mr[COL_VALIDATED_MATCHES]
        self._sub_events: AsyncCollection = mr[COL_SUB_EVENTS]
        self._users: AsyncCollection = client[DB_SERVER_MEMBERS][COL_USERS]

    # -------------------- session and user lookups --------------------
    # From MongoQueries, which S7 deleted. MatchService is the only caller
    # of the lookups; auth keeps its own against the same collection.

    async def start_session(self) -> AsyncClientSession:
        return self._client.start_session()

    async def get_user_by_discord_id(self, discord_id: str) -> Optional[Dict[str, Any]]:
        return await self._users.find_one({"discord_id": discord_id})

    async def get_user_by_steam_id(self, steam_id: str) -> Optional[Dict[str, Any]]:
        return await self._users.find_one(
            {
                "$or": [
                    {"steam_id": steam_id},
                    {"linked_platform": "steam", "linked_account_id": steam_id},
                ]
            }
        )

    async def find_pending_by_hash(
        self, save_file_hash: str
    ) -> Optional[Dict[str, Any]]:
        return await self._pending.find_one({"save_file_hash": save_file_hash})

    async def find_pending_by_bytes(
        self, save_bytes_sha256: str
    ) -> Optional[Dict[str, Any]]:
        return await self._pending.find_one({"save_bytes_sha256": save_bytes_sha256})

    async def find_validated_by_bytes(
        self, save_bytes_sha256: str
    ) -> Optional[Dict[str, Any]]:
        """The cross-collection half of the dedup. The unique indexes are
        per-collection, so approval moving a document out of pending_matches
        is what reopened the double-rating path. D83, Entry 12."""
        return await self._validated.find_one({"save_bytes_sha256": save_bytes_sha256})

    async def find_pending_by_id(self, oid: ObjectId) -> Optional[Dict[str, Any]]:
        return await self._pending.find_one({"_id": oid})

    async def find_validated_by_id(self, oid: ObjectId) -> Optional[Dict[str, Any]]:
        return await self._validated.find_one({"_id": oid})

    async def claim_pending_match(
        self, oid: ObjectId, *, now: datetime
    ) -> Optional[Dict[str, Any]]:
        """Claim a pending match for approval; None means already claimed or gone.

        D84's claim. Pending-ness is which collection the document is in, so
        the claim is an additive field rather than a status transition.
        """
        return await self._pending.find_one_and_update(
            {"_id": oid, "approving_at": {"$exists": False}},
            {"$set": {"approving_at": now}},
        )

    async def release_pending_claim(self, oid: ObjectId) -> None:
        await self._pending.update_one({"_id": oid}, {"$unset": {"approving_at": ""}})

    async def insert_pending_match(
        self, match_doc: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> ObjectId:
        res = await self._pending.insert_one(dict(match_doc), session=session)
        return res.inserted_id

    async def update_pending_match_set(
        self,
        oid: ObjectId,
        changes: Mapping[str, Any],
        *,
        session: AsyncClientSession | None = None,
    ) -> bool:
        res = await self._pending.update_one(
            {"_id": oid}, {"$set": dict(changes)}, session=session
        )
        return res.matched_count == 1

    async def replace_pending_match(
        self,
        oid: ObjectId,
        match_doc: Mapping[str, Any],
        *,
        session: AsyncClientSession | None = None,
    ) -> bool:
        res = await self._pending.replace_one(
            {"_id": oid}, dict(match_doc), session=session
        )
        return res.matched_count == 1

    async def delete_pending_match(
        self, oid: ObjectId, *, session: AsyncClientSession | None = None
    ) -> bool:
        res = await self._pending.delete_one({"_id": oid}, session=session)
        return res.deleted_count == 1

    async def delete_validated_match(
        self, oid: ObjectId, *, session: AsyncClientSession | None = None
    ) -> bool:
        res = await self._validated.delete_one({"_id": oid}, session=session)
        return res.deleted_count == 1

    # -------------------- validated matches --------------------

    async def insert_validated_match(
        self, match_doc: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> ObjectId:
        res = await self._validated.insert_one(dict(match_doc), session=session)
        return res.inserted_id

    # -------------------- subs --------------------

    async def record_sub_in(
        self,
        discord_id: str,
        match_id: ObjectId,
        *,
        session: AsyncClientSession | None = None,
    ) -> None:
        # One dated row per sub-in, not a counter. A TTL on occurred_at is
        # what makes the 30-day quota roll; a counter only ever went up.
        await self._sub_events.insert_one(
            {
                "discord_id": Int64(discord_id),
                "match_id": match_id,
                "occurred_at": datetime.now(UTC),
            },
            session=session,
        )

    async def remove_sub_in(
        self,
        discord_id: str,
        match_id: ObjectId,
        *,
        session: AsyncClientSession | None = None,
    ) -> None:
        # Reverting deletes that match's row. The counter it replaces could
        # be driven below zero by a repeated revert; this cannot.
        await self._sub_events.delete_one(
            {"discord_id": Int64(discord_id), "match_id": match_id},
            session=session,
        )

    async def ensure_indexes(self) -> None:
        # Partial on {$exists: true}: a plain unique index collapses every
        # missing value into one null and rejects the second document that
        # has no byte hash. Legacy documents have none. Playbook Entry 12.
        for col, name in (
            (self._pending, "pending_matches_save_bytes_uq"),
            (self._validated, "validated_matches_save_bytes_uq"),
        ):
            await col.create_index(
                [("save_bytes_sha256", ASCENDING)],
                unique=True,
                partialFilterExpression={"save_bytes_sha256": {"$exists": True}},
                name=name,
            )
        # find_pending_by_hash's composition lookup, a collection scan today.
        await self._pending.create_index(
            [("save_file_hash", ASCENDING)],
            name="pending_matches_save_file_hash_idx",
        )
        # 30 days. The TTL is storage hygiene, not the rule -- the quota
        # query has to be time-bounded anyway, because the monitor can lag.
        await self._sub_events.create_index(
            [("occurred_at", ASCENDING)],
            expireAfterSeconds=2592000,
            name="sub_events_ttl_idx",
        )
        # The TTL index is on occurred_at alone and cannot serve the quota
        # query, which filters on discord_id first.
        await self._sub_events.create_index(
            [("discord_id", ASCENDING), ("occurred_at", DESCENDING)],
            name="sub_events_player_recent_idx",
        )


__all__ = ["MatchRepository"]
