from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId
from bson.int64 import Int64
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import OperationFailure

from app.core.constants import COL_SUB_EVENTS, COL_USERS, DB_SERVER_MEMBERS, GAMES_DB

logger = logging.getLogger(__name__)

COL_PENDING_MATCHES = "pending_matches"
COL_VALIDATED_MATCHES = "validated_matches"

# An approve takes seconds; a claim older than this was left by a crash.
CLAIM_TTL = timedelta(minutes=5)


def _editable(now: datetime) -> dict[str, Any]:
    """Filter for a pending match that no live approve holds."""
    return {
        "$or": [
            {"approving_at": {"$exists": False}},
            {"approving_at": {"$lt": now - CLAIM_TTL}},
        ]
    }


class MatchRepository:
    """Every read and write against the match collections."""

    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client
        mr = client[GAMES_DB]
        self._pending: AsyncCollection = mr[COL_PENDING_MATCHES]
        self._validated: AsyncCollection = mr[COL_VALIDATED_MATCHES]
        self._sub_events: AsyncCollection = mr[COL_SUB_EVENTS]
        self._users: AsyncCollection = client[DB_SERVER_MEMBERS][COL_USERS]

    async def start_session(self) -> AsyncClientSession:
        return self._client.start_session()

    async def get_user_by_discord_id(self, discord_id: str) -> dict[str, Any] | None:
        return await self._users.find_one({"discord_id": discord_id})

    async def get_user_by_steam_id(self, steam_id: str) -> dict[str, Any] | None:
        return await self._users.find_one(
            {
                "$or": [
                    {"steam_id": steam_id},
                    {"linked_platform": "steam", "linked_account_id": steam_id},
                ]
            }
        )

    async def get_users_by_steam_ids(
        self, steam_ids: list[str]
    ) -> dict[str, dict[str, Any]]:
        """The registered user for each Steam id found, in one query."""
        cursor = self._users.find(
            {
                "$or": [
                    {"steam_id": {"$in": steam_ids}},
                    {
                        "linked_platform": "steam",
                        "linked_account_id": {"$in": steam_ids},
                    },
                ]
            }
        )
        found: dict[str, dict[str, Any]] = {}
        async for doc in cursor:
            keys = [doc.get("steam_id")]
            if doc.get("linked_platform") == "steam":
                keys.append(doc.get("linked_account_id"))
            for key in keys:
                if key in steam_ids and key not in found:
                    found[key] = doc
        return found

    async def find_pending_by_hash(self, save_file_hash: str) -> dict[str, Any] | None:
        return await self._pending.find_one({"save_file_hash": save_file_hash})

    async def find_pending_by_bytes(
        self, save_bytes_sha256: str
    ) -> dict[str, Any] | None:
        return await self._pending.find_one({"save_bytes_sha256": save_bytes_sha256})

    async def find_validated_by_bytes(
        self, save_bytes_sha256: str
    ) -> dict[str, Any] | None:
        """The same file among approved matches. The unique indexes are per collection,
        so approval moving a match out of pending would otherwise reopen it."""
        return await self._validated.find_one({"save_bytes_sha256": save_bytes_sha256})

    async def find_validated_by_hash(self, save_file_hash: str) -> list[dict[str, Any]]:
        """Approved games with this composition, newest first."""
        cursor = self._validated.find({"save_file_hash": save_file_hash})
        return await cursor.sort("_id", DESCENDING).limit(20).to_list()

    async def find_pending_by_id(self, oid: ObjectId) -> dict[str, Any] | None:
        return await self._pending.find_one({"_id": oid})

    async def find_validated_by_id(self, oid: ObjectId) -> dict[str, Any] | None:
        return await self._validated.find_one({"_id": oid})

    async def claim_pending_match(
        self, oid: ObjectId, *, now: datetime
    ) -> dict[str, Any] | None:
        """Claim a pending match for approval; None means already claimed or gone."""
        return await self._pending.find_one_and_update(
            {"_id": oid, **_editable(now)},
            {"$set": {"approving_at": now}},
        )

    async def release_pending_claim(
        self, oid: ObjectId, *, claimed_at: datetime
    ) -> None:
        # Only its own claim: a later approve may have taken over a lapsed one.
        await self._pending.update_one(
            {"_id": oid, "approving_at": claimed_at}, {"$unset": {"approving_at": ""}}
        )

    async def insert_pending_match(
        self, match_doc: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> ObjectId:
        res = await self._pending.insert_one(dict(match_doc), session=session)
        return res.inserted_id

    # The three guarded writes below miss (None or False) when the match is
    # gone or an approve holds it; the caller asks which.

    async def set_pending_players(
        self, oid: ObjectId, players: list[dict[str, Any]], *, now: datetime
    ) -> bool:
        # Players only, so a contest written meanwhile is kept.
        res = await self._pending.update_one(
            {"_id": oid, **_editable(now)}, {"$set": {"players": players}}
        )
        return res.matched_count == 1

    async def push_contest(
        self,
        oid: ObjectId,
        entry: Mapping[str, Any],
        *,
        now: datetime,
    ) -> bool:
        res = await self._pending.update_one(
            {"_id": oid, **_editable(now)},
            {"$push": {"contest_report_list": dict(entry)}},
        )
        return res.matched_count == 1

    async def take_pending_match(
        self, oid: ObjectId, *, now: datetime
    ) -> dict[str, Any] | None:
        return await self._pending.find_one_and_delete({"_id": oid, **_editable(now)})

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

    async def count_sub_ins_since(self, discord_id: str, since: datetime) -> int:
        # Served by sub_events_player_recent_idx; bounded by the window, not
        # by the TTL monitor, which can lag.
        return await self._sub_events.count_documents(
            {"discord_id": Int64(discord_id), "occurred_at": {"$gte": since}}
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
        await self._ensure_composition_index()
        # An upload is checked against approved games' compositions too.
        await self._validated.create_index(
            [("save_file_hash", ASCENDING)], name="validated_matches_save_file_hash_idx"
        )
        # A player's games, newest first: the stats history and its summary.
        await self._validated.create_index(
            [("players.discord_id", ASCENDING), ("_id", DESCENDING)],
            name="validated_matches_player_recent_idx",
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

    async def _ensure_composition_index(self) -> None:
        # One pending report per game, so two uploads at once cannot both get
        # in. If repeats are already stored, the plain index stays.
        plain, unique = (
            "pending_matches_save_file_hash_idx",
            "pending_matches_save_file_hash_uq",
        )
        held = await self._pending.index_information()
        if unique in held:
            return
        if plain in held:
            try:
                await self._pending.drop_index(plain)
            except OperationFailure as exc:
                if exc.code != 27:  # IndexNotFound: another process dropped it
                    raise
        try:
            await self._pending.create_index(
                [("save_file_hash", ASCENDING)],
                unique=True,
                partialFilterExpression={"save_file_hash": {"$exists": True}},
                name=unique,
            )
        except OperationFailure as exc:
            if exc.code != 11000:
                raise
            logger.error("Repeated save_file_hash in pending_matches; index not unique")
            await self._pending.create_index(
                [("save_file_hash", ASCENDING)], name=plain
            )


__all__ = ["MatchRepository"]
