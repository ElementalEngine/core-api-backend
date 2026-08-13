from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Final

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo import ASCENDING
from pymongo.collection import ReturnDocument

from app.features.infractions.models import (
    ActiveSuspension,
    PendingSuspensionDocument,
    SuspensionDocument,
)
from app.shared.persistence.mongo_queries import DB_SERVER_MEMBERS

COL_SUSPENSIONS: Final[str] = "suspensions"
COL_SUSPENSIONS_DUE: Final[str] = "suspensions_due"


# ─── Collection accessors ─────────────────────────────────────────────────────


def suspensions_col(db: AsyncMongoClient) -> AsyncCollection:  # type: ignore[type-arg]
    return db[DB_SERVER_MEMBERS][COL_SUSPENSIONS]


def suspensions_due_col(db: AsyncMongoClient) -> AsyncCollection:  # type: ignore[type-arg]
    return db[DB_SERVER_MEMBERS][COL_SUSPENSIONS_DUE]


# ─── Index creation (called once at startup in lifespan) ─────────────────────


async def create_indexes(db: AsyncMongoClient) -> None:
    col = suspensions_col(db)
    await col.create_index(
        [("suspended", ASCENDING), ("ends", ASCENDING)],
        name="suspensions_suspended_ends_idx",
    )
    await col.create_index(
        [("discord_id", ASCENDING)],
        unique=True,
        name="suspensions_discord_id_uq",
    )


# ─── Core CRUD ────────────────────────────────────────────────────────────────


async def find_suspension(
    db: AsyncMongoClient, discord_id: str
) -> SuspensionDocument | None:
    col = suspensions_col(db)
    result: dict[str, Any] | None = await col.find_one({"discord_id": discord_id})
    if result is None:
        return None
    return SuspensionDocument.model_validate(result)


async def upsert_suspension(
    db: AsyncMongoClient, discord_id: str, update: dict[str, Any]
) -> None:
    col = suspensions_col(db)
    await col.update_one(
        {"discord_id": discord_id},
        {"$set": update},
        upsert=True,
    )


async def find_or_create_suspension(
    db: AsyncMongoClient, discord_id: str
) -> SuspensionDocument:
    """Atomic upsert — never a read-then-write race.

    $setOnInsert runs only when a new document is created; existing documents
    are returned as-is, preserving any existing tier/suspension state.
    """
    col = suspensions_col(db)
    _default_ir: dict[str, Any] = {"tier": 0, "decays": None}
    result: dict[str, Any] | None = await col.find_one_and_update(
        {"discord_id": discord_id},
        {
            "$setOnInsert": {
                "discord_id": discord_id,
                "suspended": False,
                "ends": None,
                "suspendedRoles": [],
                "quit": _default_ir,
                "minor": _default_ir,
                "moderate": _default_ir,
                "major": _default_ir,
                "extreme": _default_ir,
                "active_category": None,
            }
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    if result is None:
        raise RuntimeError(
            f"find_one_and_update with upsert returned None for discord_id={discord_id!r}"
        )
    return SuspensionDocument.model_validate(result)


# ─── Pending suspensions ──────────────────────────────────────────────────────


async def find_pending_suspension(
    db: AsyncMongoClient, discord_id: str
) -> PendingSuspensionDocument | None:
    col = suspensions_due_col(db)
    result: dict[str, Any] | None = await col.find_one({"_id": discord_id})
    if result is None:
        return None
    return PendingSuspensionDocument.model_validate(result)


async def create_pending_suspension(
    db: AsyncMongoClient,
    discord_id: str,
    punishment_type: str,
    reason: str | None,
) -> None:
    col = suspensions_due_col(db)
    now = datetime.now(timezone.utc)
    await col.replace_one(
        {"_id": discord_id},
        {
            "_id": discord_id,
            "punishment_type": punishment_type,
            "reason": reason,
            "created_at": now,
        },
        upsert=True,
    )


async def delete_pending_suspension(db: AsyncMongoClient, discord_id: str) -> None:
    col = suspensions_due_col(db)
    await col.delete_one({"_id": discord_id})


# ─── Scheduler recovery ───────────────────────────────────────────────────────


async def get_active_suspensions(db: AsyncMongoClient) -> list[ActiveSuspension]:
    """Query uses compound index { suspended: 1, ends: 1 }."""
    col = suspensions_col(db)
    now = datetime.now(timezone.utc)
    cursor = col.find(
        {"suspended": True, "ends": {"$gt": now}},
        {"discord_id": 1, "ends": 1, "_id": 0},
    )
    results: list[dict[str, Any]] = await cursor.to_list(length=None)
    return [ActiveSuspension.model_validate(r) for r in results]


async def get_overdue_suspensions(db: AsyncMongoClient) -> list[ActiveSuspension]:
    """Suspensions whose `ends` has already passed but the flag was never
    cleared — happens if the bot was offline (crash, restart, redeploy) when
    the in-memory expiry timer should have fired. Mirrors
    get_active_suspensions exactly, with the `ends` bound inverted; uses the
    same compound index.
    """
    col = suspensions_col(db)
    now = datetime.now(timezone.utc)
    cursor = col.find(
        {"suspended": True, "ends": {"$lte": now}},
        {"discord_id": 1, "ends": 1, "_id": 0},
    )
    results: list[dict[str, Any]] = await cursor.to_list(length=None)
    return [ActiveSuspension.model_validate(r) for r in results]
