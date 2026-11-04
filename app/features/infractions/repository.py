from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

from pymongo import ASCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import DB_SERVER_MEMBERS

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


# ─── Pending suspensions ──────────────────────────────────────────────────────


async def create_pending_suspension(
    db: AsyncMongoClient,
    discord_id: str,
    punishment_type: str,
    reason: str | None,
) -> None:
    col = suspensions_due_col(db)
    now = datetime.now(UTC)
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
