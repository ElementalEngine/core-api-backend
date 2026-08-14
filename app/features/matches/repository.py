from __future__ import annotations

from pymongo import ASCENDING, AsyncMongoClient

from app.shared.persistence.mongo_queries import MongoQueries


class MatchRepository(MongoQueries):
    def __init__(self, client: AsyncMongoClient) -> None:
        super().__init__(client)

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


__all__ = ["MatchRepository"]
