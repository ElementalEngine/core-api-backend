from __future__ import annotations

from typing import Any, Dict, List

from pymongo import ASCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from app.core.constants import COL_CIV_DATA, GAMES_DB


class CivDataRepository:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._civ_data: AsyncCollection = client[GAMES_DB][COL_CIV_DATA]

    async def ensure_indexes(self) -> None:
        # Leaders and civs share one namespace per edition, so the pair is
        # unique without a partial filter -- every document has both fields.
        await self._civ_data.create_index(
            [("edition", ASCENDING), ("token", ASCENDING)],
            unique=True,
            name="civ_data_edition_token_uq",
        )

    async def seed(
        self, edition: str, documents: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Upsert one edition's documents, then drop tokens the file dropped.

        Upsert rather than drop-and-insert: the collection is never empty
        mid-run, so a deployed route cannot serve a half-seeded payload.
        """
        upserted = 0
        modified = 0
        for doc in documents:
            result = await self._civ_data.replace_one(
                {"edition": edition, "token": doc["token"]}, doc, upsert=True
            )
            upserted += 1 if result.upserted_id is not None else 0
            modified += result.modified_count
        pruned = await self._civ_data.delete_many(
            {
                "edition": edition,
                "token": {"$nin": [doc["token"] for doc in documents]},
            }
        )
        return {
            "upserted": upserted,
            "modified": modified,
            "pruned": pruned.deleted_count,
            "total": await self._civ_data.count_documents({"edition": edition}),
        }


__all__ = ["CivDataRepository"]
