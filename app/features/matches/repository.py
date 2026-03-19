from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient

from app.shared.persistence.mongo_queries import MongoQueries


class MatchRepository(MongoQueries):
    def __init__(self, client: AsyncIOMotorClient) -> None:
        super().__init__(client)


__all__ = ["MatchRepository"]
