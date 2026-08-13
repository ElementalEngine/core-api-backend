from __future__ import annotations

from pymongo import AsyncMongoClient

from app.shared.persistence.mongo_queries import MongoQueries


class MatchRepository(MongoQueries):
    def __init__(self, client: AsyncMongoClient) -> None:
        super().__init__(client)


__all__ = ["MatchRepository"]
