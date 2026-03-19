from __future__ import annotations

from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.errors import AppDependencyError


def get_database(request: Request) -> AsyncIOMotorClient:
    client = getattr(request.app.state, "mongodb_client", None)
    if client is None:
        raise AppDependencyError("Mongo client not initialized")
    return client


def get_mongo_database(request: Request) -> AsyncIOMotorDatabase:
    database = getattr(request.app.state, "mongodb", None)
    if database is None:
        raise AppDependencyError("Mongo database not initialized")
    return database
