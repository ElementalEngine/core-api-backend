from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pymongo import AsyncMongoClient

from app.core.config import settings
from app.core.constants import DB_SERVER_MEMBERS, GAMES_DB
from app.features.auth.repository import AuthRepository
from app.features.civdata.repository import CivDataRepository
from app.features.infractions.repository import (
    create_indexes as create_infraction_indexes,
)
from app.features.lobbies.repository import LobbyRepository
from app.features.matches.repository import MatchRepository
from app.features.ratings.repository import RatingsRepository
from app.features.seasons.repository import SeasonsRepository

logger = logging.getLogger("app.db")


@asynccontextmanager
async def db_lifespan(app: FastAPI):
    uri = settings.mongo_url.get_secret_value()
    timeout_ms = settings.mongodb_timeout_ms
    min_pool = settings.mongodb_min_pool_size
    max_pool = settings.mongodb_max_pool_size

    client: AsyncMongoClient | None = None
    try:
        client = AsyncMongoClient(
            uri,
            uuidRepresentation="standard",
            minPoolSize=min_pool,
            maxPoolSize=max_pool,
            connectTimeoutMS=timeout_ms,
            serverSelectionTimeoutMS=timeout_ms,
            socketTimeoutMS=timeout_ms,
            retryReads=True,
            retryWrites=True,
            tz_aware=True,
        )

        await client.admin.command("ping")

        app.state.mongodb_client = client
        logger.info(
            "🟢 MongoDB connected (games=%s, members=%s)", GAMES_DB, DB_SERVER_MEMBERS
        )

        await AuthRepository(client).ensure_indexes()
        logger.info("🟢 Auth indexes ensured")

        await create_infraction_indexes(client)
        logger.info("🟢 Infraction indexes ensured")

        await RatingsRepository(client).ensure_indexes()
        logger.info("🟢 Rating event indexes ensured")

        await MatchRepository(client).ensure_indexes()
        logger.info("🟢 Match indexes ensured")

        await CivDataRepository(client).ensure_indexes()
        logger.info("🟢 Civ data indexes ensured")

        await SeasonsRepository(client).ensure_indexes()
        logger.info("🟢 Season indexes ensured")

        await LobbyRepository(client).ensure_indexes()
        logger.info("🟢 Lobby indexes ensured")

        yield
    except Exception:
        logger.exception("🔴 Failed to connect to MongoDB")
        raise
    finally:
        if client is not None:
            await client.close()
            logger.info("🟠 MongoDB connection closed")
