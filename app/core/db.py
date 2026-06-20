from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import settings
from app.core.logging import configure_logging
from app.features.auth.repository import AuthRepository
from app.features.infractions.repository import create_indexes as create_infraction_indexes

configure_logging()
logger = logging.getLogger("app.db")


@asynccontextmanager
async def db_lifespan(app: FastAPI):
    uri = settings.mongo_url.get_secret_value()
    timeout_ms = settings.mongodb_timeout_ms
    min_pool = settings.mongodb_min_pool_size
    max_pool = settings.mongodb_max_pool_size

    client: Optional[AsyncIOMotorClient] = None
    try:
        client = AsyncIOMotorClient(
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

        db: AsyncIOMotorDatabase = client[settings.mongo_db_name]
        app.state.mongodb_client = client
        app.state.mongodb = db
        logger.info("🟢 MongoDB connected (db=%s)", db.name)

        await AuthRepository(client).ensure_indexes()
        logger.info("🟢 Auth indexes ensured")
        
        await create_infraction_indexes(client)
        logger.info("🟢 Infraction indexes ensured")

        yield
    except Exception:
        logger.exception("🔴 Failed to connect to MongoDB")
        if client is not None:
            client.close()
        raise
    finally:
        existing_client = getattr(app.state, "mongodb_client", None)
        if existing_client is not None:
            existing_client.close()
            logger.info("🟠 MongoDB connection closed")
