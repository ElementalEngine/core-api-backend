from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database
from app.core.errors import api_error

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get("/")
async def root():
    return {"service": "civ-save-tool", "status": "ok"}


@router.get("/healthz")
async def healthz():
    return {"status": "ok"}


# Ready when MongoDB answers a ping; db_ms is that ping's round trip, which
# Mite's /ping shows apart from its own call.
@router.get("/readyz")
async def readyz(client: AsyncMongoClient = Depends(get_database)):
    started = time.perf_counter()
    try:
        await client.admin.command("ping")
        return {
            "status": "ready",
            "db_ms": round((time.perf_counter() - started) * 1000, 1),
        }
    except (
        Exception
    ) as exc:  # pragma: no cover - exercised in integration, not unit tests
        logger.warning("MongoDB not ready: %s", exc)
        raise api_error(
            code="UNAVAILABLE",
            message="Database not ready",
            status_code=503,
            retryable=True,
        ) from exc
