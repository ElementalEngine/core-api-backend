from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, HTTPException
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database

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
        raise HTTPException(status_code=503, detail=f"DB not ready: {exc!s}") from exc
