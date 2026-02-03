import logging

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.responses import JSONResponse

from app.config import settings
from app.db import db_lifespan
from app.dependencies import get_database
from app.routes import router

logger = logging.getLogger(__name__)

app = FastAPI(title="Civ Save Tool", lifespan=db_lifespan)
app.include_router(router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.allowed_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"service": "civ-save-tool", "status": "ok"}

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/readyz")
async def readyz(client: AsyncIOMotorClient = Depends(get_database)):
    try:
        await client.admin.command("ping")
        return {"status": "ready"}
    except Exception as e:
        logger.warning("MongoDB not ready: %s", e)
        raise HTTPException(status_code=503, detail=f"DB not ready: {e!s}")


@app.get("/_debug/db-stats")
async def db_stats(client: AsyncIOMotorClient = Depends(get_database)):
    try:
        stats = await client[settings.mongo_db_name].command("dbstats", scale=1)
        return JSONResponse(stats)
    except Exception as e:
        logger.warning("MongoDB not ready for dbstats: %s", e)
        raise HTTPException(status_code=503, detail=f"DB not ready: {e!s}")