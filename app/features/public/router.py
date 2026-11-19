"""The CPL website's read routes, behind its own token. They read and never
write, and name players without their Discord ids."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pymongo import AsyncMongoClient

from app.core.constants import Edition
from app.core.dependencies import get_database, require_website_token
from app.features.public.schemas import Ladders, Overview
from app.features.public.service import ladders, overview

router = APIRouter(
    prefix="/api/v2/public",
    tags=["public-v2"],
    dependencies=[Depends(require_website_token)],
)


@router.get("/overview", response_model=Overview)
async def get_overview(db: AsyncMongoClient = Depends(get_database)) -> Overview:
    return await overview(db)


@router.get("/ladders/{edition}", response_model=Ladders)
async def get_ladders(
    edition: Edition, db: AsyncMongoClient = Depends(get_database)
) -> Ladders:
    return await ladders(db, edition)


__all__ = ["router"]
