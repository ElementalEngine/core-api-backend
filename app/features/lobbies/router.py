"""Mite's lobby routes, under `/mite`: /draft's deal."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, status
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database, require_mito_token
from app.core.errors import api_error, invalid_request
from app.features.civdata.repository import CivDataRepository
from app.features.lobbies.draft import DraftRefused, deal_draft
from app.features.lobbies.schemas import DealRequest, DealResponse

# Everything the bot may reach lives under one prefix, so "under /mite needs
# the Mito token" is true by construction rather than by a test.
mite_router = APIRouter(
    prefix="/api/v2/lobbies/mite",
    tags=["lobbies"],
    dependencies=[Depends(require_mito_token)],
)


@mite_router.post("/deal", response_model=DealResponse)
async def deal_for_draft(
    request: DealRequest, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    """One /draft deal. Nothing is stored."""
    civ_data = await CivDataRepository(db).fetch(request.edition)
    if civ_data["leader_data_version"] is None:
        raise api_error(
            code="CIV_DATA_NOT_SEEDED",
            message=f"civ_data holds no documents for {request.edition}.",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        return deal_draft(civ_data, request)
    except DraftRefused as exc:
        raise invalid_request(str(exc)) from exc


__all__ = ["mite_router"]
