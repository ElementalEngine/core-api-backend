"""Mite's match setup routes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, status
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database, require_mito_token
from app.core.errors import api_error, invalid_request
from app.features.civdata.repository import CivDataRepository
from app.features.match_setups.deal import DraftRefused, deal_draft
from app.features.match_setups.record import draft_setup
from app.features.match_setups.repository import MatchSetupRepository
from app.features.match_setups.schemas import DraftDeal, DraftRequest
from app.features.seasons.repository import SeasonNotSeededError, SeasonsRepository

router = APIRouter(
    prefix="/api/v2/match-setups",
    tags=["match-setups"],
    dependencies=[Depends(require_mito_token)],
)


async def _open_season_id(db: AsyncMongoClient, edition: str) -> Any:
    """The season a game counts towards; none before seasons are seeded."""
    try:
        return (await SeasonsRepository(db).get_current_season(edition))["_id"]
    except SeasonNotSeededError:
        return None


@router.post("/draft", response_model=DraftDeal)
async def deal_draft_and_record(
    request: DraftRequest, db: AsyncMongoClient = Depends(get_database)
) -> dict[str, Any]:
    """/draft: deal the pools, then record the setup."""
    civ_data = await CivDataRepository(db).fetch(request.edition)
    if civ_data["leader_data_version"] is None:
        raise api_error(
            code="CIV_DATA_NOT_SEEDED",
            message=f"civ_data holds no documents for {request.edition}.",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        deal = deal_draft(civ_data, request)
    except DraftRefused as exc:
        raise invalid_request(str(exc)) from exc
    season_id = await _open_season_id(db, request.edition)
    now = datetime.now(UTC)
    await MatchSetupRepository(db).insert(draft_setup(request, deal, season_id, now))
    return deal


__all__ = ["router"]
