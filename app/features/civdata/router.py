from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, status
from pymongo import AsyncMongoClient

from app.core.constants import Edition
from app.core.dependencies import get_database, require_any_service_token
from app.core.errors import api_error
from app.features.civdata.repository import CivDataRepository
from app.features.civdata.schemas import CivDataResponse

router = APIRouter(
    prefix="/api/v2/civ-data",
    tags=["civ-data"],
    dependencies=[Depends(require_any_service_token)],
)


@router.get("/{edition}", response_model=CivDataResponse)
async def get_civ_data(
    edition: Edition,
    client: AsyncMongoClient = Depends(get_database),
) -> CivDataResponse:
    return CivDataResponse(**await seeded_civ_data(client, edition))


async def seeded_civ_data(db: AsyncMongoClient, edition: str) -> dict[str, Any]:
    """An edition's civ data, or 503 while it is not seeded."""
    civ_data = await CivDataRepository(db).fetch(edition)
    if civ_data["leader_data_version"] is None:
        raise api_error(
            code="CIV_DATA_NOT_SEEDED",
            message=f"civ_data holds no documents for {edition}.",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    return civ_data
