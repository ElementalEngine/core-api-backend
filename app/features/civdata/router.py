from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database, require_any_service_token
from app.core.errors import ErrorDetail, ErrorResponse
from app.features.civdata.repository import CivDataRepository
from app.features.civdata.schemas import CivDataResponse

router = APIRouter(
    prefix="/api/v2/civ-data",
    tags=["civ-data"],
    dependencies=[Depends(require_any_service_token)],
)


@router.get("/{edition}", response_model=CivDataResponse)
async def get_civ_data(
    edition: Literal["civ6", "civ7"],
    client: AsyncMongoClient = Depends(get_database),
) -> CivDataResponse:
    payload = await CivDataRepository(client).fetch(edition)
    if payload["leader_data_version"] is None:
        # Empty means unseeded, not "this edition has no data". A client that
        # cached an empty table would draft from nothing (D49).
        detail = ErrorResponse(
            error=ErrorDetail(
                code="CIV_DATA_NOT_SEEDED",
                message=f"civ_data holds no documents for {edition}.",
                retryable=False,
            )
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail.model_dump(),
        )
    return CivDataResponse(**payload)
