from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, status
from motor.motor_asyncio import AsyncIOMotorClient

from app.core.dependencies import get_database, require_lj_token
from app.features.infractions import service as svc
from app.features.infractions.errors import InfractionError, to_http_exception
from app.features.infractions.models import (
    ActiveSuspension,
    CreatePendingSuspensionRequest,
    FlatSuspensionResponse,
    FlatType,
    ModifyDaysRequest,
    ModifyDaysResponse,
    PendingSuspensionResponse,
    RecordFlatSuspensionRequest,
    RecordTierInfractionRequest,
    RemoveTierRequest,
    RemoveTierResponse,
    SuspensionRecordResponse,
    TierCategory,
    TierInfractionResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/infractions",
    tags=["infractions"],
    dependencies=[Depends(require_lj_token)],
)

_DiscordId = Annotated[str, Path(min_length=1, max_length=64)]


def _internal_error(code: str, message: str) -> InfractionError:
    return InfractionError(code=code, message=message, status_code=500, retryable=True)


# GET /active must be declared BEFORE /{discord_id} to prevent path collision.

@router.get("/active", response_model=list[ActiveSuspension])
async def get_active_suspensions(
    db: AsyncIOMotorClient = Depends(get_database),
) -> list[ActiveSuspension]:
    try:
        return await svc.get_active_suspensions(db)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("get_active_suspensions failed")
        raise to_http_exception(_internal_error("GET_ACTIVE_FAILED", "Could not retrieve active suspensions.")) from exc

# GET /overdue must also be declared BEFORE /{discord_id} to prevent path collision.

@router.get("/overdue", response_model=list[ActiveSuspension])
async def get_overdue_suspensions(
    db: AsyncIOMotorClient = Depends(get_database),
) -> list[ActiveSuspension]:
    try:
        return await svc.get_overdue_suspensions(db)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("get_overdue_suspensions failed")
        raise to_http_exception(_internal_error("GET_OVERDUE_FAILED", "Could not retrieve overdue suspensions.")) from exc


@router.get("/{discord_id}", response_model=SuspensionRecordResponse)
async def get_record(
    discord_id: _DiscordId,
    db: AsyncIOMotorClient = Depends(get_database),
) -> SuspensionRecordResponse:
    try:
        return await svc.get_record(db, discord_id)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("get_record failed. discord_id=%s", discord_id)
        raise to_http_exception(_internal_error("GET_RECORD_FAILED", "Could not retrieve suspension record.")) from exc


@router.post("/{discord_id}/tier/{category}", response_model=TierInfractionResponse)
async def record_tier_infraction(
    discord_id: _DiscordId,
    category: TierCategory,
    payload: RecordTierInfractionRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> TierInfractionResponse:
    try:
        return await svc.record_tier_infraction(db, discord_id, category, payload.reason, payload.suspended_roles)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("record_tier_infraction failed. discord_id=%s category=%s", discord_id, category)
        raise to_http_exception(_internal_error("TIER_INFRACTION_FAILED", "Could not record tier infraction.")) from exc


@router.post("/{discord_id}/flat/{flat_type}", response_model=FlatSuspensionResponse)
async def record_flat_suspension(
    discord_id: _DiscordId,
    flat_type: FlatType,
    payload: RecordFlatSuspensionRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> FlatSuspensionResponse:
    try:
        return await svc.record_flat_suspension(db, discord_id, flat_type, payload.reason, payload.suspended_roles)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("record_flat_suspension failed. discord_id=%s flat_type=%s", discord_id, flat_type)
        raise to_http_exception(_internal_error("FLAT_SUSPENSION_FAILED", "Could not record flat suspension.")) from exc


@router.post("/{discord_id}/add-days", response_model=ModifyDaysResponse)
async def add_days(
    discord_id: _DiscordId,
    payload: ModifyDaysRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> ModifyDaysResponse:
    try:
        return await svc.add_days(db, discord_id, payload.days)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("add_days failed. discord_id=%s", discord_id)
        raise to_http_exception(_internal_error("ADD_DAYS_FAILED", "Could not add days to suspension.")) from exc


@router.post("/{discord_id}/remove-days", response_model=ModifyDaysResponse)
async def remove_days(
    discord_id: _DiscordId,
    payload: ModifyDaysRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> ModifyDaysResponse:
    try:
        return await svc.remove_days(db, discord_id, payload.days)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("remove_days failed. discord_id=%s", discord_id)
        raise to_http_exception(_internal_error("REMOVE_DAYS_FAILED", "Could not remove days from suspension.")) from exc


@router.post("/{discord_id}/remove-tier", response_model=RemoveTierResponse)
async def remove_tier(
    discord_id: _DiscordId,
    payload: RemoveTierRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> RemoveTierResponse:
    try:
        return await svc.remove_tier(db, discord_id, payload.category)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("remove_tier failed. discord_id=%s category=%s", discord_id, payload.category)
        raise to_http_exception(_internal_error("REMOVE_TIER_FAILED", "Could not remove tier.")) from exc


@router.post(
    "/{discord_id}/unsuspend",
    response_model=None,
    status_code=status.HTTP_204_NO_CONTENT,
)
async def unsuspend(
    discord_id: _DiscordId,
    db: AsyncIOMotorClient = Depends(get_database),
) -> None:
    try:
        await svc.unsuspend(db, discord_id)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("unsuspend failed. discord_id=%s", discord_id)
        raise to_http_exception(_internal_error("UNSUSPEND_FAILED", "Could not clear suspension.")) from exc


@router.get("/{discord_id}/pending", response_model=PendingSuspensionResponse | None)
async def get_pending_suspension(
    discord_id: _DiscordId,
    db: AsyncIOMotorClient = Depends(get_database),
) -> PendingSuspensionResponse | None:
    try:
        return await svc.get_pending_suspension(db, discord_id)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("get_pending_suspension failed. discord_id=%s", discord_id)
        raise to_http_exception(_internal_error("GET_PENDING_FAILED", "Could not retrieve pending suspension.")) from exc


@router.post(
    "/{discord_id}/pending",
    response_model=None,
    status_code=status.HTTP_201_CREATED,
)
async def create_pending_suspension(
    discord_id: _DiscordId,
    payload: CreatePendingSuspensionRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> None:
    try:
        await svc.create_pending_suspension(db, discord_id, payload.punishment_type, payload.reason)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("create_pending_suspension failed. discord_id=%s", discord_id)
        raise to_http_exception(_internal_error("CREATE_PENDING_FAILED", "Could not create pending suspension.")) from exc


@router.delete(
    "/{discord_id}/pending",
    response_model=None,
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_pending_suspension(
    discord_id: _DiscordId,
    db: AsyncIOMotorClient = Depends(get_database),
) -> None:
    try:
        await svc.delete_pending_suspension(db, discord_id)
    except InfractionError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("delete_pending_suspension failed. discord_id=%s", discord_id)
        raise to_http_exception(_internal_error("DELETE_PENDING_FAILED", "Could not delete pending suspension.")) from exc
