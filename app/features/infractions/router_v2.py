from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path, status
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database, require_lj_token
from app.features.infractions.models import FlatType, TierCategory
from app.features.infractions.schemas_v2 import (
    DaysBody,
    DaysResult,
    DueSuspensions,
    FlatResult,
    InfractionBody,
    Laggers,
    PendingApplied,
    PendingBody,
    ReasonBody,
    RemoveTierBody,
    RolesBody,
    Suspended,
    SuspensionCleared,
    SuspensionRecord,
    TierRemoved,
    TierResult,
)
from app.features.infractions.service_v2 import Infractions

router = APIRouter(
    prefix="/api/v2/infractions",
    tags=["infractions-v2"],
    dependencies=[Depends(require_lj_token)],
)

MemberId = Annotated[str, Path(pattern=r"^[0-9]{17,20}$")]


def _actor(
    x_actor_discord_id: Annotated[str, Header(pattern=r"^[0-9]{17,20}$")],
) -> str:
    """The staff member acting, as the bot asserts it."""
    return x_actor_discord_id


def _service(db: AsyncMongoClient = Depends(get_database)) -> Infractions:
    return Infractions(db)


Service = Annotated[Infractions, Depends(_service)]
Actor = Annotated[str, Depends(_actor)]


@router.get("/due", response_model=DueSuspensions)
async def due(service: Service) -> DueSuspensions:
    return await service.due()


@router.get("/laggers", response_model=Laggers)
async def laggers(service: Service) -> Laggers:
    return await service.laggers()


@router.get("/suspended", response_model=Suspended)
async def suspended(service: Service) -> Suspended:
    return await service.suspended()


@router.get("/{discord_id}", response_model=SuspensionRecord)
async def record(discord_id: MemberId, service: Service) -> SuspensionRecord:
    return await service.record(discord_id)


@router.post("/{discord_id}/tier/{category}", response_model=TierResult)
async def record_tier(
    discord_id: MemberId,
    category: TierCategory,
    body: InfractionBody,
    service: Service,
    actor: Actor,
) -> TierResult:
    return await service.tier(discord_id, category, body, actor)


@router.post("/{discord_id}/flat/{flat_type}", response_model=FlatResult)
async def record_flat(
    discord_id: MemberId,
    flat_type: FlatType,
    body: InfractionBody,
    service: Service,
    actor: Actor,
) -> FlatResult:
    return await service.flat(discord_id, flat_type, body, actor)


@router.post("/{discord_id}/days", response_model=DaysResult)
async def change_days(
    discord_id: MemberId, body: DaysBody, service: Service, actor: Actor
) -> DaysResult:
    return await service.days(discord_id, body.days, actor)


@router.post("/{discord_id}/remove-tier", response_model=TierRemoved)
async def remove_tier(
    discord_id: MemberId, body: RemoveTierBody, service: Service, actor: Actor
) -> TierRemoved:
    return await service.remove_tier(discord_id, body.category, actor)


@router.post("/{discord_id}/unsuspend", response_model=SuspensionCleared)
async def unsuspend(
    discord_id: MemberId,
    service: Service,
    actor: Actor,
    body: ReasonBody = ReasonBody(),
) -> SuspensionCleared:
    return await service.unsuspend(discord_id, body.reason, actor)


@router.post("/{discord_id}/expire", response_model=SuspensionCleared)
async def expire(discord_id: MemberId, service: Service) -> SuspensionCleared:
    return await service.expire(discord_id)


@router.put("/{discord_id}/pending", status_code=status.HTTP_204_NO_CONTENT)
async def set_pending(
    discord_id: MemberId, body: PendingBody, service: Service, actor: Actor
) -> None:
    await service.set_pending(discord_id, body.punishment_type, body.reason, actor)


@router.post("/{discord_id}/pending/apply", response_model=PendingApplied)
async def apply_pending(
    discord_id: MemberId, service: Service, body: RolesBody = RolesBody()
) -> PendingApplied:
    return await service.apply_pending(discord_id, body.suspended_roles)
