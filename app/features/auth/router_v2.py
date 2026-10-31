from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database, require_service_token
from app.features.auth.registration_v2 import RegistrationFlow
from app.features.auth.schemas import DiscordLookupResponse, LinkedAccountLookupResponse
from app.features.auth.schemas_v2 import (
    ManualRegistrationBody,
    RankRoleBody,
    Registration,
    SessionComplete,
    SessionStart,
    SessionStarted,
    SessionState,
    TwoKRegistrationBody,
)

router = APIRouter(
    prefix="/api/v2/auth",
    tags=["auth-v2"],
    dependencies=[Depends(require_service_token)],
)

SessionId = Annotated[str, Path(min_length=1, max_length=64)]


def _actor(
    x_actor_discord_id: Annotated[str, Header(pattern=r"^[0-9]{17,20}$")],
) -> str:
    """The staff member registering a player, as the bot asserts it."""
    return x_actor_discord_id


def _flow(db: AsyncMongoClient = Depends(get_database)) -> RegistrationFlow:
    return RegistrationFlow(db)


Flow = Annotated[RegistrationFlow, Depends(_flow)]


@router.post("/sessions", response_model=SessionStarted)
async def start_session(body: SessionStart, flow: Flow) -> SessionStarted:
    return await flow.start(body.discord_user_id, body.game)


@router.get("/sessions/{session_id}", response_model=SessionState)
async def get_session(session_id: SessionId, flow: Flow) -> SessionState:
    return await flow.state(session_id)


@router.post("/sessions/{session_id}/complete", response_model=Registration)
async def complete_session(
    session_id: SessionId, body: SessionComplete, flow: Flow
) -> Registration:
    return await flow.complete(session_id, body.discord_user_id)


@router.post("/rank-roles", response_model=Registration)
async def add_rank_role(body: RankRoleBody, flow: Flow) -> Registration:
    return await flow.rank_role(body.discord_user_id, body.game)


@router.post("/manual-registrations", response_model=Registration)
async def register_manually(
    body: ManualRegistrationBody,
    flow: Flow,
    actor: str = Depends(_actor),
) -> Registration:
    return await flow.manual(actor, body)


@router.post("/2k-registrations", response_model=Registration)
async def register_two_k(body: TwoKRegistrationBody, flow: Flow) -> Registration:
    return await flow.two_k(body)


@router.get("/accounts/discord/{discord_id}", response_model=DiscordLookupResponse)
async def lookup_discord(
    discord_id: Annotated[str, Path(pattern=r"^[0-9]{17,20}$")], flow: Flow
) -> DiscordLookupResponse:
    return await flow.by_discord(discord_id)


@router.get(
    "/accounts/linked/{linked_account_id}", response_model=LinkedAccountLookupResponse
)
async def lookup_linked(
    linked_account_id: Annotated[str, Path(min_length=1, max_length=128)], flow: Flow
) -> LinkedAccountLookupResponse:
    return await flow.by_linked(linked_account_id)
