from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path, Query
from fastapi.responses import HTMLResponse
from pymongo import AsyncMongoClient

from app.core.dependencies import actor_is_staff, get_database, require_service_token
from app.core.middleware import current_correlation_id
from app.features.auth.errors import AuthError, InvalidStateError, StaffRequiredError
from app.features.auth.page import sign_in_page
from app.features.auth.registration_v2 import RegistrationFlow
from app.features.auth.schemas_v2 import (
    AccountLookup,
    ManualRegistrationBody,
    RankRoleBody,
    Registration,
    SessionComplete,
    SessionStart,
    SessionStarted,
    SessionState,
    TwoKRegistrationBody,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v2/auth",
    tags=["auth-v2"],
    dependencies=[Depends(require_service_token)],
)
# Discord sends the player's browser here, so it carries no service token.
public_router = APIRouter(tags=["auth-public"])

SessionId = Annotated[str, Path(min_length=1, max_length=64)]


def _actor(
    x_actor_discord_id: Annotated[str, Header(pattern=r"^[0-9]{17,20}$")],
) -> str:
    """The staff member registering a player, as the bot asserts it."""
    return x_actor_discord_id


def _staff_only(staff: bool = Depends(actor_is_staff)) -> None:
    """Refused before the body is read, so a non-staff caller learns nothing."""
    if not staff:
        raise StaffRequiredError()


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


@router.post(
    "/manual-registrations",
    response_model=Registration,
    dependencies=[Depends(_staff_only)],
)
async def register_manually(
    body: ManualRegistrationBody, flow: Flow, actor: str = Depends(_actor)
) -> Registration:
    return await flow.manual(actor, body)


@router.post("/2k-registrations", response_model=Registration)
async def register_two_k(body: TwoKRegistrationBody, flow: Flow) -> Registration:
    return await flow.two_k(body)


@router.get("/accounts/discord/{discord_id}", response_model=AccountLookup)
async def lookup_discord(
    discord_id: Annotated[str, Path(pattern=r"^[0-9]{17,20}$")], flow: Flow
) -> AccountLookup:
    return await flow.by_discord(discord_id)


@router.get("/accounts/linked/{linked_account_id}", response_model=AccountLookup)
async def lookup_linked(
    linked_account_id: Annotated[str, Path(min_length=1, max_length=128)], flow: Flow
) -> AccountLookup:
    return await flow.by_linked(linked_account_id)


# The bot reads the outcome from the session; the page only reports it.
@public_router.get("/oauth/discord/callback", response_class=HTMLResponse)
async def discord_oauth_callback(
    code: Annotated[str | None, Query(max_length=512)] = None,
    state: Annotated[str | None, Query(max_length=512)] = None,
    error: Annotated[str | None, Query(max_length=512)] = None,
    db: AsyncMongoClient = Depends(get_database),
) -> HTMLResponse:
    ref = current_correlation_id.get()
    try:
        if not state:
            raise InvalidStateError()
        await RegistrationFlow(db).callback(code=code, state=state, error=error)
    except AuthError as exc:
        logger.info("Sign-in refused code=%s correlation_id=%s", exc.code, ref)
        # As in the bot: a server set up wrong is not the player's to read.
        hidden = exc.code.endswith("MISCONFIGURED")
        return sign_in_page(
            exc.status_code,
            "Sign-in did not finish",
            "Something went wrong on our side. Staff can look into it."
            if hidden
            else exc.message,
            ref if exc.status_code >= 500 else None,
        )
    except Exception:
        logger.exception("Sign-in failed correlation_id=%s", ref)
        return sign_in_page(
            500,
            "Sign-in did not finish",
            "Something went wrong on our side. Please try again.",
            ref,
        )
    return sign_in_page(
        200,
        "Signed in",
        "Discord sign-in worked. Go back to Discord to see the result.",
    )
