from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, status
from motor.motor_asyncio import AsyncIOMotorClient

from app.core.dependencies import get_database, require_service_token
from app.features.auth.enums import RegistrationPlatform, RegistrationSessionStatus
from app.features.auth.errors import (
    AccountLookupNotFoundError,
    AuthError,
    InvalidStateError,
    SessionExpiredError,
    SessionNotFoundError,
    to_http_exception,
)
from app.features.auth.manual_registration_service import ManualRegistrationService
from app.features.auth.oauth_service import DiscordOAuthService
from app.features.auth.operation_service import OperationService
from app.features.auth.registration_service import RegistrationService
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import (
    AccountLookupResponse,
    CompleteRegistrationSessionRequest,
    CreateRegistrationSessionRequest,
    DiscordOAuthCallbackResult,
    FinalizeRegistrationOperationRequest,
    ManualRegistrationRequest,
    RankRoleRequest,
    RegistrationOperationResponse,
    RegistrationSessionResponse,
    RegistrationSessionStatusResponse,
)
from app.features.auth.session_service import SessionService
from app.features.auth.steam_service import SteamService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/auth",
    tags=["auth"],
    dependencies=[Depends(require_service_token)],
)
public_router = APIRouter(tags=["auth-public"])


def _repo(db: AsyncIOMotorClient) -> AuthRepository:
    return AuthRepository(db)


@router.get("/admin/accounts/discord/{discord_id}", response_model=AccountLookupResponse)
async def lookup_account_by_discord(
    discord_id: Annotated[str, Path(min_length=1, max_length=64)],
    db: AsyncIOMotorClient = Depends(get_database),
) -> AccountLookupResponse:
    account = await RegistrationService(_repo(db)).lookup_by_discord_id(discord_id)
    if account is None:
        raise to_http_exception(AccountLookupNotFoundError(field="discord_id", value=discord_id))
    return account


@router.get("/admin/accounts/steam/{steam_id}", response_model=AccountLookupResponse)
async def lookup_account_by_steam(
    steam_id: Annotated[str, Path(min_length=1, max_length=64)],
    db: AsyncIOMotorClient = Depends(get_database),
) -> AccountLookupResponse:
    account = await RegistrationService(_repo(db)).lookup_by_steam_id(steam_id)
    if account is None:
        raise to_http_exception(AccountLookupNotFoundError(field="steam_id", value=steam_id))
    return account


@router.post("/registration-sessions", response_model=RegistrationSessionResponse)
async def create_registration_session(
    payload: CreateRegistrationSessionRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> RegistrationSessionResponse:
    try:
        return await SessionService(_repo(db)).create_registration_session(payload)
    except AuthError as exc:
        raise to_http_exception(exc) from exc


@router.get("/registration-sessions/{session_id}", response_model=RegistrationSessionStatusResponse)
async def get_registration_session(
    session_id: Annotated[str, Path(min_length=1, max_length=128)],
    db: AsyncIOMotorClient = Depends(get_database),
) -> RegistrationSessionStatusResponse:
    try:
        return await SessionService(_repo(db)).get_registration_session_status(session_id)
    except AuthError as exc:
        raise to_http_exception(exc) from exc


@router.post(
    "/registration-sessions/{session_id}/complete",
    response_model=RegistrationOperationResponse,
)
async def complete_registration_session(
    session_id: Annotated[str, Path(min_length=1, max_length=128)],
    payload: CompleteRegistrationSessionRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> RegistrationOperationResponse:
    repository = _repo(db)
    session_service = SessionService(repository)
    registration_service = RegistrationService(repository)
    steam_service = SteamService()
    try:
        session = await session_service.load_session_for_completion(
            session_id=session_id,
            discord_user_id=payload.discord_user_id,
        )
        steam_id = str(session.get("validated_account_id", ""))
        validation = await steam_service.validate_linked_account(
            steam_id=steam_id,
            game=str(session["game"]),
        )
        await registration_service.assert_registration_conflicts(
            discord_user_id=payload.discord_user_id,
            steam_id=steam_id,
            game=str(session["game"]),
        )
        return await registration_service.create_registration_operation(
            session=session,
            steam_validation=validation,
        )
    except AuthError as exc:
        raise to_http_exception(exc) from exc


@router.post(
    "/registration-operations/{operation_id}/finalize",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def finalize_registration_operation(
    operation_id: Annotated[str, Path(min_length=1, max_length=128)],
    payload: FinalizeRegistrationOperationRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> None:
    try:
        await OperationService(_repo(db)).finalize_operation(operation_id, payload)
    except AuthError as exc:
        raise to_http_exception(exc) from exc


@router.post("/rank-role-requests", response_model=RegistrationOperationResponse)
async def create_rank_role_request(
    payload: RankRoleRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> RegistrationOperationResponse:
    repository = _repo(db)
    registration_service = RegistrationService(repository)
    steam_service = SteamService()
    try:
        steam_id = await registration_service.get_registered_steam_id(
            payload.discord_user_id,
            payload.game.value,
        )
        validation = await steam_service.validate_linked_account(
            steam_id=steam_id,
            game=payload.game.value,
        )
        return await registration_service.create_rank_role_operation(
            discord_user_id=payload.discord_user_id,
            game=payload.game,
            steam_validation=validation,
        )
    except AuthError as exc:
        raise to_http_exception(exc) from exc


@router.post("/admin/manual-registrations", response_model=RegistrationOperationResponse)
async def create_manual_registration(
    payload: ManualRegistrationRequest,
    db: AsyncIOMotorClient = Depends(get_database),
) -> RegistrationOperationResponse:
    try:
        return await ManualRegistrationService(_repo(db), SteamService()).create_manual_registration(payload)
    except AuthError as exc:
        raise to_http_exception(exc) from exc


@public_router.get("/oauth/discord/callback", response_model=DiscordOAuthCallbackResult)
async def discord_oauth_callback(
    code: Annotated[str | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    error: Annotated[str | None, Query()] = None,
    db: AsyncIOMotorClient = Depends(get_database),
) -> DiscordOAuthCallbackResult:
    repository = _repo(db)
    session_service = SessionService(repository)
    registration_service = RegistrationService(repository)
    oauth_service = DiscordOAuthService()

    if error or not code or not state:
        if state:
            try:
                session = await repository.get_registration_session_by_state(state)
                if session is not None:
                    await session_service.mark_failed(
                        str(session["session_id"]),
                        failure_code="DISCORD_OAUTH_FAILED",
                        failure_message="Discord authentication was cancelled or denied. Please start again.",
                        extra={"oauth_error": error} if error else None,
                    )
            except Exception:
                logger.exception("Failed to persist OAuth callback denial state")
        raise to_http_exception(InvalidStateError())

    try:
        session = await session_service.load_session_by_state(state)
        session_id = str(session["session_id"])
        platform = RegistrationPlatform(str(session["platform"]))

        await session_service.mark_validating(session_id)
        user, connection = await oauth_service.fetch_identity_and_connection(
            code=code,
            platform=platform,
        )
        user_id = str(user.get("id", ""))
        if user_id and user_id != str(session["discord_user_id"]):
            await session_service.mark_failed(
                session_id,
                failure_code="DISCORD_USER_MISMATCH",
                failure_message="Authenticated Discord account did not match the registration session.",
                extra={"oauth_discord_user_id": user_id},
            )
            raise to_http_exception(InvalidStateError())

        linked_account_id = str(connection.get("id", ""))
        linked_account_name = connection.get("name") if isinstance(connection.get("name"), str) else None

        if platform is RegistrationPlatform.STEAM:
            await session_service.mark_validated(
                session_id,
                linked_account_id=linked_account_id,
                linked_account_name=linked_account_name,
                oauth_username_snapshot=user.get("username") if isinstance(user.get("username"), str) else None,
                oauth_display_name_snapshot=user.get("global_name") if isinstance(user.get("global_name"), str) else None,
            )
            await repository.append_audit_event(
                {
                    "action": "registration_session_validated",
                    "session_id": session_id,
                    "discord_user_id": str(session["discord_user_id"]),
                    "platform": platform.value,
                    "linked_account_id": linked_account_id,
                    "linked_account_name": linked_account_name,
                    "game": str(session["game"]),
                }
            )
            return DiscordOAuthCallbackResult(
                session_id=session_id,
                status=RegistrationSessionStatus.VALIDATED,
                platform=platform,
                linked_account_id=linked_account_id,
                linked_account_name=linked_account_name,
            )

        registration_service.manual_required_for_platform(platform, account_name=linked_account_name)
        raise AssertionError("manual_required_for_platform should have raised")
    except (SessionNotFoundError, SessionExpiredError, AuthError) as exc:
        if isinstance(exc, AuthError) and not isinstance(exc, (SessionNotFoundError, SessionExpiredError)):
            try:
                session = await repository.get_registration_session_by_state(state)
                if session is not None:
                    await session_service.mark_failed(
                        str(session["session_id"]),
                        failure_code=exc.code,
                        failure_message=exc.message,
                        extra={"details": exc.details} if exc.details else None,
                    )
            except Exception:
                logger.exception("Failed to persist auth callback failure state")
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("Unexpected auth callback failure")
        try:
            session = await repository.get_registration_session_by_state(state)
            if session is not None:
                await session_service.mark_failed(
                    str(session["session_id"]),
                    failure_code="DISCORD_LINKED_ACCOUNT_FETCH_FAILED",
                    failure_message="We could not read your Discord linked accounts. Please try again.",
                )
        except Exception:
            logger.exception("Failed to persist unexpected callback failure")
        raise to_http_exception(
            AuthError(
                code="DISCORD_LINKED_ACCOUNT_FETCH_FAILED",
                message="We could not read your Discord linked accounts. Please try again.",
                status_code=502,
                retryable=True,
            )
        ) from exc
