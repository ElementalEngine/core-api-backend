from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, status
from pymongo import AsyncMongoClient

from app.core.dependencies import get_database, require_service_token
from app.features.auth.enums import RegistrationPlatform, RegistrationSessionStatus
from app.features.auth.errors import (
    AccountLookupNotFoundError,
    AuthError,
    DiscordOAuthError,
    DiscordUserMismatchError,
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
    CompleteRegistrationSessionRequest,
    CreateRegistrationSessionRequest,
    DiscordLookupResponse,
    DiscordOAuthCallbackResult,
    FinalizeRegistrationOperationRequest,
    LinkedAccountLookupResponse,
    ManualRegistrationRequest,
    RankRoleRequest,
    RegistrationOperationResponse,
    RegistrationSessionResponse,
    RegistrationSessionStatusResponse,
    SelfServiceRegistrationRequest,
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


def _repo(db: AsyncMongoClient) -> AuthRepository:
    return AuthRepository(db)


def _internal_auth_error(code: str, message: str) -> AuthError:
    return AuthError(code=code, message=message, status_code=502, retryable=True)


@router.get(
    "/admin/accounts/discord/{discord_id}", response_model=DiscordLookupResponse
)
async def lookup_account_by_discord(
    discord_id: Annotated[str, Path(min_length=1, max_length=64)],
    db: AsyncMongoClient = Depends(get_database),
) -> DiscordLookupResponse:
    try:
        account = await RegistrationService(_repo(db)).lookup_by_discord_id(discord_id)
        if account is None:
            raise AccountLookupNotFoundError(field="discord_id", value=discord_id)
        return account
    except AuthError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception("Unexpected Discord lookup failure. discord_id=%s", discord_id)
        raise to_http_exception(
            _internal_auth_error(
                "ACCOUNT_LOOKUP_FAILED",
                "The auth service could not complete the Discord account lookup right now. Please try again.",
            )
        ) from exc


@router.get(
    "/admin/accounts/linked-account/{linked_account_id}",
    response_model=LinkedAccountLookupResponse,
)
async def lookup_account_by_linked_account(
    linked_account_id: Annotated[str, Path(min_length=1, max_length=128)],
    db: AsyncMongoClient = Depends(get_database),
) -> LinkedAccountLookupResponse:
    try:
        account = await RegistrationService(_repo(db)).lookup_by_linked_account_id(
            linked_account_id
        )
        if account is None:
            raise AccountLookupNotFoundError(
                field="linked_account_id", value=linked_account_id
            )
        return account
    except AuthError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected linked-account lookup failure. linked_account_id=%s",
            linked_account_id,
        )
        raise to_http_exception(
            _internal_auth_error(
                "ACCOUNT_LOOKUP_FAILED",
                "The auth service could not complete the linked-account lookup right now. Please try again.",
            )
        ) from exc


@router.get(
    "/admin/accounts/steam/{steam_id}", response_model=LinkedAccountLookupResponse
)
async def lookup_account_by_steam(
    steam_id: Annotated[str, Path(min_length=1, max_length=64)],
    db: AsyncMongoClient = Depends(get_database),
) -> LinkedAccountLookupResponse:
    return await lookup_account_by_linked_account(steam_id, db)


@router.post("/registration-sessions", response_model=RegistrationSessionResponse)
async def create_registration_session(
    payload: CreateRegistrationSessionRequest,
    db: AsyncMongoClient = Depends(get_database),
) -> RegistrationSessionResponse:
    try:
        return await SessionService(_repo(db)).create_registration_session(payload)
    except AuthError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected registration-session creation failure. discord_user_id=%s game=%s",
            payload.discord_user_id,
            payload.game.value,
        )
        raise to_http_exception(
            _internal_auth_error(
                "REGISTRATION_START_FAILED",
                "The auth service could not start registration right now. Please try again.",
            )
        ) from exc


@router.get(
    "/registration-sessions/{session_id}",
    response_model=RegistrationSessionStatusResponse,
)
async def get_registration_session(
    session_id: Annotated[str, Path(min_length=1, max_length=128)],
    db: AsyncMongoClient = Depends(get_database),
) -> RegistrationSessionStatusResponse:
    try:
        return await SessionService(_repo(db)).get_registration_session_status(
            session_id
        )
    except AuthError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected registration-session status failure. session_id=%s", session_id
        )
        raise to_http_exception(
            _internal_auth_error(
                "REGISTRATION_STATUS_FAILED",
                "The auth service could not load registration status right now. Please try again.",
            )
        ) from exc


@router.post(
    "/registration-sessions/{session_id}/complete",
    response_model=RegistrationOperationResponse,
)
async def complete_registration_session(
    session_id: Annotated[str, Path(min_length=1, max_length=128)],
    payload: CompleteRegistrationSessionRequest,
    db: AsyncMongoClient = Depends(get_database),
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
            platform=RegistrationPlatform.STEAM,
            account_id=steam_id,
            game=str(session["game"]),
        )
        return await registration_service.create_registration_operation(
            session=session,
            steam_validation=validation,
        )
    except AuthError as exc:
        logger.warning(
            "Complete registration failed. session_id=%s discord_user_id=%s code=%s message=%s details=%s",
            session_id,
            payload.discord_user_id,
            exc.code,
            exc.message,
            exc.details,
        )
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected complete registration failure. session_id=%s discord_user_id=%s",
            session_id,
            payload.discord_user_id,
        )
        raise to_http_exception(
            _internal_auth_error(
                "AUTH_COMPLETE_INTERNAL_ERROR",
                "The auth service could not complete registration right now. Please try again.",
            )
        ) from exc


@router.post(
    "/registration-operations/{operation_id}/finalize",
    response_model=None,
    status_code=status.HTTP_200_OK,
)
async def finalize_registration_operation(
    operation_id: Annotated[str, Path(min_length=1, max_length=128)],
    payload: FinalizeRegistrationOperationRequest,
    db: AsyncMongoClient = Depends(get_database),
) -> None:
    try:
        await OperationService(_repo(db)).finalize_operation(operation_id, payload)
    except AuthError as exc:
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected finalize registration failure. operation_id=%s", operation_id
        )
        raise to_http_exception(
            _internal_auth_error(
                "REGISTRATION_FINALIZE_FAILED",
                "The auth service could not finalize registration right now. Please try again.",
            )
        ) from exc


@router.post("/rank-role-requests", response_model=RegistrationOperationResponse)
async def create_rank_role_request(
    payload: RankRoleRequest,
    db: AsyncMongoClient = Depends(get_database),
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
    except Exception as exc:
        logger.exception(
            "Unexpected rank-role request failure. discord_user_id=%s game=%s",
            payload.discord_user_id,
            payload.game.value,
        )
        raise to_http_exception(
            _internal_auth_error(
                "RANK_ROLE_REQUEST_FAILED",
                "The auth service could not add the ranked role right now. Please try again.",
            )
        ) from exc


@router.post(
    "/admin/manual-registrations", response_model=RegistrationOperationResponse
)
async def create_manual_registration(
    payload: ManualRegistrationRequest,
    db: AsyncMongoClient = Depends(get_database),
) -> RegistrationOperationResponse:
    try:
        return await ManualRegistrationService(_repo(db)).create_manual_registration(
            payload
        )
    except AuthError as exc:
        logger.warning(
            "Manual registration failed. actor=%s subject=%s code=%s message=%s details=%s",
            payload.actor_discord_id,
            payload.subject_discord_id,
            exc.code,
            exc.message,
            exc.details,
        )
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected manual registration failure. actor=%s subject=%s platform=%s account_id=%s game=%s",
            payload.actor_discord_id,
            payload.subject_discord_id,
            payload.platform.value,
            payload.platform_account_id,
            payload.game.value,
        )
        raise to_http_exception(
            _internal_auth_error(
                "MANUAL_REGISTRATION_FAILED",
                "The auth service could not complete manual registration right now. Please try again.",
            )
        ) from exc


@router.post(
    "/manual-registration-requests", response_model=RegistrationOperationResponse
)
async def create_self_service_registration(
    payload: SelfServiceRegistrationRequest,
    db: AsyncMongoClient = Depends(get_database),
) -> RegistrationOperationResponse:
    try:
        return await ManualRegistrationService(
            _repo(db)
        ).create_self_service_registration(payload)
    except AuthError as exc:
        logger.warning(
            "Self-service registration failed. discord_user_id=%s game=%s platform=%s code=%s message=%s",
            payload.discord_user_id,
            payload.game.value,
            payload.platform.value,
            exc.code,
            exc.message,
        )
        raise to_http_exception(exc) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected self-service registration failure. discord_user_id=%s game=%s platform=%s account_id=%s",
            payload.discord_user_id,
            payload.game.value,
            payload.platform.value,
            payload.platform_account_id,
        )
        raise to_http_exception(
            _internal_auth_error(
                "SELF_SERVICE_REGISTRATION_FAILED",
                "The auth service could not complete self-service registration right now. Please try again.",
            )
        ) from exc


async def _persist_callback_failure(
    repository: AuthRepository,
    session_service: SessionService,
    *,
    state: str,
    failure_code: str,
    failure_message: str,
    base_details: object | None,
    linked_account_id: str | None,
    linked_account_name: str | None,
    oauth_username_snapshot: str | None,
    oauth_display_name_snapshot: str | None,
    oauth_locale_snapshot: str | None,
    oauth_verified_snapshot: bool | None,
    oauth_mfa_enabled_snapshot: bool | None,
    log_context: str,
) -> None:
    try:
        session = await repository.get_registration_session_by_state(state)
        if session is not None:
            extra: dict[str, object] = (
                dict(base_details) if isinstance(base_details, dict) else {}
            )
            if linked_account_id:
                extra.setdefault("validated_account_id", linked_account_id)
            if linked_account_name:
                extra.setdefault("validated_account_name", linked_account_name)
            if oauth_username_snapshot:
                extra.setdefault("oauth_username_snapshot", oauth_username_snapshot)
            if oauth_display_name_snapshot:
                extra.setdefault(
                    "oauth_display_name_snapshot", oauth_display_name_snapshot
                )
            if oauth_locale_snapshot:
                extra.setdefault("oauth_locale_snapshot", oauth_locale_snapshot)
            if oauth_verified_snapshot is not None:
                extra.setdefault("oauth_verified_snapshot", oauth_verified_snapshot)
            if oauth_mfa_enabled_snapshot is not None:
                extra.setdefault(
                    "oauth_mfa_enabled_snapshot", oauth_mfa_enabled_snapshot
                )
            await session_service.mark_failed(
                str(session["session_id"]),
                failure_code=failure_code,
                failure_message=failure_message,
                extra=extra or None,
            )
    except Exception:
        logger.exception(log_context)


@public_router.get("/oauth/discord/callback", response_model=DiscordOAuthCallbackResult)
async def discord_oauth_callback(
    code: Annotated[str | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    error: Annotated[str | None, Query()] = None,
    db: AsyncMongoClient = Depends(get_database),
) -> DiscordOAuthCallbackResult:
    repository = _repo(db)
    session_service = SessionService(repository)
    oauth_service = DiscordOAuthService()
    steam_service = SteamService()

    if error or not code or not state:
        failure_message = (
            "Discord authentication was cancelled or denied. Please start again."
            if error
            else "Discord authentication did not complete correctly. Please start again."
        )
        if state:
            try:
                session = await repository.get_registration_session_by_state(state)
                if session is not None:
                    await session_service.mark_failed(
                        str(session["session_id"]),
                        failure_code="DISCORD_OAUTH_FAILED",
                        failure_message=failure_message,
                        extra={"oauth_error": error} if error else None,
                    )
            except Exception:
                logger.exception("Failed to persist OAuth callback denial state")
        if not state:
            raise to_http_exception(InvalidStateError())
        raise to_http_exception(DiscordOAuthError(failure_message))

    session_id: str | None = None
    platform: RegistrationPlatform | None = None
    linked_account_id: str | None = None
    linked_account_name: str | None = None
    oauth_username_snapshot: str | None = None
    oauth_display_name_snapshot: str | None = None
    oauth_locale_snapshot: str | None = None
    oauth_verified_snapshot: bool | None = None
    oauth_mfa_enabled_snapshot: bool | None = None

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
        linked_account_id = str(connection.get("id") or "").strip()
        linked_account_name = str(connection.get("name") or "").strip() or None
        oauth_username_snapshot = str(user.get("username") or "").strip() or None
        oauth_display_name_snapshot = (
            str(user.get("global_name") or "").strip() or oauth_username_snapshot
        )
        oauth_locale_snapshot = str(user.get("locale") or "").strip() or None
        oauth_verified_snapshot = (
            user.get("verified") if isinstance(user.get("verified"), bool) else None
        )
        oauth_mfa_enabled_snapshot = (
            user.get("mfa_enabled")
            if isinstance(user.get("mfa_enabled"), bool)
            else None
        )

        if user_id and user_id != str(session["discord_user_id"]):
            raise DiscordUserMismatchError(
                session_user_id=str(session["discord_user_id"]),
                request_user_id=user_id,
            )

        RegistrationService.manual_required_for_platform(
            platform, account_name=linked_account_name
        )

        if platform is RegistrationPlatform.STEAM:
            await steam_service.validate_linked_account(
                steam_id=linked_account_id,
                game=str(session["game"]),
            )

        await session_service.mark_validated(
            session_id,
            linked_account_id=linked_account_id,
            linked_account_name=linked_account_name,
            oauth_username_snapshot=oauth_username_snapshot,
            oauth_display_name_snapshot=oauth_display_name_snapshot,
            oauth_locale_snapshot=oauth_locale_snapshot,
            oauth_verified_snapshot=oauth_verified_snapshot,
            oauth_mfa_enabled_snapshot=oauth_mfa_enabled_snapshot,
        )
        return DiscordOAuthCallbackResult(
            session_id=session_id,
            status=RegistrationSessionStatus.VALIDATED,
            platform=platform,
            linked_account_id=linked_account_id,
            linked_account_name=linked_account_name,
            details={
                "discord_username": oauth_username_snapshot,
                "discord_display_name": oauth_display_name_snapshot,
                "discord_locale": oauth_locale_snapshot,
                "discord_verified": oauth_verified_snapshot,
                "discord_mfa_enabled": oauth_mfa_enabled_snapshot,
            },
        )
    except SessionNotFoundError as exc:
        raise to_http_exception(exc) from exc
    except SessionExpiredError as exc:
        raise to_http_exception(exc) from exc
    except AuthError as exc:
        await _persist_callback_failure(
            repository,
            session_service,
            state=state,
            failure_code=exc.code,
            failure_message=exc.message,
            base_details=exc.details,
            linked_account_id=linked_account_id,
            linked_account_name=linked_account_name,
            oauth_username_snapshot=oauth_username_snapshot,
            oauth_display_name_snapshot=oauth_display_name_snapshot,
            oauth_locale_snapshot=oauth_locale_snapshot,
            oauth_verified_snapshot=oauth_verified_snapshot,
            oauth_mfa_enabled_snapshot=oauth_mfa_enabled_snapshot,
            log_context="Failed to persist OAuth callback auth error state",
        )
        raise to_http_exception(exc) from exc
    except Exception as exc:
        await _persist_callback_failure(
            repository,
            session_service,
            state=state,
            failure_code="AUTH_CALLBACK_INTERNAL_ERROR",
            failure_message="The auth service could not finish Discord verification. Please try again.",
            base_details=None,
            linked_account_id=linked_account_id,
            linked_account_name=linked_account_name,
            oauth_username_snapshot=oauth_username_snapshot,
            oauth_display_name_snapshot=oauth_display_name_snapshot,
            oauth_locale_snapshot=oauth_locale_snapshot,
            oauth_verified_snapshot=oauth_verified_snapshot,
            oauth_mfa_enabled_snapshot=oauth_mfa_enabled_snapshot,
            log_context="Failed to persist OAuth callback internal error state",
        )
        logger.exception(
            "Unexpected OAuth callback failure. state=%s session_id=%s platform=%s",
            state,
            session_id,
            platform.value if platform else None,
        )
        raise to_http_exception(
            _internal_auth_error(
                "AUTH_CALLBACK_INTERNAL_ERROR",
                "The auth service could not finish Discord verification. Please try again.",
            )
        ) from exc
