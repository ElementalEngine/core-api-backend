from __future__ import annotations

from datetime import datetime, UTC
from secrets import token_urlsafe
from typing import Any

from app.features.auth.enums import (
    STEAM_API_REGISTRATION_METHODS,
    RegistrationMethod,
    RegistrationOperationStatus,
    RegistrationPlatform,
    RoleIntent,
    SupportedGame,
)
from app.features.auth.errors import (
    AlreadyRegisteredError,
    DiscordSteamConflictError,
    LinkedAccountConflictError,
    ManualRegistrationRequiredError,
    RankRoleEligibilityError,
    SteamIdConflictError,
)
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import (
    DiscordAccountLookupHit,
    DiscordLookupResponse,
    LinkedAccountLookupHit,
    LinkedAccountLookupResponse,
    RegistrationOperationResponse,
    RegistrationSummary,
)


class RegistrationService:
    def __init__(self, repository: AuthRepository) -> None:
        self._repository = repository

    async def lookup_by_discord_id(
        self, discord_id: str
    ) -> DiscordLookupResponse | None:
        docs = await self._repository.find_users_by_discord_id(discord_id)
        return _to_discord_lookup_response(docs) if docs else None

    async def lookup_by_linked_account_id(
        self, linked_account_id: str
    ) -> LinkedAccountLookupResponse | None:
        docs = await self._repository.find_users_by_linked_account_id(linked_account_id)
        return (
            _to_linked_account_lookup_response(docs, linked_account_id)
            if docs
            else None
        )

    async def assert_registration_conflicts(
        self,
        *,
        discord_user_id: str,
        platform: RegistrationPlatform,
        account_id: str,
        game: str,
    ) -> None:
        existing_by_discord = await self._repository.get_user_by_discord_id(
            discord_user_id
        )
        existing_by_linked = (
            await self._repository.get_user_by_steam_id(account_id)
            if platform is RegistrationPlatform.STEAM
            else await self._repository.get_user_by_linked_account(
                platform.value, account_id
            )
        )

        if (
            existing_by_linked
            and str(existing_by_linked.get("discord_id")) != discord_user_id
        ):
            if platform is RegistrationPlatform.STEAM:
                raise SteamIdConflictError(
                    steam_id=account_id,
                    existing_discord_id=str(existing_by_linked.get("discord_id", "")),
                )
            raise LinkedAccountConflictError(
                platform=platform.value,
                account_id=account_id,
                existing_discord_id=str(existing_by_linked.get("discord_id", "")),
            )

        if existing_by_discord:
            existing_platform = existing_by_discord.get("linked_platform")
            existing_account = existing_by_discord.get("linked_account_id")
            existing_steam = existing_by_discord.get("steam_id")
            if (
                platform is RegistrationPlatform.STEAM
                and existing_steam
                and str(existing_steam) != account_id
            ):
                raise DiscordSteamConflictError(
                    discord_user_id=discord_user_id,
                    existing_steam_id=str(existing_steam),
                )
            if (
                existing_platform
                and existing_account
                and (
                    str(existing_platform) != platform.value
                    or str(existing_account) != account_id
                )
            ):
                if str(existing_platform) == RegistrationPlatform.STEAM.value:
                    raise DiscordSteamConflictError(
                        discord_user_id=discord_user_id,
                        existing_steam_id=str(existing_account),
                    )
                raise LinkedAccountConflictError(
                    platform=str(existing_platform),
                    account_id=str(existing_account),
                    existing_discord_id=discord_user_id,
                )
            regs = existing_by_discord.get("registrations") or {}
            if game in regs:
                raise AlreadyRegisteredError(game)

    async def get_registered_steam_id(self, discord_user_id: str, game: str) -> str:
        user = await self._repository.get_user_by_discord_id(discord_user_id)
        if not user:
            raise RankRoleEligibilityError(discord_user_id)

        steam_id = user.get("steam_id")
        if (
            not steam_id
            and user.get("linked_platform") == RegistrationPlatform.STEAM.value
        ):
            steam_id = user.get("linked_account_id")

        if not steam_id:
            raise RankRoleEligibilityError(discord_user_id)

        # Rank roles require an account whose Steam ownership is API-verifiable. Only
        # OAuth-Steam registrations (and the legacy "oauth" value) qualify. Steam Family
        # Share and any admin-attested registration are explicitly ineligible even though
        # they may carry a steam linked account.
        regs = user.get("registrations") or {}
        if not _has_steam_api_registration(regs):
            raise RankRoleEligibilityError(discord_user_id)
        if game in regs:
            raise AlreadyRegisteredError(game)
        return str(steam_id)

    @staticmethod
    def manual_required_for_platform(
        platform: RegistrationPlatform,
        *,
        account_name: str | None = None,
    ) -> None:
        # The OAuth/session flow can only validate Steam. Any other platform must be
        # registered through a manual/self-service path.
        if platform is not RegistrationPlatform.STEAM:
            raise ManualRegistrationRequiredError(
                platform.value, account_name=account_name
            )

    async def create_registration_operation(
        self,
        *,
        session: dict[str, Any],
        steam_validation: dict[str, Any],
    ) -> RegistrationOperationResponse:
        game = SupportedGame(str(session["game"]))
        return await self._create_operation(
            operation_type="registration",
            discord_user_id=str(session["discord_user_id"]),
            linked_platform=RegistrationPlatform.STEAM,
            linked_account_id=str(steam_validation["steam_id"]),
            linked_account_name=(
                str(session["validated_account_name"])
                if session.get("validated_account_name")
                else None
            ),
            steam_id=str(steam_validation["steam_id"]),
            steam_name=(
                str(session["validated_account_name"])
                if session.get("validated_account_name")
                else None
            ),
            game=game,
            registration_method=RegistrationMethod.OAUTH_STEAM_API,
            role_intents=_build_registration_role_intents(
                game, RegistrationPlatform.STEAM
            ),
            source_session_id=str(session["session_id"]),
            username_snapshot=(
                str(session["oauth_username_snapshot"])
                if session.get("oauth_username_snapshot")
                else None
            ),
            display_name_snapshot=(
                str(session["oauth_display_name_snapshot"])
                if session.get("oauth_display_name_snapshot")
                else None
            ),
            locale_snapshot=(
                str(session["oauth_locale_snapshot"])
                if session.get("oauth_locale_snapshot")
                else None
            ),
            verified_snapshot=(
                bool(session["oauth_verified_snapshot"])
                if isinstance(session.get("oauth_verified_snapshot"), bool)
                else None
            ),
            mfa_enabled_snapshot=(
                bool(session["oauth_mfa_enabled_snapshot"])
                if isinstance(session.get("oauth_mfa_enabled_snapshot"), bool)
                else None
            ),
            ownership_verified_at=steam_validation.get("ownership_verified_at"),
            playtime_minutes=steam_validation.get("playtime_minutes"),
        )

    async def create_rank_role_operation(
        self,
        *,
        discord_user_id: str,
        game: SupportedGame,
        steam_validation: dict[str, Any],
    ) -> RegistrationOperationResponse:
        return await self._create_operation(
            operation_type="rank_role",
            discord_user_id=discord_user_id,
            linked_platform=RegistrationPlatform.STEAM,
            linked_account_id=str(steam_validation["steam_id"]),
            linked_account_name=None,
            steam_id=str(steam_validation["steam_id"]),
            steam_name=None,
            game=game,
            registration_method=RegistrationMethod.OAUTH_STEAM_API,
            role_intents=[_rank_role_for_game(game)],
            ownership_verified_at=steam_validation.get("ownership_verified_at"),
            playtime_minutes=steam_validation.get("playtime_minutes"),
        )

    async def create_manual_registration_operation(
        self,
        *,
        actor_discord_id: str,
        subject_discord_id: str,
        game: SupportedGame,
        platform: RegistrationPlatform,
        method: RegistrationMethod,
        account_id: str,
        account_name: str | None,
        reason: str | None,
        username_snapshot: str | None = None,
        display_name_snapshot: str | None = None,
    ) -> RegistrationOperationResponse:
        return await self._create_operation(
            operation_type="manual_registration",
            discord_user_id=subject_discord_id,
            linked_platform=platform,
            linked_account_id=account_id,
            linked_account_name=account_name,
            steam_id=account_id if platform is RegistrationPlatform.STEAM else None,
            steam_name=account_name if platform is RegistrationPlatform.STEAM else None,
            game=game,
            registration_method=method,
            role_intents=_build_registration_role_intents(game, platform),
            ownership_verified_at=None,
            playtime_minutes=None,
            username_snapshot=username_snapshot,
            display_name_snapshot=display_name_snapshot,
            extra_operation_fields={
                "actor_discord_id": actor_discord_id,
                **({"manual_reason": reason} if reason else {}),
            },
        )

    async def create_self_service_registration_operation(
        self,
        *,
        discord_user_id: str,
        game: SupportedGame,
        platform: RegistrationPlatform,
        account_id: str,
        account_name: str | None,
        method: RegistrationMethod,
        username_snapshot: str | None = None,
        display_name_snapshot: str | None = None,
    ) -> RegistrationOperationResponse:
        return await self._create_operation(
            operation_type="self_service_registration",
            discord_user_id=discord_user_id,
            linked_platform=platform,
            linked_account_id=account_id,
            linked_account_name=account_name,
            steam_id=None,
            steam_name=None,
            game=game,
            registration_method=method,
            role_intents=_build_registration_role_intents(game, platform),
            ownership_verified_at=None,
            playtime_minutes=None,
            username_snapshot=username_snapshot,
            display_name_snapshot=display_name_snapshot,
        )

    async def _create_operation(
        self,
        *,
        operation_type: str,
        discord_user_id: str,
        linked_platform: RegistrationPlatform,
        linked_account_id: str,
        linked_account_name: str | None,
        steam_id: str | None,
        steam_name: str | None,
        game: SupportedGame,
        registration_method: RegistrationMethod,
        role_intents: list[RoleIntent],
        source_session_id: str | None = None,
        username_snapshot: str | None = None,
        display_name_snapshot: str | None = None,
        locale_snapshot: str | None = None,
        verified_snapshot: bool | None = None,
        mfa_enabled_snapshot: bool | None = None,
        ownership_verified_at: datetime | None = None,
        playtime_minutes: int | None = None,
        extra_operation_fields: dict[str, Any] | None = None,
    ) -> RegistrationOperationResponse:
        operation_id = token_urlsafe(24)
        now = datetime.now(UTC)
        operation_doc: dict[str, Any] = {
            "operation_id": operation_id,
            "type": operation_type,
            "status": RegistrationOperationStatus.PENDING.value,
            "discord_user_id": discord_user_id,
            "linked_platform": linked_platform.value,
            "linked_account_id": linked_account_id,
            "linked_account_name": linked_account_name,
            "steam_id": steam_id,
            "steam_name": steam_name,
            "game": game.value,
            "registration_method": registration_method.value,
            "role_intents": [intent.value for intent in role_intents],
            "source_session_id": source_session_id,
            "username_snapshot": username_snapshot,
            "display_name_snapshot": display_name_snapshot,
            "locale_snapshot": locale_snapshot,
            "verified_snapshot": verified_snapshot,
            "mfa_enabled_snapshot": mfa_enabled_snapshot,
            "ownership_verified_at": ownership_verified_at,
            "playtime_minutes": playtime_minutes,
            "created_at": now,
            "updated_at": now,
        }
        if extra_operation_fields:
            operation_doc.update(extra_operation_fields)

        await self._repository.insert_registration_operation(operation_doc)

        return RegistrationOperationResponse(
            operation_id=operation_id,
            status=RegistrationOperationStatus.PENDING,
            discord_user_id=discord_user_id,
            steam_id=steam_id or linked_account_id,
            steam_name=steam_name,
            linked_platform=linked_platform,
            linked_account_id=linked_account_id,
            linked_account_name=linked_account_name,
            registration_method=registration_method,
            game=game,
            role_intents=role_intents,
        )


def _registration_summaries(doc: dict[str, Any]) -> list[RegistrationSummary]:
    regs = doc.get("registrations") or {}
    summaries: list[RegistrationSummary] = []
    for game_key, entry in regs.items():
        try:
            game = SupportedGame(str(game_key))
        except ValueError:
            continue
        method: str | None = None
        registered_at: datetime | None = None
        if isinstance(entry, dict):
            method = str(entry["method"]) if entry.get("method") else None
            raw_registered_at = entry.get("registered_at")
            registered_at = (
                raw_registered_at if isinstance(raw_registered_at, datetime) else None
            )
        summaries.append(
            RegistrationSummary(game=game, method=method, registered_at=registered_at)
        )
    summaries.sort(key=lambda summary: summary.game.value)
    return summaries


def _has_steam_api_registration(registrations: dict[str, Any]) -> bool:
    for entry in registrations.values():
        if (
            isinstance(entry, dict)
            and str(entry.get("method") or "") in STEAM_API_REGISTRATION_METHODS
        ):
            return True
    return False


def _rank_role_for_game(game: SupportedGame) -> RoleIntent:
    return {
        SupportedGame.CIV6: RoleIntent.GRANT_CIV6_RANK,
        SupportedGame.CIV7: RoleIntent.GRANT_CIV7_RANK,
    }[game]


def _build_registration_role_intents(
    game: SupportedGame,
    platform: RegistrationPlatform,
) -> list[RoleIntent]:
    return [
        _rank_role_for_game(game),
        RoleIntent.GRANT_NOVICE,
        RoleIntent.GRANT_SERVER_NEWS,
        RoleIntent.GRANT_CIV6_NEWS
        if game is SupportedGame.CIV6
        else RoleIntent.GRANT_CIV7_NEWS,
        (
            RoleIntent.GRANT_PC_STEAM
            if platform is RegistrationPlatform.STEAM
            else RoleIntent.GRANT_2K_CROSSPLATFORM
        ),
        RoleIntent.REMOVE_NON_VERIFIED,
    ]


def _to_discord_lookup_response(docs: list[dict[str, Any]]) -> DiscordLookupResponse:
    primary = docs[0]
    seen: set[tuple[str | None, str]] = set()
    linked_accounts: list[LinkedAccountLookupHit] = []

    for doc in docs:
        platform = _normalize_platform(doc)
        account_id = _normalize_linked_account_id(doc)
        if not account_id:
            continue
        key = (platform.value if platform else None, account_id)
        if key in seen:
            continue
        seen.add(key)
        linked_accounts.append(
            LinkedAccountLookupHit(
                linked_platform=platform,
                linked_account_id=account_id,
                linked_account_name=_normalize_linked_account_name(doc),
                registrations=_registration_summaries(doc),
            )
        )

    linked_accounts.sort(
        key=lambda hit: (
            (hit.linked_platform.value if hit.linked_platform else ""),
            hit.linked_account_id,
        )
    )

    return DiscordLookupResponse(
        discord_id=str(primary.get("discord_id", "")),
        discord_username=_normalize_discord_username(primary),
        discord_display_name=_normalize_display_name(primary),
        linked_accounts=linked_accounts,
    )


def _to_linked_account_lookup_response(
    docs: list[dict[str, Any]],
    linked_account_id: str,
) -> LinkedAccountLookupResponse:
    primary_doc = next(
        (doc for doc in docs if _normalize_linked_account_id(doc) == linked_account_id),
        docs[0],
    )
    seen: set[str] = set()
    discord_accounts: list[DiscordAccountLookupHit] = []

    for doc in docs:
        discord_id = str(doc.get("discord_id", "")).strip()
        if not discord_id or discord_id in seen:
            continue
        seen.add(discord_id)
        discord_accounts.append(
            DiscordAccountLookupHit(
                discord_id=discord_id,
                discord_username=_normalize_discord_username(doc),
                discord_display_name=_normalize_display_name(doc),
                registrations=_registration_summaries(doc),
            )
        )

    discord_accounts.sort(key=lambda hit: hit.discord_id)

    return LinkedAccountLookupResponse(
        linked_account_id=linked_account_id,
        linked_account_name=_normalize_linked_account_name(primary_doc),
        linked_platform=_normalize_platform(primary_doc),
        discord_accounts=discord_accounts,
    )


def _normalize_platform(doc: dict[str, Any]) -> RegistrationPlatform | None:
    if doc.get("linked_platform"):
        return RegistrationPlatform(str(doc["linked_platform"]))
    if doc.get("steam_id"):
        return RegistrationPlatform.STEAM
    return None


def _normalize_linked_account_id(doc: dict[str, Any]) -> str | None:
    if doc.get("linked_account_id"):
        return str(doc["linked_account_id"])
    if doc.get("steam_id"):
        return str(doc["steam_id"])
    return None


def _normalize_linked_account_name(doc: dict[str, Any]) -> str | None:
    if doc.get("linked_account_name"):
        return str(doc["linked_account_name"])
    if doc.get("steam_name"):
        return str(doc["steam_name"])
    return None


def _normalize_discord_username(doc: dict[str, Any]) -> str | None:
    if doc.get("discord_username"):
        return str(doc["discord_username"])
    if doc.get("user_name"):
        return str(doc["user_name"])
    return None


def _normalize_display_name(doc: dict[str, Any]) -> str | None:
    return str(doc["display_name"]) if doc.get("display_name") else None
