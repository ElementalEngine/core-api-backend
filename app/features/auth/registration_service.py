from __future__ import annotations

from datetime import datetime, timezone
from secrets import token_urlsafe
from typing import Any

from app.features.auth.enums import (
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
)


class RegistrationService:
    def __init__(self, repository: AuthRepository) -> None:
        self._repository = repository

    async def lookup_by_discord_id(self, discord_id: str) -> DiscordLookupResponse | None:
        docs = await self._repository.find_users_by_discord_id(discord_id)
        if not docs:
            return None

        primary = docs[0]
        linked_accounts = [_to_linked_account_hit(doc) for doc in docs]
        linked_accounts = [hit for hit in linked_accounts if hit is not None]
        linked_accounts.sort(
            key=lambda hit: (
                hit.linked_platform.value if hit.linked_platform else "",
                hit.linked_account_id,
                hit.linked_account_name or "",
            )
        )

        return DiscordLookupResponse(
            discord_id=discord_id,
            discord_username=_resolve_discord_username(primary),
            discord_display_name=_resolve_display_name(primary),
            linked_accounts=linked_accounts,
        )

    async def lookup_by_linked_account_id(self, linked_account_id: str) -> LinkedAccountLookupResponse | None:
        docs = await self._repository.find_users_by_linked_account_id(linked_account_id)
        if not docs:
            return None

        discord_accounts = [_to_discord_account_hit(doc) for doc in docs]
        discord_accounts.sort(
            key=lambda hit: (
                hit.discord_id,
                hit.discord_username or "",
                hit.discord_display_name or "",
            )
        )

        primary = _pick_primary_linked_account_doc(docs, linked_account_id)

        return LinkedAccountLookupResponse(
            linked_account_id=linked_account_id,
            linked_account_name=_resolve_linked_account_name(primary),
            linked_platform=_resolve_linked_platform(primary),
            discord_accounts=discord_accounts,
        )

    async def assert_not_already_registered(self, *, discord_user_id: str, game: str) -> None:
        existing = await self._repository.get_user_by_discord_id(discord_user_id)
        regs = (existing or {}).get("registrations") or {}
        if game in regs:
            raise AlreadyRegisteredError(game)

    async def assert_registration_conflicts(
        self,
        *,
        discord_user_id: str,
        platform: RegistrationPlatform,
        account_id: str,
        game: str,
    ) -> None:
        existing_by_discord = await self._repository.get_user_by_discord_id(discord_user_id)
        existing_by_linked = (
            await self._repository.get_user_by_steam_id(account_id)
            if platform is RegistrationPlatform.STEAM
            else await self._repository.get_user_by_linked_account(platform.value, account_id)
        )

        if existing_by_linked and str(existing_by_linked.get("discord_id")) != discord_user_id:
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
            if existing_platform and existing_account and (
                str(existing_platform) != platform.value or str(existing_account) != account_id
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
        if not steam_id and user.get("linked_platform") == RegistrationPlatform.STEAM.value:
            steam_id = user.get("linked_account_id")

        if not steam_id:
            raise RankRoleEligibilityError(discord_user_id)

        regs = user.get("registrations") or {}
        if game in regs:
            raise AlreadyRegisteredError(game)
        return str(steam_id)

    @staticmethod
    def manual_required_for_platform(
        platform: RegistrationPlatform,
        *,
        account_name: str | None = None,
    ) -> None:
        if platform in {RegistrationPlatform.EPIC, RegistrationPlatform.XBOX}:
            raise ManualRegistrationRequiredError(platform.value, account_name=account_name)

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
                str(session["validated_account_name"]) if session.get("validated_account_name") else None
            ),
            steam_id=str(steam_validation["steam_id"]),
            steam_name=(
                str(session["validated_account_name"]) if session.get("validated_account_name") else None
            ),
            game=game,
            role_intents=_build_registration_role_intents(game),
            source_session_id=str(session["session_id"]),
            username_snapshot=(
                str(session["oauth_username_snapshot"]) if session.get("oauth_username_snapshot") else None
            ),
            display_name_snapshot=(
                str(session["oauth_display_name_snapshot"]) if session.get("oauth_display_name_snapshot") else None
            ),
            locale_snapshot=(
                str(session["oauth_locale_snapshot"]) if session.get("oauth_locale_snapshot") else None
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
        account_id: str,
        account_name: str | None,
        ownership_verified_at: datetime | None,
        playtime_minutes: int | None,
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
            role_intents=_build_registration_role_intents(game),
            ownership_verified_at=ownership_verified_at,
            playtime_minutes=playtime_minutes,
            username_snapshot=username_snapshot,
            display_name_snapshot=display_name_snapshot,
            extra_operation_fields={
                "actor_discord_id": actor_discord_id,
                **({"manual_reason": reason} if reason else {}),
            },
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
        now = datetime.now(timezone.utc)
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
            game=game,
            role_intents=role_intents,
        )


def _rank_role_for_game(game: SupportedGame) -> RoleIntent:
    return {
        SupportedGame.CIV6: RoleIntent.GRANT_CIV6_RANK,
        SupportedGame.CIV7: RoleIntent.GRANT_CIV7_RANK,
    }[game]


def _build_registration_role_intents(game: SupportedGame) -> list[RoleIntent]:
    if game is SupportedGame.CIV6:
        return [
            RoleIntent.GRANT_CIV6_RANK,
            RoleIntent.GRANT_NOVICE,
            RoleIntent.REMOVE_NON_VERIFIED,
        ]
    return [RoleIntent.GRANT_CIV7_RANK, RoleIntent.REMOVE_NON_VERIFIED]


def _pick_primary_linked_account_doc(docs: list[dict[str, Any]], linked_account_id: str) -> dict[str, Any]:
    for doc in docs:
        if _resolve_linked_account_id(doc) == linked_account_id:
            return doc
    return docs[0]



def _to_linked_account_hit(doc: dict[str, Any]) -> LinkedAccountLookupHit | None:
    linked_account_id = _resolve_linked_account_id(doc)
    if not linked_account_id:
        return None

    return LinkedAccountLookupHit(
        linked_platform=_resolve_linked_platform(doc),
        linked_account_id=linked_account_id,
        linked_account_name=_resolve_linked_account_name(doc),
    )



def _to_discord_account_hit(doc: dict[str, Any]) -> DiscordAccountLookupHit:
    return DiscordAccountLookupHit(
        discord_id=str(doc.get("discord_id", "")),
        discord_username=_resolve_discord_username(doc),
        discord_display_name=_resolve_display_name(doc),
    )



def _resolve_discord_username(doc: dict[str, Any]) -> str | None:
    if doc.get("discord_username"):
        return str(doc["discord_username"])
    if doc.get("user_name"):
        return str(doc["user_name"])
    return None



def _resolve_display_name(doc: dict[str, Any]) -> str | None:
    return str(doc["display_name"]) if doc.get("display_name") else None



def _resolve_linked_platform(doc: dict[str, Any]) -> RegistrationPlatform | None:
    if doc.get("linked_platform"):
        return RegistrationPlatform(str(doc["linked_platform"]))
    if doc.get("steam_id"):
        return RegistrationPlatform.STEAM
    return None



def _resolve_linked_account_id(doc: dict[str, Any]) -> str | None:
    if doc.get("linked_account_id"):
        return str(doc["linked_account_id"])
    if doc.get("steam_id"):
        return str(doc["steam_id"])
    return None



def _resolve_linked_account_name(doc: dict[str, Any]) -> str | None:
    if doc.get("linked_account_name"):
        return str(doc["linked_account_name"])
    if doc.get("steam_name"):
        return str(doc["steam_name"])
    return None
