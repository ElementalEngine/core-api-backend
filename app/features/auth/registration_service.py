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
    ManualRegistrationRequiredError,
    RankRoleEligibilityError,
    SteamIdConflictError,
)
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import AccountLookupResponse, RegistrationOperationResponse


class RegistrationService:
    def __init__(self, repository: AuthRepository) -> None:
        self._repository = repository

    async def lookup_by_discord_id(self, discord_id: str) -> AccountLookupResponse | None:
        doc = await self._repository.get_user_by_discord_id(discord_id)
        return _to_lookup_response(doc) if doc else None

    async def lookup_by_steam_id(self, steam_id: str) -> AccountLookupResponse | None:
        doc = await self._repository.get_user_by_steam_id(steam_id)
        return _to_lookup_response(doc) if doc else None

    async def assert_not_already_registered(self, *, discord_user_id: str, game: str) -> None:
        existing = await self._repository.get_user_by_discord_id(discord_user_id)
        regs = (existing or {}).get("registrations") or {}
        if game in regs:
            raise AlreadyRegisteredError(game)

    async def assert_registration_conflicts(
        self,
        *,
        discord_user_id: str,
        steam_id: str,
        game: str,
    ) -> None:
        existing_by_discord = await self._repository.get_user_by_discord_id(discord_user_id)
        existing_by_steam = await self._repository.get_user_by_steam_id(steam_id)

        if existing_by_steam and str(existing_by_steam.get("discord_id")) != discord_user_id:
            raise SteamIdConflictError(
                steam_id=steam_id,
                existing_discord_id=str(existing_by_steam.get("discord_id", "")),
            )

        if existing_by_discord:
            existing_steam = existing_by_discord.get("steam_id")
            if existing_steam and str(existing_steam) != steam_id:
                raise DiscordSteamConflictError(
                    discord_user_id=discord_user_id,
                    existing_steam_id=str(existing_steam),
                )
            regs = existing_by_discord.get("registrations") or {}
            if game in regs:
                raise AlreadyRegisteredError(game)

    async def get_registered_steam_id(self, discord_user_id: str, game: str) -> str:
        user = await self._repository.get_user_by_discord_id(discord_user_id)
        if not user or not user.get("steam_id"):
            raise RankRoleEligibilityError(discord_user_id)
        regs = user.get("registrations") or {}
        if game in regs:
            raise AlreadyRegisteredError(game)
        return str(user["steam_id"])

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
            steam_id=str(steam_validation["steam_id"]),
            game=game,
            role_intents=_build_registration_role_intents(game),
            source_session_id=str(session["session_id"]),
            username_snapshot=session.get("oauth_username_snapshot"),
            display_name_snapshot=session.get("oauth_display_name_snapshot"),
            ownership_verified_at=steam_validation.get("ownership_verified_at"),
            playtime_minutes=steam_validation.get("playtime_minutes"),
            audit_action="registration_operation_created",
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
            steam_id=str(steam_validation["steam_id"]),
            game=game,
            role_intents=[_rank_role_for_game(game)],
            ownership_verified_at=steam_validation.get("ownership_verified_at"),
            playtime_minutes=steam_validation.get("playtime_minutes"),
            audit_action="rank_role_operation_created",
        )

    async def create_manual_registration_operation(
        self,
        *,
        actor_discord_id: str,
        subject_discord_id: str,
        game: SupportedGame,
        steam_validation: dict[str, Any],
        reason: str,
    ) -> RegistrationOperationResponse:
        return await self._create_operation(
            operation_type="manual_registration",
            discord_user_id=subject_discord_id,
            steam_id=str(steam_validation["steam_id"]),
            game=game,
            role_intents=_build_registration_role_intents(game),
            ownership_verified_at=steam_validation.get("ownership_verified_at"),
            playtime_minutes=steam_validation.get("playtime_minutes"),
            audit_action="manual_registration_operation_created",
            extra_operation_fields={
                "actor_discord_id": actor_discord_id,
                "manual_reason": reason,
            },
        )

    async def _create_operation(
        self,
        *,
        operation_type: str,
        discord_user_id: str,
        steam_id: str,
        game: SupportedGame,
        role_intents: list[RoleIntent],
        audit_action: str,
        source_session_id: str | None = None,
        username_snapshot: str | None = None,
        display_name_snapshot: str | None = None,
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
            "steam_id": steam_id,
            "game": game.value,
            "role_intents": [intent.value for intent in role_intents],
            "source_session_id": source_session_id,
            "username_snapshot": username_snapshot,
            "display_name_snapshot": display_name_snapshot,
            "ownership_verified_at": ownership_verified_at,
            "playtime_minutes": playtime_minutes,
            "created_at": now,
            "updated_at": now,
        }
        if extra_operation_fields:
            operation_doc.update(extra_operation_fields)
        await self._repository.insert_registration_operation(operation_doc)
        await self._repository.append_audit_event(
            {
                "action": audit_action,
                "operation_id": operation_id,
                "discord_user_id": discord_user_id,
                "steam_id": steam_id,
                "game": game.value,
                "role_intents": [intent.value for intent in role_intents],
            }
        )
        return RegistrationOperationResponse(
            operation_id=operation_id,
            status=RegistrationOperationStatus.PENDING,
            discord_user_id=discord_user_id,
            steam_id=steam_id,
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


def _to_lookup_response(doc: dict[str, Any]) -> AccountLookupResponse:
    return AccountLookupResponse(
        discord_id=str(doc.get("discord_id", "")),
        steam_id=doc.get("steam_id"),
        username_snapshot=doc.get("user_name"),
        display_name_snapshot=doc.get("display_name"),
        registrations=doc.get("registrations") or {},
        created_at=doc.get("created_at"),
        updated_at=doc.get("updated_at"),
    )
