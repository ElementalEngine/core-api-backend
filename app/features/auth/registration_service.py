from __future__ import annotations

from datetime import datetime, timezone
from secrets import token_urlsafe
from typing import Any

from app.features.auth.enums import RegistrationOperationStatus, RegistrationPlatform, RoleIntent, SupportedGame
from app.features.auth.errors import (
    AlreadyRegisteredError,
    DiscordSteamConflictError,
    ManualRegistrationRequiredError,
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

    async def create_registration_operation(
        self,
        *,
        session: dict[str, Any],
    ) -> RegistrationOperationResponse:
        operation_id = token_urlsafe(24)
        game = str(session["game"])
        role_intents = self._role_intents_for(game)
        doc = {
            "operation_id": operation_id,
            "status": RegistrationOperationStatus.PENDING.value,
            "source_session_id": str(session["session_id"]),
            "discord_user_id": str(session["discord_user_id"]),
            "steam_id": str(session["validated_account_id"]),
            "game": game,
            "role_intents": [intent.value for intent in role_intents],
            "validated_account_name": session.get("validated_account_name"),
            "username_snapshot": session.get("username_snapshot"),
            "display_name_snapshot": session.get("display_name_snapshot"),
            "ownership_verified_at": session.get("ownership_verified_at"),
            "playtime_minutes": session.get("playtime_minutes"),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        await self._repository.insert_registration_operation(doc)
        await self._repository.append_audit_event(
            {
                "action": "registration_operation_created",
                "operation_id": operation_id,
                "discord_user_id": doc["discord_user_id"],
                "steam_id": doc["steam_id"],
                "game": game,
                "role_intents": doc["role_intents"],
                "source_session_id": doc["source_session_id"],
            }
        )
        return RegistrationOperationResponse(
            operation_id=operation_id,
            status=RegistrationOperationStatus.PENDING,
            discord_user_id=doc["discord_user_id"],
            steam_id=doc["steam_id"],
            game=SupportedGame(game),
            role_intents=role_intents,
        )

    @staticmethod
    def manual_required_for_platform(
        platform: RegistrationPlatform,
        *,
        account_name: str | None = None,
    ) -> None:
        if platform in {RegistrationPlatform.EPIC, RegistrationPlatform.XBOX}:
            raise ManualRegistrationRequiredError(platform.value, account_name=account_name)

    @staticmethod
    def _role_intents_for(game: str) -> list[RoleIntent]:
        if game == SupportedGame.CIV6.value:
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
