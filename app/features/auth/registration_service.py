from __future__ import annotations

from typing import Any

from app.features.auth.enums import RegistrationPlatform
from app.features.auth.errors import AlreadyRegisteredError, ManualRegistrationRequiredError
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import AccountLookupResponse


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

    @staticmethod
    def manual_required_for_platform(
        platform: RegistrationPlatform,
        *,
        account_name: str | None = None,
    ) -> None:
        if platform in {RegistrationPlatform.EPIC, RegistrationPlatform.XBOX}:
            raise ManualRegistrationRequiredError(platform.value, account_name=account_name)



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
