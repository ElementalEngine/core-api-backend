from __future__ import annotations

from app.features.auth.registration_service import RegistrationService
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import ManualRegistrationRequest, RegistrationOperationResponse
from app.features.auth.steam_service import SteamService


class ManualRegistrationService:
    def __init__(self, repository: AuthRepository, steam_service: SteamService) -> None:
        self._registration_service = RegistrationService(repository)
        self._steam_service = steam_service

    async def create_operation(
        self,
        payload: ManualRegistrationRequest,
    ) -> RegistrationOperationResponse:
        steam_validation = await self._steam_service.validate_linked_account(
            steam_id=payload.steam_id,
            game=payload.game.value,
        )
        return await self._registration_service.create_manual_registration_operation(
            actor_discord_id=payload.actor_discord_id,
            subject_discord_id=payload.subject_discord_id,
            steam_id=payload.steam_id,
            game=payload.game.value,
            reason=payload.reason,
            ownership_verified_at=steam_validation["ownership_verified_at"],
            playtime_minutes=int(steam_validation.get("actual_minutes") or 0),
        )
