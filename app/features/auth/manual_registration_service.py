from __future__ import annotations

from app.features.auth.registration_service import RegistrationService
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import ManualRegistrationRequest, RegistrationOperationResponse
from app.features.auth.steam_service import SteamService


class ManualRegistrationService:
    def __init__(self, repository: AuthRepository, steam_service: SteamService) -> None:
        self._registration_service = RegistrationService(repository)
        self._steam_service = steam_service

    async def create_manual_registration(
        self,
        payload: ManualRegistrationRequest,
    ) -> RegistrationOperationResponse:
        ownership_verified_at = None
        playtime_minutes = None
        account_name = None

        if payload.platform.value == 'steam':
            validation = await self._steam_service.validate_linked_account(
                steam_id=payload.account_id,
                game=payload.game.value,
            )
            ownership_verified_at = validation.get('ownership_verified_at')
            playtime_minutes = validation.get('playtime_minutes')
            account_name = str(validation['steam_name']) if validation.get('steam_name') else None

        await self._registration_service.assert_registration_conflicts(
            discord_user_id=payload.subject_discord_id,
            platform=payload.platform,
            account_id=payload.account_id,
            game=payload.game.value,
        )
        return await self._registration_service.create_manual_registration_operation(
            actor_discord_id=payload.actor_discord_id,
            subject_discord_id=payload.subject_discord_id,
            game=payload.game,
            platform=payload.platform,
            account_id=payload.account_id,
            account_name=account_name,
            ownership_verified_at=ownership_verified_at,
            playtime_minutes=playtime_minutes,
            reason=payload.reason,
        )
