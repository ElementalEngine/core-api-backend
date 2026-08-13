from __future__ import annotations

from app.features.auth.enums import (
    RegistrationMethod,
    RegistrationPlatform,
    SupportedGame,
)
from app.features.auth.errors import SelfServiceRegistrationNotAllowedError
from app.features.auth.registration_service import RegistrationService
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import (
    ManualRegistrationRequest,
    RegistrationOperationResponse,
    SelfServiceRegistrationRequest,
)


class ManualRegistrationService:
    def __init__(self, repository: AuthRepository) -> None:
        self._registration_service = RegistrationService(repository)

    async def create_manual_registration(
        self,
        payload: ManualRegistrationRequest,
    ) -> RegistrationOperationResponse:
        platform, method = payload.platform.resolved()

        await self._registration_service.assert_registration_conflicts(
            discord_user_id=payload.subject_discord_id,
            platform=platform,
            account_id=payload.platform_account_id,
            game=payload.game.value,
        )

        return await self._registration_service.create_manual_registration_operation(
            actor_discord_id=payload.actor_discord_id,
            subject_discord_id=payload.subject_discord_id,
            game=payload.game,
            platform=platform,
            method=method,
            account_id=payload.platform_account_id,
            account_name=payload.platform_account_name,
            reason=payload.reason,
            username_snapshot=payload.discord_username,
            display_name_snapshot=payload.discord_display_name,
        )

    async def create_self_service_registration(
        self,
        payload: SelfServiceRegistrationRequest,
    ) -> RegistrationOperationResponse:
        if (
            payload.game is not SupportedGame.CIV7
            or payload.platform is not RegistrationPlatform.TWOK
        ):
            raise SelfServiceRegistrationNotAllowedError(
                game=payload.game.value,
                platform=payload.platform.value,
            )

        await self._registration_service.assert_registration_conflicts(
            discord_user_id=payload.discord_user_id,
            platform=RegistrationPlatform.TWOK,
            account_id=payload.platform_account_id,
            game=payload.game.value,
        )

        return (
            await self._registration_service.create_self_service_registration_operation(
                discord_user_id=payload.discord_user_id,
                game=payload.game,
                platform=RegistrationPlatform.TWOK,
                account_id=payload.platform_account_id,
                account_name=payload.platform_account_name,
                method=RegistrationMethod.SELF_SERVICE_2K,
                username_snapshot=payload.discord_username,
                display_name_snapshot=payload.discord_display_name,
            )
        )
