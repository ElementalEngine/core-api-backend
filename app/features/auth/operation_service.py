from __future__ import annotations

import logging
from datetime import UTC, datetime

from app.features.auth.enums import (
    RegistrationOperationStatus,
    RegistrationSessionStatus,
)
from app.features.auth.errors import OperationNotFoundError, OperationStateConflictError
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import FinalizeRegistrationOperationRequest

logger = logging.getLogger(__name__)


class OperationService:
    def __init__(self, repository: AuthRepository) -> None:
        self._repository = repository

    async def finalize_operation(
        self,
        operation_id: str,
        payload: FinalizeRegistrationOperationRequest,
    ) -> None:
        operation = await self._repository.get_registration_operation(operation_id)
        if operation is None:
            raise OperationNotFoundError(operation_id)

        status_value = str(
            operation.get("status", RegistrationOperationStatus.PENDING.value)
        )
        if status_value != RegistrationOperationStatus.PENDING.value:
            expected = (
                RegistrationOperationStatus.SUCCEEDED.value
                if payload.result == "succeeded"
                else RegistrationOperationStatus.FAILED.value
            )
            if status_value == expected:
                return
            raise OperationStateConflictError(operation_id, status_value)

        now = datetime.now(UTC)
        if payload.result == "succeeded":
            registration_method = operation.get("registration_method")
            if not registration_method:
                # Back-compat for operations created before registration_method existed.
                registration_method = (
                    "manual_admin"
                    if operation.get("type") == "manual_registration"
                    else "oauth"
                )
            await self._repository.upsert_registered_user(
                discord_user_id=str(operation["discord_user_id"]),
                linked_platform=str(operation.get("linked_platform") or "steam"),
                linked_account_id=str(
                    operation.get("linked_account_id") or operation["steam_id"]
                ),
                linked_account_name=(
                    str(operation["linked_account_name"])
                    if operation.get("linked_account_name")
                    else (
                        str(operation["steam_name"])
                        if operation.get("steam_name")
                        else None
                    )
                ),
                game=str(operation["game"]),
                method=str(registration_method),
                discord_username=(
                    str(operation["username_snapshot"])
                    if operation.get("username_snapshot")
                    else None
                ),
                display_name=(
                    str(operation["display_name_snapshot"])
                    if operation.get("display_name_snapshot")
                    else None
                ),
                ownership_verified_at=operation.get("ownership_verified_at"),
                playtime_minutes=operation.get("playtime_minutes"),
            )
            await self._repository.update_registration_operation(
                operation_id,
                {
                    "status": RegistrationOperationStatus.SUCCEEDED.value,
                    "applied_role_intents": [
                        intent.value for intent in payload.applied_role_intents
                    ],
                    "completed_at": now,
                    "updated_at": now,
                },
            )
            source_session_id = operation.get("source_session_id")
            if isinstance(source_session_id, str) and source_session_id:
                try:
                    await self._repository.update_registration_session(
                        source_session_id,
                        {
                            "status": RegistrationSessionStatus.COMPLETED.value,
                            "updated_at": now,
                        },
                    )
                except Exception:
                    logger.warning(
                        "Failed to mark source session completed after successful operation finalize. operation_id=%s session_id=%s",
                        operation_id,
                        source_session_id,
                        exc_info=True,
                    )
            return

        await self._repository.update_registration_operation(
            operation_id,
            {
                "status": RegistrationOperationStatus.FAILED.value,
                "failure_code": payload.failure_code,
                "failure_message": payload.failure_message,
                "completed_at": now,
                "updated_at": now,
            },
        )
        source_session_id = operation.get("source_session_id")
        if isinstance(source_session_id, str) and source_session_id:
            try:
                await self._repository.update_registration_session(
                    source_session_id,
                    {
                        "status": RegistrationSessionStatus.FAILED.value,
                        "failure_code": payload.failure_code,
                        "failure_message": payload.failure_message,
                        "updated_at": now,
                    },
                )
            except Exception:
                logger.warning(
                    "Failed to mark source session failed after failed operation finalize. operation_id=%s session_id=%s",
                    operation_id,
                    source_session_id,
                    exc_info=True,
                )
