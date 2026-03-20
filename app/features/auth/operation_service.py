from __future__ import annotations

from datetime import datetime, timezone

from app.features.auth.enums import (
    RegistrationOperationStatus,
    RegistrationSessionStatus,
)
from app.features.auth.errors import OperationNotFoundError, OperationStateConflictError
from app.features.auth.repository import AuthRepository
from app.features.auth.schemas import FinalizeRegistrationOperationRequest


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

        status_value = str(operation.get("status", RegistrationOperationStatus.PENDING.value))
        if status_value != RegistrationOperationStatus.PENDING.value:
            if status_value == payload.result:
                return
            raise OperationStateConflictError(operation_id, status_value)

        now = datetime.now(timezone.utc)
        if payload.result == RegistrationOperationStatus.SUCCEEDED.value:
            await self._repository.upsert_registered_user(
                discord_user_id=str(operation["discord_user_id"]),
                steam_id=str(operation["steam_id"]),
                game=str(operation["game"]),
                username_snapshot=operation.get("username_snapshot"),
                display_name_snapshot=operation.get("display_name_snapshot"),
                method="oauth",
                ownership_verified_at=operation.get("ownership_verified_at") or now,
                playtime_minutes=int(operation.get("playtime_minutes") or 0),
            )

        await self._repository.update_registration_operation(
            operation_id,
            {
                "status": payload.result,
                "applied_role_intents": [intent.value for intent in payload.applied_role_intents],
                "failure_code": payload.failure_code,
                "failure_message": payload.failure_message,
                "updated_at": now,
            },
        )

        source_session_id = operation.get("source_session_id")
        if source_session_id:
            session_changes = {"updated_at": now}
            if payload.result == RegistrationOperationStatus.SUCCEEDED.value:
                session_changes["status"] = RegistrationSessionStatus.COMPLETED.value
            else:
                session_changes["status"] = RegistrationSessionStatus.FAILED.value
                session_changes["failure_code"] = payload.failure_code
                session_changes["failure_message"] = payload.failure_message
            await self._repository.update_registration_session(
                str(source_session_id),
                session_changes,
            )

        await self._repository.append_audit_event(
            {
                "action": "registration_operation_finalized",
                "operation_id": operation_id,
                "discord_user_id": str(operation["discord_user_id"]),
                "steam_id": str(operation["steam_id"]),
                "game": str(operation["game"]),
                "result": payload.result,
                "failure_code": payload.failure_code,
            }
        )
