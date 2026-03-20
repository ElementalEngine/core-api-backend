from __future__ import annotations
from enum import StrEnum
class SupportedGame(StrEnum):
    CIV6 = "civ6"
    CIV7 = "civ7"
class RegistrationPlatform(StrEnum):
    STEAM = "steam"
    EPIC = "epic"
    XBOX = "xbox"
    @property
    def discord_connection_type(self) -> str:
        return {RegistrationPlatform.STEAM:"steam",RegistrationPlatform.EPIC:"epicgames",RegistrationPlatform.XBOX:"xbox"}[self]
class RegistrationSessionStatus(StrEnum):
    PENDING_AUTH = "pending_auth"
    VALIDATING = "validating"
    VALIDATED = "validated"
    FAILED = "failed"
    EXPIRED = "expired"
    COMPLETED = "completed"
  