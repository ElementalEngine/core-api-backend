from __future__ import annotations

from enum import StrEnum


class SupportedGame(StrEnum):
    CIV6 = "civ6"
    CIV7 = "civ7"


# Epic and Xbox are only read from older records; nothing registers them.
class RegistrationPlatform(StrEnum):
    STEAM = "steam"
    EPIC = "epic"
    TWOK = "2k"
    XBOX = "xbox"


class RegistrationMethod(StrEnum):
    """How a registration's account ownership was established."""

    OAUTH_STEAM_API = "oauth_steam_api"
    ADMIN_STEAM_FAMILY_SHARE = "admin_steam_family_share"
    ADMIN_STAFF_ATTESTED = "admin_staff_attested"
    SELF_SERVICE_2K = "self_service_2k"


class ManualRegistrationChoice(StrEnum):
    """Staff-facing platform choice for `/manual-register`."""

    STEAM = "steam"
    STEAM_FAMILY_SHARE = "steam_family_share"
    TWOK = "2k"

    def resolved(self) -> tuple[RegistrationPlatform, RegistrationMethod]:
        return {
            ManualRegistrationChoice.STEAM: (
                RegistrationPlatform.STEAM,
                RegistrationMethod.ADMIN_STAFF_ATTESTED,
            ),
            ManualRegistrationChoice.STEAM_FAMILY_SHARE: (
                RegistrationPlatform.STEAM,
                RegistrationMethod.ADMIN_STEAM_FAMILY_SHARE,
            ),
            ManualRegistrationChoice.TWOK: (
                RegistrationPlatform.TWOK,
                RegistrationMethod.ADMIN_STAFF_ATTESTED,
            ),
        }[self]


class RoleIntent(StrEnum):
    GRANT_CIV6_RANK = "grant_civ6_rank"
    GRANT_CIV7_RANK = "grant_civ7_rank"
    GRANT_NOVICE = "grant_novice"
    GRANT_SERVER_NEWS = "grant_server_news"
    GRANT_CIV6_NEWS = "grant_civ6_news"
    GRANT_CIV7_NEWS = "grant_civ7_news"
    GRANT_PC_STEAM = "grant_pc_steam"
    GRANT_2K_CROSSPLATFORM = "grant_2k_crossplatform"
    REMOVE_NON_VERIFIED = "remove_non_verified"


STEAM_API_REGISTRATION_METHODS: frozenset[str] = frozenset(
    {RegistrationMethod.OAUTH_STEAM_API.value, "oauth"}
)
