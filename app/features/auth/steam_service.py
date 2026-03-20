from app.features.auth.errors import AuthConfigurationError
class SteamService:
    """TODO: Implement Steam-specific validation logic."""
    async def validate_linked_account(self, *, steam_id:str, game:str)->dict[str, object]:
        raise AuthConfigurationError("Steam validation has not been enabled in this backend patch yet.")
