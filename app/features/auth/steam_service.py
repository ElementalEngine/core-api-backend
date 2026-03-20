from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from app.core.config import settings
from app.features.auth.constants import DEFAULT_STEAM_APP_IDS, DEFAULT_STEAM_PLAYTIME_MINUTES
from app.features.auth.enums import SupportedGame
from app.features.auth.errors import (
    AuthConfigurationError,
    SteamApiError,
    SteamOwnershipMissingError,
    SteamPlaytimeBelowThresholdError,
    SteamProfilePrivateError,
)


class SteamService:
    async def validate_linked_account(self, *, steam_id: str, game: str) -> dict[str, object]:
        if not settings.auth_steam_api_key.get_secret_value():
            raise AuthConfigurationError("Steam API is not configured for auth registration.")

        app_id = self._app_id_for(game)
        required_minutes = self._required_minutes_for(game)
        payload = await self._get_owned_games(steam_id=steam_id, app_id=app_id)
        response = payload.get("response") if isinstance(payload, dict) else None
        games = response.get("games") if isinstance(response, dict) else None
        if not games:
            raise SteamProfilePrivateError(steam_id)

        match = next((g for g in games if int(g.get("appid", 0)) == app_id), None)
        if match is None:
            raise SteamOwnershipMissingError(steam_id=steam_id, game=game, app_id=app_id)

        actual_minutes = int(match.get("playtime_forever") or 0)
        if actual_minutes < required_minutes:
            raise SteamPlaytimeBelowThresholdError(
                steam_id=steam_id,
                game=game,
                required_minutes=required_minutes,
                actual_minutes=actual_minutes,
            )

        return {
            "steam_id": steam_id,
            "app_id": app_id,
            "required_minutes": required_minutes,
            "actual_minutes": actual_minutes,
            "ownership_verified_at": datetime.now(timezone.utc),
            "account_name": match.get("name") if isinstance(match.get("name"), str) else None,
        }

    async def _get_owned_games(self, *, steam_id: str, app_id: int) -> dict[str, object]:
        params = urlencode(
            {
                "key": settings.auth_steam_api_key.get_secret_value(),
                "steamid": steam_id,
                "include_appinfo": "1",
                "include_played_free_games": "1",
                "appids_filter[0]": str(app_id),
                "format": "json",
            }
        )
        url = f"https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?{params}"

        def _request() -> dict[str, object]:
            req = Request(url, headers={"Accept": "application/json"}, method="GET")
            try:
                with urlopen(req, timeout=settings.auth_steam_timeout_seconds) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                raise SteamApiError() from exc

        return await asyncio.to_thread(_request)

    @staticmethod
    def _app_id_for(game: str) -> int:
        mapping = {
            SupportedGame.CIV6.value: settings.auth_steam_civ6_app_id,
            SupportedGame.CIV7.value: settings.auth_steam_civ7_app_id,
        }
        return mapping.get(game, DEFAULT_STEAM_APP_IDS[game])

    @staticmethod
    def _required_minutes_for(game: str) -> int:
        mapping = {
            SupportedGame.CIV6.value: settings.auth_steam_civ6_required_minutes,
            SupportedGame.CIV7.value: settings.auth_steam_civ7_required_minutes,
        }
        return mapping.get(game, DEFAULT_STEAM_PLAYTIME_MINUTES[game])
