from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.config import settings
from app.features.auth.constants import DEFAULT_STEAM_APP_IDS, DEFAULT_STEAM_PLAYTIME_MINUTES, STEAM_OWNED_GAMES_URL
from app.features.auth.enums import SupportedGame
from app.features.auth.errors import (
    AuthConfigurationError,
    SteamOwnershipMissingError,
    SteamPlaytimeBelowThresholdError,
    SteamProfilePrivateError,
    SteamValidationError,
)


class SteamService:
    async def validate_linked_account(self, *, steam_id: str, game: str) -> dict[str, object]:
        api_key = settings.auth_steam_api_key.get_secret_value()
        if not api_key:
            raise AuthConfigurationError("Steam validation is not configured on the backend.")

        normalized_game = SupportedGame(game).value
        app_id = self._app_id_for_game(normalized_game)
        required_minutes = self._required_minutes_for_game(normalized_game)
        payload = await self._get_owned_games(steam_id=steam_id, app_id=app_id)
        response = payload.get("response") if isinstance(payload, dict) else None
        if not isinstance(response, dict):
            raise SteamValidationError()

        games = response.get("games")
        if response.get("game_count") is None and games is None:
            raise SteamProfilePrivateError()
        if not isinstance(games, list):
            raise SteamOwnershipMissingError(normalized_game)

        match = next((item for item in games if int(item.get("appid", 0)) == app_id), None)
        if not isinstance(match, dict):
            raise SteamOwnershipMissingError(normalized_game)

        playtime_minutes = int(match.get("playtime_forever", 0) or 0)
        if playtime_minutes < required_minutes:
            raise SteamPlaytimeBelowThresholdError(
                game=normalized_game,
                required_minutes=required_minutes,
                actual_minutes=playtime_minutes,
            )

        return {
            "steam_id": steam_id,
            "game": normalized_game,
            "app_id": app_id,
            "playtime_minutes": playtime_minutes,
            "ownership_verified_at": datetime.now(timezone.utc),
        }

    async def _get_owned_games(self, *, steam_id: str, app_id: int) -> dict[str, object]:
        def _request() -> dict[str, object]:
            params = urlencode(
                {
                    "key": settings.auth_steam_api_key.get_secret_value(),
                    "steamid": steam_id,
                    "appids_filter[0]": str(app_id),
                    "include_played_free_games": "1",
                }
            )
            req = Request(
                f"{STEAM_OWNED_GAMES_URL}?{params}",
                headers={"Accept": "application/json"},
                method="GET",
            )
            try:
                with urlopen(req, timeout=settings.auth_steam_timeout_seconds) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError, ValueError) as exc:
                raise SteamValidationError() from exc

        return await asyncio.to_thread(_request)

    @staticmethod
    def _app_id_for_game(game: str) -> int:
        mapping = {
            SupportedGame.CIV6.value: settings.auth_steam_civ6_app_id,
            SupportedGame.CIV7.value: settings.auth_steam_civ7_app_id,
        }
        return mapping.get(game, DEFAULT_STEAM_APP_IDS[game])

    @staticmethod
    def _required_minutes_for_game(game: str) -> int:
        mapping = {
            SupportedGame.CIV6.value: settings.auth_steam_civ6_required_minutes,
            SupportedGame.CIV7.value: settings.auth_steam_civ7_required_minutes,
        }
        return mapping.get(game, DEFAULT_STEAM_PLAYTIME_MINUTES[game])
