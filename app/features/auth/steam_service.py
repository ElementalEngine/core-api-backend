from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.config import settings
from app.features.auth.constants import (
    DEFAULT_STEAM_APP_IDS,
    DEFAULT_STEAM_PLAYTIME_MINUTES,
    STEAM_OWNED_GAMES_URL,
    STEAM_PLAYER_SUMMARIES_URL,
)
from app.features.auth.enums import SupportedGame
from app.features.auth.errors import (
    AuthConfigurationError,
    SteamOwnershipMissingError,
    SteamPlaytimeBelowThresholdError,
    SteamProfilePrivateError,
    SteamValidationError,
)

logger = logging.getLogger(__name__)
STEAM_HTTP_USER_AGENT = "CivPlayersAuth/1.0 (+https://elementalengine.net)"


class SteamService:
    async def validate_linked_account(
        self, *, steam_id: str, game: str
    ) -> dict[str, object]:
        api_key = settings.auth_steam_api_key.get_secret_value()
        if not api_key:
            raise AuthConfigurationError(
                "Steam validation is not configured on the backend."
            )

        normalized_game = SupportedGame(game).value
        app_id = self._app_id_for_game(normalized_game)
        required_minutes = self._required_minutes_for_game(normalized_game)

        payload = await self._get_owned_games(steam_id=steam_id, app_id=app_id)
        response = payload.get("response") if isinstance(payload, dict) else None
        if not isinstance(response, dict):
            logger.warning(
                "Steam owned-games response shape invalid. steam_id=%s game=%s payload=%s",
                steam_id,
                normalized_game,
                json.dumps(payload, default=str)[:1000],
            )
            raise SteamValidationError()

        games = response.get("games")
        if response.get("game_count") is None and games is None:
            logger.info(
                "Steam profile appears private or owned games unavailable. steam_id=%s game=%s",
                steam_id,
                normalized_game,
            )
            raise SteamProfilePrivateError(game=normalized_game)

        if not isinstance(games, list):
            logger.info(
                "Steam ownership missing because games list was unavailable. steam_id=%s game=%s response=%s",
                steam_id,
                normalized_game,
                json.dumps(response, default=str)[:1000],
            )
            raise SteamOwnershipMissingError(normalized_game)

        match = next(
            (item for item in games if int(item.get("appid", 0)) == app_id), None
        )
        if not isinstance(match, dict):
            logger.info(
                "Steam ownership missing. steam_id=%s game=%s app_id=%s",
                steam_id,
                normalized_game,
                app_id,
            )
            raise SteamOwnershipMissingError(normalized_game)

        playtime_minutes = int(match.get("playtime_forever", 0) or 0)
        if playtime_minutes < required_minutes:
            logger.info(
                "Steam playtime below threshold. steam_id=%s game=%s required=%s actual=%s",
                steam_id,
                normalized_game,
                required_minutes,
                playtime_minutes,
            )
            raise SteamPlaytimeBelowThresholdError(
                game=normalized_game,
                required_minutes=required_minutes,
                actual_minutes=playtime_minutes,
            )

        return {
            "steam_id": steam_id,
            "steam_name": None,
            "game": normalized_game,
            "app_id": app_id,
            "playtime_minutes": playtime_minutes,
            "ownership_verified_at": datetime.now(UTC),
        }

    async def account_created(self, steam_id: str) -> datetime | None:
        """When the Steam account was made, if Steam says; None when it cannot."""
        try:
            payload = await self._get_json(
                STEAM_PLAYER_SUMMARIES_URL, {"steamids": steam_id}
            )
        except SteamValidationError:
            return None
        response = payload.get("response")
        players = response.get("players") if isinstance(response, dict) else None
        first = players[0] if isinstance(players, list) and players else None
        created = first.get("timecreated") if isinstance(first, dict) else None
        return (
            datetime.fromtimestamp(created, UTC) if isinstance(created, int) else None
        )

    async def _get_owned_games(
        self, *, steam_id: str, app_id: int
    ) -> dict[str, object]:
        return await self._get_json(
            STEAM_OWNED_GAMES_URL,
            {
                "steamid": steam_id,
                "appids_filter[0]": str(app_id),
                "include_played_free_games": "1",
            },
        )

    async def _get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        def _request() -> dict[str, Any]:
            query = urlencode(
                {"key": settings.auth_steam_api_key.get_secret_value(), **params},
                safe="[]",
            )
            req = Request(
                f"{url}?{query}",
                headers={
                    "Accept": "application/json",
                    "User-Agent": STEAM_HTTP_USER_AGENT,
                },
                method="GET",
            )
            timeout_seconds = max(1, int(settings.auth_steam_timeout_seconds))
            try:
                with urlopen(req, timeout=timeout_seconds) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
            except HTTPError as exc:
                response = exc.read().decode("utf-8", errors="replace")
                logger.warning(
                    "Steam request failed. url=%s params=%s status=%s response=%s",
                    url,
                    params,
                    exc.code,
                    response[:1000],
                )
                raise SteamValidationError() from exc
            except (URLError, TimeoutError) as exc:
                logger.warning(
                    "Steam request failed. url=%s params=%s error=%r", url, params, exc
                )
                raise SteamValidationError() from exc
            try:
                payload = json.loads(body)
            except ValueError as exc:
                logger.warning(
                    "Steam returned invalid JSON. url=%s params=%s body=%s",
                    url,
                    params,
                    body[:1000],
                )
                raise SteamValidationError() from exc
            if not isinstance(payload, dict):
                raise SteamValidationError()
            return payload

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
