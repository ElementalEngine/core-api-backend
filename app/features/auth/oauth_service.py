from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.config import settings
from app.features.auth.constants import (
    DISCORD_API_CONNECTIONS_URL,
    DISCORD_API_ME_URL,
    DISCORD_OAUTH_AUTHORIZE_URL,
    DISCORD_OAUTH_SCOPES,
    DISCORD_OAUTH_TOKEN_URL,
)
from app.features.auth.errors import (
    AuthConfigurationError,
    DiscordOAuthError,
    LinkedAccountFetchError,
    LinkedAccountNotFoundError,
)
from app.features.auth.schemas_v2 import STEAM_ID

logger = logging.getLogger(__name__)
DISCORD_HTTP_USER_AGENT = "CivPlayersAuth/1.0 (+https://elementalengine.net)"


def _oauth_error(body: str) -> object:
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    return parsed.get("error") if isinstance(parsed, dict) else None


def authorize_url(state_token: str) -> str:
    params = {
        "client_id": settings.auth_discord_client_id,
        "redirect_uri": settings.auth_discord_redirect_uri,
        "response_type": "code",
        "scope": " ".join(DISCORD_OAUTH_SCOPES),
        "prompt": "consent",
        "state": state_token,
    }
    return f"{DISCORD_OAUTH_AUTHORIZE_URL}?{urlencode(params)}"


class DiscordOAuthService:
    async def exchange_code(self, code: str) -> str:
        if (
            not settings.auth_discord_client_id
            or not settings.auth_discord_client_secret.get_secret_value()
            or not settings.auth_discord_redirect_uri
        ):
            raise AuthConfigurationError(
                "Discord OAuth is not configured for auth registration."
            )

        def _exchange() -> str:
            payload = urlencode(
                {
                    "client_id": settings.auth_discord_client_id,
                    "client_secret": settings.auth_discord_client_secret.get_secret_value(),
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.auth_discord_redirect_uri,
                }
            ).encode("utf-8")
            req = Request(
                DISCORD_OAUTH_TOKEN_URL,
                data=payload,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "User-Agent": DISCORD_HTTP_USER_AGENT,
                },
                method="POST",
            )
            try:
                with urlopen(req, timeout=settings.auth_oauth_timeout_seconds) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
            except HTTPError as exc:
                response_body = ""
                try:
                    response_body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    response_body = "<unavailable>"
                logger.warning(
                    "Discord OAuth token exchange failed with HTTP %s. redirect_uri=%s response=%s",
                    exc.code,
                    settings.auth_discord_redirect_uri,
                    response_body[:1000],
                )
                if exc.code in (400, 401) and _oauth_error(response_body) == (
                    "invalid_client"
                ):
                    # A wrong client id or secret: ours to fix, not the player's.
                    raise AuthConfigurationError(
                        "Discord OAuth is not configured for auth registration."
                    ) from exc
                raise DiscordOAuthError(
                    "Discord rejected the authentication callback. Please try again."
                ) from exc
            except (URLError, TimeoutError) as exc:
                logger.warning(
                    "Discord OAuth token exchange network failure. redirect_uri=%s error=%r",
                    settings.auth_discord_redirect_uri,
                    exc,
                )
                raise DiscordOAuthError(
                    "Discord authentication is temporarily unavailable. Please try again."
                ) from exc
            token = body.get("access_token")
            if not isinstance(token, str) or not token:
                logger.warning(
                    "Discord OAuth token exchange returned no access_token. redirect_uri=%s body=%s",
                    settings.auth_discord_redirect_uri,
                    json.dumps(body)[:1000],
                )
                raise DiscordOAuthError(
                    "Discord did not return a valid access token. Please try again."
                )
            return token

        return await asyncio.to_thread(_exchange)

    async def fetch_current_user(self, access_token: str) -> dict[str, Any]:
        payload = await self._get_json(DISCORD_API_ME_URL, access_token=access_token)
        if not isinstance(payload, dict):
            raise LinkedAccountFetchError()
        return payload

    async def fetch_connections(self, access_token: str) -> list[dict[str, Any]]:
        payload = await self._get_json(
            DISCORD_API_CONNECTIONS_URL, access_token=access_token
        )
        if not isinstance(payload, list):
            raise LinkedAccountFetchError()
        return [item for item in payload if isinstance(item, dict)]

    async def fetch_identity_and_steam(
        self, *, code: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        token = await self.exchange_code(code)
        user, connections = await asyncio.gather(
            self.fetch_current_user(token),
            self.fetch_connections(token),
        )
        return user, self._steam_connection(connections)

    @staticmethod
    def _steam_connection(connections: Iterable[dict[str, Any]]) -> dict[str, Any]:
        """The first Steam connection Discord has verified, holding a SteamID64."""
        for connection in connections:
            if (
                connection.get("type") == "steam"
                and connection.get("verified") is True
                and STEAM_ID.match(str(connection.get("id") or ""))
            ):
                return connection
        raise LinkedAccountNotFoundError()

    async def _get_json(
        self, url: str, *, access_token: str
    ) -> dict[str, Any] | list[Any]:
        def _request() -> dict[str, Any] | list[Any]:
            req = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": DISCORD_HTTP_USER_AGENT,
                },
                method="GET",
            )
            try:
                with urlopen(req, timeout=settings.auth_oauth_timeout_seconds) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except HTTPError as exc:
                response_body = ""
                try:
                    response_body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    response_body = "<unavailable>"
                logger.warning(
                    "Discord API request failed. url=%s status=%s response=%s",
                    url,
                    exc.code,
                    response_body[:1000],
                )
                raise LinkedAccountFetchError() from exc
            except (URLError, TimeoutError) as exc:
                logger.warning(
                    "Discord API network failure. url=%s error=%r",
                    url,
                    exc,
                )
                raise LinkedAccountFetchError() from exc

        return await asyncio.to_thread(_request)
