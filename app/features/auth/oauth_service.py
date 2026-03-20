from __future__ import annotations

import asyncio
import json
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.config import settings
from app.features.auth.constants import (
    DISCORD_API_CONNECTIONS_URL,
    DISCORD_API_ME_URL,
    DISCORD_OAUTH_TOKEN_URL,
)
from app.features.auth.enums import RegistrationPlatform
from app.features.auth.errors import (
    DiscordOAuthError,
    LinkedAccountFetchError,
    LinkedAccountNotFoundError,
)


class DiscordOAuthService:
    async def exchange_code(self, code: str) -> str:
        if (
            not settings.auth_discord_client_id
            or not settings.auth_discord_client_secret.get_secret_value()
            or not settings.auth_discord_redirect_uri
        ):
            raise DiscordOAuthError("Discord OAuth is not configured on the backend.")

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
                },
                method="POST",
            )
            try:
                with urlopen(req, timeout=settings.auth_oauth_timeout_seconds) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError) as exc:
                raise DiscordOAuthError() from exc
            token = body.get("access_token")
            if not isinstance(token, str) or not token:
                raise DiscordOAuthError()
            return token

        return await asyncio.to_thread(_exchange)

    async def fetch_current_user(self, access_token: str) -> dict[str, Any]:
        payload = await self._get_json(DISCORD_API_ME_URL, access_token=access_token)
        if not isinstance(payload, dict):
            raise LinkedAccountFetchError()
        return payload

    async def fetch_connections(self, access_token: str) -> list[dict[str, Any]]:
        payload = await self._get_json(DISCORD_API_CONNECTIONS_URL, access_token=access_token)
        if not isinstance(payload, list):
            raise LinkedAccountFetchError()
        return [item for item in payload if isinstance(item, dict)]

    async def fetch_identity_and_connection(
        self,
        *,
        code: str,
        platform: RegistrationPlatform,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        token = await self.exchange_code(code)
        user, connections = await asyncio.gather(
            self.fetch_current_user(token),
            self.fetch_connections(token),
        )
        return user, self._pick_connection(connections, platform)

    @staticmethod
    def _pick_connection(
        connections: Iterable[dict[str, Any]],
        platform: RegistrationPlatform,
    ) -> dict[str, Any]:
        expected = platform.discord_connection_type
        for connection in connections:
            if connection.get("type") == expected:
                return connection
        raise LinkedAccountNotFoundError(platform.value)

    async def _get_json(self, url: str, *, access_token: str) -> dict[str, Any] | list[Any]:
        def _request() -> dict[str, Any] | list[Any]:
            req = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {access_token}",
                },
                method="GET",
            )
            try:
                with urlopen(req, timeout=settings.auth_oauth_timeout_seconds) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError) as exc:
                raise LinkedAccountFetchError() from exc

        return await asyncio.to_thread(_request)
