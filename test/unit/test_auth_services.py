import asyncio
import io
from urllib.error import HTTPError

import pytest
from pydantic import SecretStr

from app.features.auth import oauth_service
from app.features.auth.errors import AuthError, LinkedAccountNotFoundError
from app.features.auth.oauth_service import DiscordOAuthService


def test_oauth_service_picks_the_steam_connection():
    picked = DiscordOAuthService._steam_connection(
        [
            {"id": "2", "type": "epicgames", "name": "epic-user"},
            {"id": "1", "type": "steam", "name": "steam-user"},
        ]
    )
    assert picked["id"] == "1"


def test_oauth_service_raises_when_steam_is_not_linked():
    with pytest.raises(LinkedAccountNotFoundError) as caught:
        DiscordOAuthService._steam_connection([{"id": "2", "type": "xbox"}])
    assert caught.value.details == {"platform": "steam"}


@pytest.mark.parametrize("missing", ["client_id", "client_secret", "redirect_uri"])
def test_discord_oauth_set_up_wrong_is_misconfigured_not_a_discord_failure(
    monkeypatch, missing
):
    configured = {
        "auth_discord_client_id": "client",
        "auth_discord_client_secret": SecretStr("secret"),
        "auth_discord_redirect_uri": "https://x/cb",
    }
    configured[f"auth_discord_{missing}"] = (
        SecretStr("") if missing == "client_secret" else ""
    )
    for name, value in configured.items():
        monkeypatch.setattr(oauth_service.settings, name, value)

    with pytest.raises(AuthError) as caught:
        asyncio.run(DiscordOAuthService().exchange_code("code"))
    error = caught.value
    assert (error.code, error.status_code, error.retryable) == (
        "AUTH_MISCONFIGURED",
        503,
        False,
    )


@pytest.mark.parametrize(
    ("status", "error", "code", "retryable"),
    [
        (401, "invalid_client", "AUTH_MISCONFIGURED", False),
        (400, "invalid_client", "AUTH_MISCONFIGURED", False),
        (400, "invalid_grant", "DISCORD_OAUTH_FAILED", True),
    ],
)
def test_a_wrong_client_secret_is_misconfigured_and_a_bad_code_is_not(
    monkeypatch, status, error, code, retryable
):
    monkeypatch.setattr(oauth_service.settings, "auth_discord_client_id", "client")
    monkeypatch.setattr(
        oauth_service.settings, "auth_discord_client_secret", SecretStr("wrong")
    )
    monkeypatch.setattr(
        oauth_service.settings, "auth_discord_redirect_uri", "https://x/cb"
    )

    def urlopen(req, timeout):
        body = io.BytesIO(f'{{"error": "{error}"}}'.encode())
        raise HTTPError(req.full_url, status, "no", {}, body)

    monkeypatch.setattr(oauth_service, "urlopen", urlopen)
    with pytest.raises(AuthError) as caught:
        asyncio.run(DiscordOAuthService().exchange_code("code"))
    assert (caught.value.code, caught.value.retryable) == (code, retryable)
