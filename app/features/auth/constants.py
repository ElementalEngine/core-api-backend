from __future__ import annotations

from typing import Final

AUTH_DB_NAME: Final[str] = "auth"
COL_REGISTRATION_SESSIONS: Final[str] = "registration_sessions"
COL_REGISTRATION_OPERATIONS: Final[str] = "registration_operations"

DISCORD_OAUTH_AUTHORIZE_URL: Final[str] = "https://discord.com/oauth2/authorize"
DISCORD_OAUTH_TOKEN_URL: Final[str] = "https://discord.com/api/oauth2/token"
DISCORD_API_ME_URL: Final[str] = "https://discord.com/api/v10/users/@me"
DISCORD_API_CONNECTIONS_URL: Final[str] = "https://discord.com/api/v10/users/@me/connections"
DISCORD_OAUTH_SCOPES: Final[tuple[str, ...]] = ("identify", "connections")

DEFAULT_STEAM_APP_IDS: Final[dict[str, int]] = {
    "civ6": 289070,
    "civ7": 1295660,
}

DEFAULT_STEAM_PLAYTIME_MINUTES: Final[dict[str, int]] = {
    "civ6": 2880,
    "civ7": 120,
}

STEAM_OWNED_GAMES_URL: Final[str] = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"
