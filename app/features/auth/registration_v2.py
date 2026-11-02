"""Registration for v2: a short-lived session for Discord sign-in, and the
player's record in `users` written as soon as the checks pass."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from secrets import token_urlsafe
from typing import Any, cast, get_args

from pymongo import ASCENDING, AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from app.core.config import settings
from app.core.constants import DB_SERVER_MEMBERS
from app.core.errors import ErrorDetail
from app.core.middleware import current_correlation_id
from app.features.auth.constants import COL_REGISTRATION_SESSIONS
from app.features.auth.enums import (
    STEAM_API_REGISTRATION_METHODS,
    RegistrationMethod,
    RegistrationPlatform,
    RoleIntent,
    SupportedGame,
)
from app.features.auth.errors import (
    AccountLookupNotFoundError,
    AlreadyRegisteredError,
    AuthConfigurationError,
    AuthError,
    DiscordSteamConflictError,
    DiscordUserMismatchError,
    InvalidStateError,
    LinkedAccountConflictError,
    RankRoleEligibilityError,
    RankRoleNeedsSteamError,
    SessionExpiredError,
    SessionNotFoundError,
    SessionNotValidatedError,
    SignInCancelledError,
    SteamIdConflictError,
)
from app.features.auth.oauth_service import DiscordOAuthService, authorize_url
from app.features.auth.repository import (
    AuthRepository,
    account_id_of,
    account_name_of,
    display_name_of,
    platform_of,
    username_of,
)
from app.features.auth.schemas_v2 import (
    AccountFlag,
    AccountLookup,
    AccountRecord,
    ManualRegistrationBody,
    Registration,
    RegistrationEntry,
    SessionStarted,
    SessionState,
    SessionStatus,
    Standing,
    TwoKRegistrationBody,
)
from app.features.auth.steam_service import SteamService
from app.features.infractions.models import SuspensionDocument
from app.features.infractions.repository import suspensions_col
from app.features.infractions.service import TIER_CAPS

logger = logging.getLogger(__name__)

DISCORD_EPOCH_MS = 1420070400000
OPEN = ("pending_auth", "validating", "validated", "completing")
SNAPSHOT = (
    "linked_account_id",
    "linked_account_name",
    "discord_username",
    "discord_display_name",
    "discord_locale",
    "discord_verified",
    "discord_mfa_enabled",
)
CLASHES = (SteamIdConflictError, LinkedAccountConflictError, DiscordSteamConflictError)
GAMES = {game.value for game in SupportedGame}
FLAGS = set(get_args(AccountFlag))


class SessionStore:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._col: AsyncCollection = client[DB_SERVER_MEMBERS][
            COL_REGISTRATION_SESSIONS
        ]

    async def ensure_indexes(self) -> None:
        await self._col.create_index(
            [("session_id", ASCENDING)], unique=True, name="registration_session_id_uq"
        )
        await self._col.create_index(
            [("state_token", ASCENDING)], unique=True, name="registration_state_uq"
        )
        await self._col.create_index(
            [("discord_user_id", ASCENDING)], name="registration_session_discord_idx"
        )
        # Mongo deletes a session about a minute after it expires.
        await self._col.create_index(
            [("expires_at", ASCENDING)],
            expireAfterSeconds=0,
            name="registration_session_ttl",
        )

    async def get(self, session_id: str) -> dict[str, Any] | None:
        return await self._col.find_one({"session_id": session_id})

    async def by_state(self, state_token: str) -> dict[str, Any] | None:
        return await self._col.find_one({"state_token": state_token})

    async def replace_for(self, discord_user_id: str, doc: dict[str, Any]) -> None:
        await self._col.delete_many({"discord_user_id": discord_user_id})
        await self._col.insert_one(doc)

    async def claim(
        self, match: dict[str, Any], held: str, taken: str, now: datetime
    ) -> dict[str, Any] | None:
        """Move a live session from one status to the next, once: a second
        caller finds nothing to claim."""
        return await self._col.find_one_and_update(
            {**match, "status": held, "expires_at": {"$gt": now}},
            {"$set": {"status": taken}},
            return_document=ReturnDocument.AFTER,
        )

    async def settle(self, session_id: str, held: str, changes: dict[str, Any]) -> bool:
        result = await self._col.update_one(
            {"session_id": session_id, "status": held}, {"$set": changes}
        )
        return result.matched_count == 1


def status_of(session: dict[str, Any], now: datetime) -> SessionStatus:
    """The stored status, or expired once an open session is past its time."""
    status = str(session["status"])
    expired = status in OPEN and now >= session["expires_at"]
    return cast(SessionStatus, "expired" if expired else status)


def discord_created(discord_id: str) -> datetime:
    return datetime.fromtimestamp(
        ((int(discord_id) >> 22) + DISCORD_EPOCH_MS) / 1000, UTC
    )


def account_flags(
    discord_id: str, now: datetime, *, steam: bool, steam_created: datetime | None
) -> list[AccountFlag]:
    """Young accounts are worth a look; so is a Steam account whose age Steam
    would not give."""
    flags: list[AccountFlag] = []
    if now - discord_created(discord_id) < timedelta(
        days=settings.auth_young_discord_days
    ):
        flags.append("young_discord")
    if steam and steam_created is None:
        flags.append("unknown_steam_age")
    elif steam_created and now - steam_created < timedelta(
        days=settings.auth_young_steam_days
    ):
        flags.append("young_steam")
    return flags


async def standings(
    client: AsyncMongoClient, discord_ids: list[str]
) -> dict[str, Standing]:
    """Each account's record with Lady Justice, for those that have one."""
    now = datetime.now(UTC)
    found: dict[str, Standing] = {}
    async for raw in suspensions_col(client).find({"discord_id": {"$in": discord_ids}}):
        doc = SuspensionDocument.model_validate(raw)
        tiers = {name: getattr(doc, name).tier for name in TIER_CAPS}
        found[doc.discord_id] = Standing(
            suspended=doc.suspended and (doc.ends is None or doc.ends > now),
            ends=doc.ends,
            active_category=doc.active_category,
            tiers={name: tier for name, tier in tiers.items() if tier},
            at_ban_threshold=any(tiers[name] >= cap for name, cap in TIER_CAPS.items()),
        )
    return found


def rank_role_for(game: SupportedGame) -> RoleIntent:
    return {
        SupportedGame.CIV6: RoleIntent.GRANT_CIV6_RANK,
        SupportedGame.CIV7: RoleIntent.GRANT_CIV7_RANK,
    }[game]


def role_intents(
    game: SupportedGame, platform: RegistrationPlatform
) -> list[RoleIntent]:
    return [
        rank_role_for(game),
        RoleIntent.GRANT_NOVICE,
        RoleIntent.GRANT_SERVER_NEWS,
        RoleIntent.GRANT_CIV6_NEWS
        if game is SupportedGame.CIV6
        else RoleIntent.GRANT_CIV7_NEWS,
        RoleIntent.GRANT_PC_STEAM
        if platform is RegistrationPlatform.STEAM
        else RoleIntent.GRANT_2K_CROSSPLATFORM,
        RoleIntent.REMOVE_NON_VERIFIED,
    ]


async def refuse_clash(
    users: AuthRepository,
    *,
    discord_user_id: str,
    platform: RegistrationPlatform,
    account_id: str,
    game: str,
) -> None:
    """Refuse an account another player holds, a player who already holds a
    different account, and a game the player already has."""
    by_discord = await users.get_user_by_discord_id(discord_user_id)
    by_linked = (
        await users.get_user_by_steam_id(account_id)
        if platform is RegistrationPlatform.STEAM
        else await users.get_user_by_linked_account(platform.value, account_id)
    )
    if by_linked and str(by_linked.get("discord_id")) != discord_user_id:
        holder = str(by_linked.get("discord_id", ""))
        if platform is RegistrationPlatform.STEAM:
            raise SteamIdConflictError(steam_id=account_id, existing_discord_id=holder)
        raise LinkedAccountConflictError(
            platform=platform.value, account_id=account_id, existing_discord_id=holder
        )
    if not by_discord:
        return
    held_platform = by_discord.get("linked_platform")
    held_account = by_discord.get("linked_account_id")
    held_steam = by_discord.get("steam_id")
    if (
        platform is RegistrationPlatform.STEAM
        and held_steam
        and str(held_steam) != account_id
    ):
        raise DiscordSteamConflictError(
            discord_user_id=discord_user_id, existing_steam_id=str(held_steam)
        )
    if (
        held_platform
        and held_account
        and (str(held_platform) != platform.value or str(held_account) != account_id)
    ):
        if str(held_platform) == RegistrationPlatform.STEAM.value:
            raise DiscordSteamConflictError(
                discord_user_id=discord_user_id, existing_steam_id=str(held_account)
            )
        raise LinkedAccountConflictError(
            platform=str(held_platform),
            account_id=str(held_account),
            existing_discord_id=discord_user_id,
            own=True,
        )
    if game in (by_discord.get("registrations") or {}):
        raise AlreadyRegisteredError(game)


def _text(value: object) -> str | None:
    return str(value) if value not in (None, "") else None


def _entries(doc: dict[str, Any]) -> list[RegistrationEntry]:
    """A record's registrations, tolerating what older writers stored."""
    entries = []
    for game, entry in (doc.get("registrations") or {}).items():
        if game not in GAMES or not isinstance(entry, dict):
            continue
        registered_at = entry.get("registered_at")
        entries.append(
            RegistrationEntry(
                game=SupportedGame(game),
                method=_text(entry.get("method")),
                registered_at=registered_at
                if isinstance(registered_at, datetime)
                else None,
                actor_discord_id=_text(entry.get("actor_discord_id")),
                reason=_text(entry.get("reason")),
                flags=[f for f in entry.get("flags") or [] if f in FLAGS],
            )
        )
    return sorted(entries, key=lambda e: e.game.value)


def _log_registration(
    discord_id: str,
    game: SupportedGame,
    method: RegistrationMethod,
    account_id: str,
    flags: list[AccountFlag],
    actor: str | None,
) -> None:
    logger.info(
        "Registered discord_id=%s game=%s method=%s account=%s flags=%s actor=%s",
        discord_id,
        game.value,
        method.value,
        account_id,
        flags,
        actor,
    )


class RegistrationFlow:
    def __init__(self, client: AsyncMongoClient) -> None:
        self._client = client
        self._users = AuthRepository(client)
        self._sessions = SessionStore(client)
        self._steam = SteamService()
        self._discord = DiscordOAuthService()

    async def start(self, discord_user_id: str, game: SupportedGame) -> SessionStarted:
        user = await self._users.get_user_by_discord_id(discord_user_id)
        if game.value in ((user or {}).get("registrations") or {}):
            raise AlreadyRegisteredError(game.value)
        if (
            not settings.auth_discord_client_id
            or not settings.auth_discord_client_secret.get_secret_value()
            or not settings.auth_discord_redirect_uri
        ):
            raise AuthConfigurationError(
                "Discord OAuth is not configured for auth registration."
            )
        now = datetime.now(UTC)
        session: dict[str, Any] = {
            "session_id": token_urlsafe(24),
            "state_token": token_urlsafe(32),
            "discord_user_id": discord_user_id,
            "game": game.value,
            "status": "pending_auth",
            "expires_at": now + timedelta(minutes=settings.auth_session_ttl_minutes),
            "created_at": now,
        }
        await self._sessions.replace_for(discord_user_id, session)
        return SessionStarted(
            session_id=session["session_id"],
            authorize_url=authorize_url(session["state_token"]),
            expires_at=session["expires_at"],
        )

    async def state(self, session_id: str) -> SessionState:
        session = await self._load(session_id)
        failure = session.get("failure")
        return SessionState(
            session_id=session_id,
            status=status_of(session, datetime.now(UTC)),
            expires_at=session["expires_at"],
            game=SupportedGame(session["game"]),
            failure=ErrorDetail(**failure) if failure else None,
            **{name: session.get(name) for name in SNAPSHOT},
        )

    async def callback(
        self, *, code: str | None, state: str, error: str | None
    ) -> None:
        """Discord sends the player's browser here after sign-in."""
        now = datetime.now(UTC)
        session = await self._sessions.claim(
            {"state_token": state}, "pending_auth", "validating", now
        )
        if session is None:
            known = await self._sessions.by_state(state)
            if known and status_of(known, now) == "expired":
                raise SessionExpiredError(str(known["session_id"]))
            raise InvalidStateError()
        session_id = str(session["session_id"])
        try:
            if error or not code:
                raise SignInCancelledError()
            user, connection = await self._discord.fetch_identity_and_steam(code=code)
            if str(user.get("id")) != session["discord_user_id"]:
                raise DiscordUserMismatchError(
                    session_user_id=session["discord_user_id"],
                    request_user_id=str(user.get("id")),
                )
            steam_id = str(connection.get("id") or "").strip()
            validation, created = await asyncio.gather(
                self._steam.validate_linked_account(
                    steam_id=steam_id, game=session["game"]
                ),
                self._steam.account_created(steam_id),
            )
        except AuthError as exc:
            await self._fail(session_id, exc.code, exc.message, exc.details)
            raise
        except Exception:
            await self._fail(
                session_id,
                "INTERNAL",
                "The auth service could not finish Discord verification. Please try again.",
                None,
            )
            raise
        username = user.get("username") or None
        linked_name = connection.get("name") or None
        settled = await self._sessions.settle(
            session_id,
            "validating",
            {
                "status": "validated",
                "linked_account_id": steam_id,
                "linked_account_name": linked_name,
                "discord_username": username,
                "discord_display_name": user.get("global_name") or username,
                "discord_locale": user.get("locale"),
                "discord_verified": user.get("verified"),
                "discord_mfa_enabled": user.get("mfa_enabled"),
                "playtime_minutes": validation["playtime_minutes"],
                "ownership_verified_at": validation["ownership_verified_at"],
                "steam_created_at": created,
            },
        )
        if not settled:
            raise InvalidStateError()

    async def complete(self, session_id: str, discord_user_id: str) -> Registration:
        """Register a signed-in player once; a repeat answers the same result."""
        now = datetime.now(UTC)
        session = await self._sessions.claim(
            {"session_id": session_id, "discord_user_id": discord_user_id},
            "validated",
            "completing",
            now,
        )
        if session is None:
            known = await self._load(session_id)
            if known["discord_user_id"] != discord_user_id:
                raise DiscordUserMismatchError(
                    session_user_id=known["discord_user_id"],
                    request_user_id=discord_user_id,
                )
            if known["status"] == "completed":
                return Registration(**known["registration"])
            status = status_of(known, now)
            if status == "expired":
                raise SessionExpiredError(session_id)
            raise SessionNotValidatedError(session_id, status)
        try:
            registration = await self._register(
                discord_user_id=discord_user_id,
                game=SupportedGame(session["game"]),
                platform=RegistrationPlatform.STEAM,
                method=RegistrationMethod.OAUTH_STEAM_API,
                account_id=session["linked_account_id"],
                account_name=session.get("linked_account_name"),
                username=session.get("discord_username"),
                display_name=session.get("discord_display_name"),
                steam_created=session.get("steam_created_at"),
                ownership_verified_at=session.get("ownership_verified_at"),
                playtime_minutes=session.get("playtime_minutes"),
            )
        except Exception:
            await self._sessions.settle(
                session_id, "completing", {"status": "validated"}
            )
            raise
        await self._sessions.settle(
            session_id,
            "completing",
            {
                "status": "completed",
                "registration": registration.model_dump(mode="json"),
            },
        )
        return registration

    async def rank_role(
        self, discord_user_id: str, game: SupportedGame
    ) -> Registration:
        """A registered, Steam-verified player adds the other game."""
        user = await self._users.get_user_by_discord_id(discord_user_id)
        if user is None:
            raise RankRoleEligibilityError(discord_user_id)
        registrations = user.get("registrations") or {}
        if game.value in registrations:
            raise AlreadyRegisteredError(game.value)
        steam_id = user.get("steam_id") or (
            user.get("linked_account_id")
            if user.get("linked_platform") == "steam"
            else None
        )
        verified = any(
            isinstance(entry, dict)
            and entry.get("method") in STEAM_API_REGISTRATION_METHODS
            for entry in registrations.values()
        )
        if not steam_id or not verified:
            raise RankRoleNeedsSteamError(discord_user_id)
        validation = await self._steam.validate_linked_account(
            steam_id=str(steam_id), game=game.value
        )
        name = user.get("linked_account_name") or user.get("steam_name")
        await self._users.upsert_registered_user(
            discord_user_id=discord_user_id,
            discord_username=user.get("discord_username") or user.get("user_name"),
            display_name=user.get("display_name"),
            linked_platform=RegistrationPlatform.STEAM.value,
            linked_account_id=str(steam_id),
            linked_account_name=name,
            game=game.value,
            method=RegistrationMethod.OAUTH_STEAM_API.value,
            ownership_verified_at=cast(datetime, validation["ownership_verified_at"]),
            playtime_minutes=cast(int, validation["playtime_minutes"]),
        )
        _log_registration(
            discord_user_id,
            game,
            RegistrationMethod.OAUTH_STEAM_API,
            str(steam_id),
            [],
            None,
        )
        return Registration(
            discord_user_id=discord_user_id,
            game=game,
            linked_platform=RegistrationPlatform.STEAM,
            linked_account_id=str(steam_id),
            linked_account_name=name,
            registration_method=RegistrationMethod.OAUTH_STEAM_API,
            role_intents=[rank_role_for(game)],
        )

    async def manual(
        self, actor_discord_id: str, body: ManualRegistrationBody
    ) -> Registration:
        platform, method = body.platform.resolved()
        extra: dict[str, Any] = {"actor_discord_id": actor_discord_id}
        if body.reason:
            extra["reason"] = body.reason
        return await self._register(
            discord_user_id=body.subject_discord_id,
            game=body.game,
            platform=platform,
            method=method,
            account_id=body.platform_account_id,
            account_name=body.platform_account_name,
            username=body.discord_username,
            display_name=body.discord_display_name,
            extra=extra,
        )

    async def two_k(self, body: TwoKRegistrationBody) -> Registration:
        return await self._register(
            discord_user_id=body.discord_user_id,
            game=SupportedGame.CIV7,
            platform=RegistrationPlatform.TWOK,
            method=RegistrationMethod.SELF_SERVICE_2K,
            account_id=body.platform_account_id,
            account_name=body.platform_account_name,
            username=body.discord_username,
            display_name=body.discord_display_name,
        )

    async def by_discord(self, discord_id: str) -> AccountLookup:
        docs = await self._users.find_users_by_discord_id(discord_id)
        if not docs:
            raise AccountLookupNotFoundError(field="discord_id", value=discord_id)
        return await self._lookup(docs)

    async def by_linked(self, linked_account_id: str) -> AccountLookup:
        docs = await self._users.find_users_by_linked_account_id(linked_account_id)
        if not docs:
            raise AccountLookupNotFoundError(
                field="linked_account_id", value=linked_account_id
            )
        return await self._lookup(docs)

    async def _lookup(self, docs: list[dict[str, Any]]) -> AccountLookup:
        docs = [doc for doc in docs if doc.get("discord_id")]
        ids = [str(doc["discord_id"]) for doc in docs]
        try:
            standing_of = await standings(self._client, ids)
        except Exception:
            logger.exception("Could not read standings for discord_ids=%s", ids)
            standing_of = {}
        return AccountLookup(
            records=[
                AccountRecord(
                    discord_id=discord_id,
                    discord_username=username_of(doc),
                    discord_display_name=display_name_of(doc),
                    linked_platform=platform_of(doc),
                    linked_account_id=account_id_of(doc),
                    linked_account_name=account_name_of(doc),
                    registrations=_entries(doc),
                    standing=standing_of.get(discord_id),
                )
                for discord_id, doc in zip(ids, docs, strict=True)
            ]
        )

    async def _load(self, session_id: str) -> dict[str, Any]:
        session = await self._sessions.get(session_id)
        if session is None:
            raise SessionNotFoundError(session_id)
        return session

    async def _fail(
        self, session_id: str, code: str, message: str, details: Any
    ) -> None:
        await self._sessions.settle(
            session_id,
            "validating",
            {
                "status": "failed",
                "failure": {
                    "code": code,
                    "message": message,
                    "details": details,
                    "correlation_id": current_correlation_id.get(),
                },
            },
        )

    async def _register(
        self,
        *,
        discord_user_id: str,
        game: SupportedGame,
        platform: RegistrationPlatform,
        method: RegistrationMethod,
        account_id: str,
        account_name: str | None,
        username: str | None,
        display_name: str | None,
        steam_created: datetime | None = None,
        ownership_verified_at: datetime | None = None,
        playtime_minutes: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> Registration:
        """Refuse a clash, naming the other account's standing; else write the
        player's record."""
        try:
            await refuse_clash(
                self._users,
                discord_user_id=discord_user_id,
                platform=platform,
                account_id=account_id,
                game=game.value,
            )
        except CLASHES as exc:
            await self._name_standing(
                cast(dict[str, Any], exc.details), discord_user_id
            )
            raise
        flags = account_flags(
            discord_user_id,
            datetime.now(UTC),
            steam=method.value in STEAM_API_REGISTRATION_METHODS,
            steam_created=steam_created,
        )
        extra = {**(extra or {}), **({"flags": flags} if flags else {})}
        await self._users.upsert_registered_user(
            discord_user_id=discord_user_id,
            discord_username=username,
            display_name=display_name,
            linked_platform=platform.value,
            linked_account_id=account_id,
            linked_account_name=account_name,
            game=game.value,
            method=method.value,
            ownership_verified_at=ownership_verified_at,
            playtime_minutes=playtime_minutes,
            entry_extra=extra,
        )
        _log_registration(
            discord_user_id,
            game,
            method,
            account_id,
            flags,
            extra.get("actor_discord_id"),
        )
        return Registration(
            discord_user_id=discord_user_id,
            game=game,
            linked_platform=platform,
            linked_account_id=account_id,
            linked_account_name=account_name,
            registration_method=method,
            role_intents=role_intents(game, platform),
            flags=flags,
        )

    async def _name_standing(self, details: dict[str, Any], caller: str) -> None:
        """Add the other account's standing to a clash; the refusal stands even
        if Lady Justice's record cannot be read."""
        other = details.get("existing_discord_id")
        if not other or other == caller:
            return
        try:
            found = (await standings(self._client, [other])).get(other)
            details["standing"] = found.model_dump() if found else None
        except Exception:
            logger.exception("Could not read the standing of discord_id=%s", other)
