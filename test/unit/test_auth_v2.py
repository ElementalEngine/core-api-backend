import asyncio
import io
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core import dependencies
from app.features.auth import registration_v2, steam_service
from app.features.auth.enums import ManualRegistrationChoice, SupportedGame
from app.features.auth.errors import (
    AuthConfigurationError,
    AuthError,
    InvalidStateError,
    LinkedAccountConflictError,
    RankRoleNeedsSteamError,
    SessionNotValidatedError,
    SteamIdConflictError,
    SteamProfilePrivateError,
    SteamValidationError,
)
from app.features.auth.registration_v2 import (
    DISCORD_EPOCH_MS,
    RegistrationFlow,
    account_flags,
    status_of,
)
from app.features.auth.router_v2 import _flow
from app.features.auth.schemas_v2 import (
    ManualRegistrationBody,
    Registration,
    SessionStart,
)
from app.features.auth.steam_service import SteamService
from app.main import app

PLAYER = "123456789012345678"
OTHER = "223456789012345678"
STAFF = "999999999999999999"
STEAM = "76561198000000001"
NOW = datetime.now(UTC)


def snowflake(created: datetime) -> str:
    return str((int(created.timestamp() * 1000) - DISCORD_EPOCH_MS) << 22)


class Collection:
    """Equality filters, `$gt`, and `$set`, yielding between calls like Mongo."""

    def __init__(self):
        self.docs = []

    def _match(self, flt):
        def ok(value, want):
            if isinstance(want, dict):
                return value in want["$in"] if "$in" in want else value > want["$gt"]
            return value == want

        return [d for d in self.docs if all(ok(d.get(k), v) for k, v in flt.items())]

    async def find(self, flt):
        for doc in self._match(flt):
            await asyncio.sleep(0)
            yield dict(doc)

    async def find_one(self, flt, projection=None):
        await asyncio.sleep(0)
        found = self._match(flt)
        return dict(found[0]) if found else None

    async def find_one_and_update(self, flt, update, return_document=None):
        await asyncio.sleep(0)
        for doc in self._match(flt)[:1]:
            doc.update(update["$set"])
            return dict(doc)
        return None

    async def insert_one(self, doc):
        self.docs.append(dict(doc))

    async def update_one(self, flt, update):
        await asyncio.sleep(0)
        found = self._match(flt)[:1]
        for doc in found:
            doc.update(update["$set"])
        return SimpleNamespace(matched_count=len(found))

    async def delete_many(self, flt):
        for doc in self._match(flt):
            self.docs.remove(doc)


class Users:
    def __init__(self, by_discord=None, by_steam=None):
        self.by_discord = by_discord or {}
        self.by_steam = by_steam or {}
        self.upserts = []

    async def find_users_by_discord_id(self, discord_id):
        found = self.by_discord.get(discord_id)
        return [found] if found else []

    async def find_users_by_linked_account_id(self, account_id):
        return list(self.by_steam.get(account_id) or [])

    async def get_user_by_discord_id(self, discord_id):
        return self.by_discord.get(discord_id)

    async def get_user_by_steam_id(self, steam_id):
        return self.by_steam.get(steam_id)

    async def get_user_by_linked_account(self, platform, account_id):
        return None

    async def upsert_registered_user(self, **kwargs):
        await asyncio.sleep(0)
        self.upserts.append(kwargs)


class Discord:
    async def fetch_identity_and_steam(self, *, code):
        await asyncio.sleep(0)
        return {"id": PLAYER, "username": "player"}, {"id": STEAM, "name": "Steamy"}


class Steam:
    def __init__(self, error=None):
        self.error = error

    async def validate_linked_account(self, *, steam_id, game):
        if self.error:
            raise self.error
        return {"playtime_minutes": 4000, "ownership_verified_at": NOW}

    async def account_created(self, steam_id):
        return NOW - timedelta(days=3)


def flow(users=None, steam=None):
    client = defaultdict(lambda: defaultdict(Collection))
    f = RegistrationFlow(client)
    f._users = users or Users()
    f._steam = steam or Steam()
    f._discord = Discord()
    return f, client["server_members"]


@pytest.fixture(autouse=True)
def oauth_configured(monkeypatch):
    monkeypatch.setattr(registration_v2.settings, "auth_discord_client_id", "client")
    monkeypatch.setattr(
        registration_v2.settings, "auth_discord_redirect_uri", "https://x/cb"
    )
    monkeypatch.setattr(
        registration_v2.settings, "auth_discord_client_secret", SecretStr("secret")
    )


def run(coro):
    return asyncio.run(coro)


def signed_in(f, members):
    started = run(f.start(PLAYER, SupportedGame.CIV6))
    state = members["registration_sessions"].docs[0]["state_token"]
    run(f.callback(code="code", state=state, error=None))
    return started.session_id


def test_a_session_runs_from_sign_in_to_a_registered_player():
    f, members = flow()
    run(f.start(PLAYER, SupportedGame.CIV6))
    session_id = signed_in(f, members)
    sessions = members["registration_sessions"].docs
    assert len(sessions) == 1  # a new start replaces the player's earlier one
    assert run(f.state(session_id)).status == "validated"

    result = run(f.complete(session_id, PLAYER))
    written = f._users.upserts[0]
    assert written["playtime_minutes"] == 4000
    assert written["entry_extra"] == {"flags": ["young_steam"]}
    assert result.flags == ["young_steam"]
    assert len(result.role_intents) == 6


def test_a_repeated_complete_writes_once_and_answers_the_same_result():
    f, members = flow()
    session_id = signed_in(f, members)

    async def both():
        return await asyncio.gather(
            f.complete(session_id, PLAYER),
            f.complete(session_id, PLAYER),
            return_exceptions=True,
        )

    first, second = run(both())
    assert len(f._users.upserts) == 1
    assert isinstance(second, SessionNotValidatedError)  # still being written
    assert run(f.complete(session_id, PLAYER)) == first  # a retry after it lands
    assert len(f._users.upserts) == 1


def test_a_second_callback_for_one_sign_in_is_refused_and_changes_nothing():
    f, members = flow()
    run(f.start(PLAYER, SupportedGame.CIV6))
    state = members["registration_sessions"].docs[0]["state_token"]

    async def both():
        return await asyncio.gather(
            f.callback(code="code", state=state, error=None),
            f.callback(code="code", state=state, error=None),
            return_exceptions=True,
        )

    first, second = run(both())
    assert isinstance(second, InvalidStateError)
    session = members["registration_sessions"].docs[0]
    assert (session["status"], session.get("failure")) == ("validated", None)


def test_a_failed_sign_in_is_kept_on_the_session_for_the_bot():
    f, members = flow(steam=Steam(error=SteamProfilePrivateError(game="civ6")))
    with pytest.raises(SteamProfilePrivateError):
        signed_in(f, members)
    session_id = members["registration_sessions"].docs[0]["session_id"]
    shown = run(f.state(session_id))
    assert shown.status == "failed"
    assert shown.failure.code == "STEAM_PROFILE_PRIVATE"


def test_an_open_session_past_its_time_reads_expired():
    past = {"status": "validated", "expires_at": NOW - timedelta(seconds=1)}
    assert status_of(past, NOW) == "expired"
    assert status_of({**past, "status": "completed"}, NOW) == "completed"


def manual(account=STEAM, platform=ManualRegistrationChoice.STEAM, **more):
    return ManualRegistrationBody(
        subject_discord_id=PLAYER,
        game=SupportedGame.CIV6,
        platform=platform,
        platform_account_id=account,
        **more,
    )


def test_a_clash_names_the_other_account_and_its_standing():
    f, members = flow(users=Users(by_steam={STEAM: {"discord_id": OTHER}}))
    members["suspensions"].docs.append(
        {"discord_id": OTHER, "suspended": False, "major": {"tier": 4}}
    )
    with pytest.raises(SteamIdConflictError) as caught:
        run(f.manual(STAFF, manual()))
    assert caught.value.details["existing_discord_id"] == OTHER
    assert caught.value.details["standing"]["at_ban_threshold"] is True
    assert caught.value.details["standing"]["tiers"] == {"major": 4}


def test_a_clash_with_the_player_s_own_record_names_no_standing():
    # The player already holds a 2K link, so the clash names their own id.
    own = {"discord_id": PLAYER, "linked_platform": "2k", "linked_account_id": "2k-id"}
    f, members = flow(users=Users(by_discord={PLAYER: own}))
    members["suspensions"].docs.append({"discord_id": PLAYER, "major": {"tier": 4}})
    with pytest.raises(LinkedAccountConflictError) as caught:
        run(f.manual(STAFF, manual()))
    assert caught.value.details["existing_discord_id"] == PLAYER
    assert "standing" not in caught.value.details
    assert caught.value.code == "LINKED_ACCOUNT_CONFLICT"
    assert caught.value.message.startswith(
        "This Discord account already has a different 2K account linked."
    )


def test_young_accounts_are_flagged_and_old_ones_are_not():
    young = snowflake(NOW - timedelta(days=2))
    old = snowflake(NOW - timedelta(days=400))
    week_old = NOW - timedelta(days=5)
    assert account_flags(young, NOW, steam=True, steam_created=week_old) == [
        "young_discord",
        "young_steam",
    ]
    year_old = NOW - timedelta(days=400)
    assert account_flags(old, NOW, steam=True, steam_created=year_old) == []
    assert account_flags(old, NOW, steam=False, steam_created=None) == []
    # Steam would not say how old the account is: staff should look.
    assert account_flags(old, NOW, steam=True, steam_created=None) == [
        "unknown_steam_age"
    ]


def test_a_rank_role_keeps_the_player_names():
    # v1 wrote a rank role without them, which unset the player's names.
    user = {
        "discord_id": PLAYER,
        "discord_username": "player",
        "display_name": "Player",
        "linked_platform": "steam",
        "linked_account_id": STEAM,
        "linked_account_name": "Steamy",
        "registrations": {"civ6": {"method": "oauth_steam_api"}},
    }
    f, _ = flow(users=Users(by_discord={PLAYER: user}))
    result = run(f.rank_role(PLAYER, SupportedGame.CIV7))
    written = f._users.upserts[0]
    assert (written["discord_username"], written["display_name"]) == (
        "player",
        "Player",
    )
    assert written["linked_account_name"] == "Steamy"
    assert [i.value for i in result.role_intents] == ["grant_civ7_rank"]


def test_a_rank_role_refuses_a_registration_not_verified_by_steam():
    user = {
        "discord_id": PLAYER,
        "linked_platform": "2k",
        "linked_account_id": "2k-id",
        "registrations": {"civ7": {"method": "self_service_2k"}},
    }
    f, _ = flow(users=Users(by_discord={PLAYER: user}))
    with pytest.raises(RankRoleNeedsSteamError):
        run(f.rank_role(PLAYER, SupportedGame.CIV6))


def test_a_manual_registration_records_who_and_why():
    f, _ = flow()
    body = manual(
        platform=ManualRegistrationChoice.STEAM_FAMILY_SHARE, reason="family share"
    )
    body.subject_discord_id = snowflake(NOW - timedelta(days=400))
    result = run(f.manual(STAFF, body))
    assert f._users.upserts[0]["entry_extra"] == {
        "actor_discord_id": STAFF,
        "reason": "family share",
    }
    assert result.registration_method.value == "admin_steam_family_share"


def test_a_steam_link_by_hand_must_be_a_steam_id():
    with pytest.raises(ValueError):
        manual(account="12345")
    two_k = manual(account="any-2k-id", platform=ManualRegistrationChoice.TWOK)
    assert two_k.platform_account_id == "any-2k-id"


def test_a_lookup_shows_who_registered_by_hand_the_flags_and_the_standing():
    user = {
        "discord_id": PLAYER,
        "linked_platform": "steam",
        "linked_account_id": STEAM,
        "registrations": {
            "civ6": {
                "method": "admin_staff_attested",
                "actor_discord_id": STAFF,
                "reason": "verified in voice",
                "flags": ["young_discord"],
            },
            "civ5": {"method": "old"},
        },
    }
    f, members = flow(users=Users(by_discord={PLAYER: user}))
    members["suspensions"].docs.append(
        {"discord_id": PLAYER, "suspended": True, "quit": {"tier": 6}}
    )
    record = run(f.by_discord(PLAYER)).records[0]
    entry = record.registrations[0]
    assert [e.game.value for e in record.registrations] == ["civ6"]
    assert (entry.actor_discord_id, entry.reason) == (STAFF, "verified in voice")
    assert entry.flags == ["young_discord"]
    assert record.standing.at_ban_threshold is True
    assert record.standing.suspended is True


def test_a_sign_in_that_fails_on_a_bug_keeps_the_answer_s_correlation_id(
    monkeypatch,
):
    client_db = defaultdict(lambda: defaultdict(Collection))
    client_db["server_members"]["registration_sessions"].docs.append(
        {
            "session_id": "s1",
            "state_token": "the-state",
            "discord_user_id": PLAYER,
            "game": "civ6",
            "status": "pending_auth",
            "expires_at": NOW + timedelta(minutes=5),
        }
    )

    class Broken:
        async def fetch_identity_and_steam(self, *, code):
            raise RuntimeError("bug")

    monkeypatch.setattr(registration_v2, "DiscordOAuthService", Broken)
    app.dependency_overrides[dependencies.get_database] = lambda: client_db
    try:
        answer = TestClient(app, raise_server_exceptions=False).get(
            "/oauth/discord/callback", params={"code": "c", "state": "the-state"}
        )
    finally:
        app.dependency_overrides.clear()
    stored = client_db["server_members"]["registration_sessions"].docs[0]["failure"]
    assert (answer.status_code, stored["code"]) == (500, "INTERNAL")
    assert answer.headers["content-type"].startswith("text/html")
    assert f"Reference: {stored['correlation_id']}" in answer.text


def test_a_linked_lookup_returns_every_holder_and_tolerates_old_entries():
    holders = [
        {
            "discord_id": PLAYER,
            "steam_id": STEAM,
            "registrations": {"civ6": {"method": "oauth"}},
        },
        {
            "discord_id": OTHER,
            "linked_platform": "steam",
            "linked_account_id": STEAM,
            "registrations": {
                "civ7": {
                    "method": 5,
                    "actor_discord_id": 999,
                    "flags": ["retired", "young_steam"],
                }
            },
        },
        {"steam_id": STEAM},
    ]
    f, _ = flow(users=Users(by_steam={STEAM: holders}))
    records = run(f.by_linked(STEAM)).records
    assert [r.discord_id for r in records] == [PLAYER, OTHER]
    entry = records[1].registrations[0]
    assert (entry.method, entry.actor_discord_id, entry.flags) == (
        "5",
        "999",
        ["young_steam"],
    )


def test_a_discord_id_must_be_ascii_digits():
    with pytest.raises(ValueError):
        SessionStart(discord_user_id="١" * 18, game="civ6")


def test_v2_answers_a_refusal_a_bug_and_a_bad_actor_in_the_envelope(monkeypatch):
    monkeypatch.setattr(
        dependencies.settings, "auth_service_token", SecretStr("auth-token")
    )

    class Failing:
        async def manual(self, actor, body):
            return Registration(
                discord_user_id=body.subject_discord_id,
                game=body.game,
                linked_platform="steam",
                linked_account_id=body.platform_account_id,
                registration_method="admin_staff_attested",
                role_intents=[],
            )

        async def state(self, session_id):
            if session_id == "refused":
                raise AuthError(code="X_REFUSED", message="no", status_code=409)
            raise RuntimeError("bug")

    app.dependency_overrides[_flow] = Failing
    try:
        client = TestClient(app, raise_server_exceptions=False)
        token = {"Authorization": "Bearer auth-token"}
        refused = client.get("/api/v2/auth/sessions/refused", headers=token)
        bug = client.get("/api/v2/auth/sessions/other", headers=token)
        denied = client.get("/api/v2/auth/sessions/refused")
        body = json.loads(manual().model_dump_json())
        bad_actor = client.post(
            "/api/v2/auth/manual-registrations",
            headers={
                **token,
                "X-Actor-Discord-Id": "staff",
                "X-Actor-Is-Staff": "true",
            },
            json=body,
        )
        not_staff = client.post(
            "/api/v2/auth/manual-registrations",
            headers={**token, "X-Actor-Discord-Id": STAFF},
            json={"not": "checked before the staff header"},
        )
        staff = client.post(
            "/api/v2/auth/manual-registrations",
            headers={**token, "X-Actor-Discord-Id": STAFF, "X-Actor-Is-Staff": "true"},
            json=body,
        )
    finally:
        app.dependency_overrides.clear()
    error = refused.json()["detail"]["error"]
    assert (refused.status_code, error["code"]) == (409, "X_REFUSED")
    assert error["correlation_id"]
    assert (bug.status_code, bug.json()["detail"]["error"]["code"]) == (500, "INTERNAL")
    assert denied.status_code == 401
    assert bad_actor.status_code == 422
    assert not_staff.status_code == 403
    assert not_staff.json()["detail"]["error"]["correlation_id"]
    assert (staff.status_code, staff.json()["linked_account_id"]) == (200, STEAM)


def test_steam_reads_account_age_and_its_failures_are_steam_failures(monkeypatch):
    monkeypatch.setattr(
        steam_service.settings, "auth_steam_api_key", SecretStr("steam-key")
    )
    answers = {}

    def urlopen(req, timeout):
        answers["url"] = req.full_url
        if "GetOwnedGames" in req.full_url:
            raise HTTPError(req.full_url, 500, "down", {}, io.BytesIO(b"oops"))
        body = {"response": {"players": [{"timecreated": 1_600_000_000}]}}
        return io.BytesIO(json.dumps(body).encode())

    monkeypatch.setattr(steam_service, "urlopen", urlopen)
    steam = SteamService()
    created = run(steam.account_created(STEAM))
    assert created == datetime.fromtimestamp(1_600_000_000, UTC)
    assert f"steamids={STEAM}" in answers["url"]
    with pytest.raises(SteamValidationError):
        run(steam.validate_linked_account(steam_id=STEAM, game="civ6"))

    async def odd(url, params):
        return {"response": {"players": ["not a player"]}}

    monkeypatch.setattr(steam, "_get_json", odd)
    assert run(steam.account_created(STEAM)) is None


# Every code v2 can answer with. The bot words each one, so a new code must be
# added here and to the bot together.
V2_CODES = {
    "ACCOUNT_NOT_FOUND",
    "ALREADY_REGISTERED",
    "AUTH_MISCONFIGURED",
    "DISCORD_ID_CONFLICT",
    "DISCORD_LINKED_ACCOUNT_FETCH_FAILED",
    "DISCORD_LINKED_ACCOUNT_NOT_FOUND",
    "DISCORD_OAUTH_FAILED",
    "DISCORD_USER_MISMATCH",
    "FORBIDDEN",
    "INVALID_AUTH_STATE",
    "LINKED_ACCOUNT_CONFLICT",
    "RANK_ROLE_NEEDS_STEAM",
    "RANK_ROLE_NOT_ELIGIBLE",
    "REGISTRATION_SESSION_EXPIRED",
    "REGISTRATION_SESSION_NOT_FOUND",
    "REGISTRATION_SESSION_NOT_READY",
    "STEAM_API_FAILURE",
    "STEAM_ID_CONFLICT",
    "STEAM_OWNERSHIP_MISSING",
    "STEAM_PLAYTIME_BELOW_THRESHOLD",
    "STEAM_PROFILE_PRIVATE",
}
# Answered by the app around the routes, not by an auth error.
TRANSPORT_CODES = {
    "AUTH_SERVICE_MISCONFIGURED",
    "INTERNAL",
    "INVALID_REQUEST",
    "UNAUTHORIZED",
    "UNAVAILABLE",
}


def test_v2_answers_only_its_pinned_codes():
    import ast
    from pathlib import Path

    auth = Path(__file__).resolve().parents[2] / "app" / "features" / "auth"
    codes = {}
    for node in ast.parse((auth / "errors.py").read_text()).body:
        if isinstance(node, ast.ClassDef):
            for call in ast.walk(node):
                for kw in getattr(call, "keywords", []):
                    if kw.arg == "code" and isinstance(kw.value, ast.Constant):
                        codes[node.name] = kw.value.value

    def raised(tree):
        found = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Raise) and n.exc is not None:
                # Only `raise Error(...)` can be read; anything else fails here.
                assert isinstance(n.exc, ast.Call), ast.unparse(n)
                assert isinstance(n.exc.func, ast.Name), ast.unparse(n)
                found.add(n.exc.func.id)
        return found

    names = set()
    for module in ("registration_v2", "router_v2", "steam_service", "oauth_service"):
        names |= raised(ast.parse((auth / f"{module}.py").read_text()))
    assert {codes[n] for n in names if n in codes} == V2_CODES
    core = Path(__file__).resolve().parents[2] / "app" / "core"
    literals = {
        n.value
        for f in ("dependencies.py", "errors.py")
        for n in ast.walk(ast.parse((core / f).read_text()))
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    }
    assert TRANSPORT_CODES <= literals
    assert all(n in codes for n in names), sorted(n for n in names if n not in codes)


def test_a_session_does_not_start_while_the_client_secret_is_unset(monkeypatch):
    monkeypatch.setattr(
        registration_v2.settings, "auth_discord_client_secret", SecretStr("")
    )
    f, members = flow()
    with pytest.raises(AuthConfigurationError) as caught:
        run(f.start(PLAYER, SupportedGame.CIV6))
    assert caught.value.code == "AUTH_MISCONFIGURED"
    assert members["registration_sessions"].docs == []


def test_a_player_who_cancels_on_discord_is_told_plainly_not_to_retry():
    f, members = flow()
    run(f.start(PLAYER, SupportedGame.CIV6))
    state = members["registration_sessions"].docs[0]["state_token"]
    with pytest.raises(AuthError) as caught:
        run(f.callback(code=None, state=state, error="access_denied"))
    error = caught.value
    assert (error.code, error.status_code, error.retryable) == (
        "DISCORD_OAUTH_FAILED",
        400,
        False,
    )
    assert members["registration_sessions"].docs[0]["status"] == "failed"
