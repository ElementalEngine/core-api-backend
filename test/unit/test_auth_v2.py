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
from app.features.auth import registration_v2, router as auth_router, steam_service
from app.features.auth.enums import ManualRegistrationChoice, SupportedGame
from app.features.auth.errors import (
    AuthError,
    InvalidStateError,
    LinkedAccountConflictError,
    RankRoleNeedsSteamError,
    SessionNotValidatedError,
    SteamIdConflictError,
    SteamProfilePrivateError,
    SteamValidationError,
)
from app.features.auth.registration_service import RegistrationService
from app.features.auth.registration_v2 import (
    DISCORD_EPOCH_MS,
    RegistrationFlow,
    account_flags,
    status_of,
)
from app.features.auth.router_v2 import _flow
from app.features.auth.schemas_v2 import ManualRegistrationBody, SessionStart
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
            return value > want["$gt"] if isinstance(want, dict) else value == want

        return [d for d in self.docs if all(ok(d.get(k), v) for k, v in flt.items())]

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
    async def fetch_identity_and_connection(self, *, code, platform):
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
    f._registrations = RegistrationService(f._users)
    f._steam = steam or Steam()
    f._discord = Discord()
    return f, client["server_members"]


@pytest.fixture(autouse=True)
def oauth_configured(monkeypatch):
    monkeypatch.setattr(registration_v2.settings, "auth_discord_client_id", "client")
    monkeypatch.setattr(
        registration_v2.settings, "auth_discord_redirect_uri", "https://x/cb"
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
    assert sessions[0]["state_token"].startswith(registration_v2.STATE_PREFIX)
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


def test_the_shared_callback_hands_a_v2_state_to_the_v2_flow(monkeypatch):
    seen = {}

    class Flow:
        def __init__(self, db):
            pass

        async def callback(self, *, code, state, error):
            seen["state"] = state
            return "v2"

    monkeypatch.setattr(auth_router, "RegistrationFlow", Flow)
    answer = run(
        auth_router.discord_oauth_callback(
            code="c", state="v2.abc", error=None, db=None
        )
    )
    assert answer == "v2"
    assert seen == {"state": "v2.abc"}


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


def test_young_accounts_are_flagged_and_old_ones_are_not():
    young = snowflake(NOW - timedelta(days=2))
    old = snowflake(NOW - timedelta(days=400))
    assert account_flags(young, NOW - timedelta(days=5), NOW) == [
        "young_discord",
        "young_steam",
    ]
    assert account_flags(old, NOW - timedelta(days=400), NOW) == []
    assert account_flags(old, None, NOW) == []


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


def test_a_discord_id_must_be_ascii_digits():
    with pytest.raises(ValueError):
        SessionStart(discord_user_id="١" * 18, game="civ6")


def test_v2_answers_a_refusal_a_bug_and_a_bad_actor_in_the_envelope(monkeypatch):
    monkeypatch.setattr(
        dependencies.settings, "auth_service_token", SecretStr("auth-token")
    )

    class Failing:
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
        bad_actor = client.post(
            "/api/v2/auth/manual-registrations",
            headers={**token, "X-Actor-Discord-Id": "staff"},
            json=json.loads(manual().model_dump_json()),
        )
    finally:
        app.dependency_overrides.clear()
    error = refused.json()["detail"]["error"]
    assert (refused.status_code, error["code"]) == (409, "X_REFUSED")
    assert error["correlation_id"]
    assert (bug.status_code, bug.json()["detail"]["error"]["code"]) == (500, "INTERNAL")
    assert denied.status_code == 401
    assert bad_actor.status_code == 422


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
