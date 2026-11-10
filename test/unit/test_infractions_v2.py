import ast
import asyncio
import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from pymongo.errors import DuplicateKeyError

from app.core import dependencies
from app.features.infractions import service_v2
from app.features.infractions.errors import (
    ChangeConflict,
    NoSuspension,
    StillSuspended,
    SuspensionHeld,
)
from app.features.infractions.models import FlatType, SuspensionDocument, TierCategory
from app.features.infractions.schemas_v2 import InfractionBody
from app.features.infractions.service_v2 import Infractions
from app.main import app

PLAYER = "123456789012345678"
STAFF = "999999999999999999"
ROLE = "111111111111111111"
OTHER = "222222222222222222"
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
DAY = timedelta(days=1)
ZERO = {"tier": 0, "decays": None}


def _get(doc, key):
    for part in key.split("."):
        doc = doc.get(part) if isinstance(doc, dict) else None
    return doc


def _ok(value, want):
    if not isinstance(want, dict):
        return value == want
    if "$in" in want:
        return value in want["$in"]
    if "$lte" in want:
        return value is not None and value <= want["$lte"]
    return value is not None and value > want["$gt"]


class Cursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, key, direction):
        self.docs.sort(key=lambda d: d[key], reverse=direction < 0)
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    async def to_list(self):
        return self.docs


class Collection:
    """Mongo's filters and updates as the service uses them, yielding like I/O."""

    def __init__(self):
        self.docs = []

    def _match(self, flt):
        return [d for d in self.docs if all(_ok(_get(d, k), v) for k, v in flt.items())]

    def _apply(self, doc, update):
        for key, value in update.get("$set", {}).items():
            *parents, last = key.split(".")
            target = doc
            for part in parents:
                target = target.setdefault(part, {})
            target[last] = value
        for key, step in update.get("$inc", {}).items():
            doc[key] = (doc.get(key) or 0) + step

    async def find_one(self, flt, projection=None, sort=None):
        await asyncio.sleep(0)
        found = self._match(flt)
        if sort:
            found.sort(key=lambda d: d[sort[0][0]])
        return dict(found[0]) if found else None

    def find(self, flt, projection=None):
        return Cursor([dict(d) for d in self._match(flt)])

    async def update_one(self, flt, update, upsert=False):
        await asyncio.sleep(0)
        found = self._match(flt)[:1]
        for doc in found:
            self._apply(doc, update)
        if found or not upsert:
            return SimpleNamespace(matched_count=len(found), upserted_id=None)
        if any(d["discord_id"] == flt["discord_id"] for d in self.docs):
            raise DuplicateKeyError("discord_id")
        doc = {"discord_id": flt["discord_id"]}
        self._apply(doc, update)
        self.docs.append(doc)
        return SimpleNamespace(matched_count=0, upserted_id="new")

    async def replace_one(self, flt, doc, upsert=False):
        self.docs = [d for d in self.docs if d["_id"] != flt["_id"]] + [dict(doc)]

    async def delete_one(self, flt):
        gone = self._match(flt)[:1]
        self.docs = [d for d in self.docs if d not in gone]


@pytest.fixture(autouse=True)
def fixed_now(monkeypatch):
    monkeypatch.setattr(service_v2, "now_utc", lambda: NOW)


def setup(**record):
    client = defaultdict(lambda: defaultdict(Collection))
    members = client["server_members"]
    if record:
        doc = {"discord_id": PLAYER, "suspended": False, "ends": None}
        doc |= {c.value: dict(ZERO) for c in TierCategory}
        members["suspensions"].docs.append(doc | record)
    return Infractions(client), members


def run(coro):
    return asyncio.run(coro)


def stored(members):
    return members["suspensions"].docs[0]


def body(*roles):
    return InfractionBody(reason="r", suspended_roles=list(roles))


def test_a_first_infraction_suspends_and_keeps_the_roles_removed():
    service, members = setup()
    result = run(service.tier(PLAYER, TierCategory.quit, body(ROLE), STAFF))
    assert (result.tier, result.days_added, result.ends) == (1, 1, NOW + DAY)
    doc = stored(members)
    assert doc["suspendedRoles"] == [ROLE]
    assert doc["quit"] == {"tier": 1, "decays": NOW + 90 * DAY}
    assert doc["rev"] == 1


def test_days_stack_on_a_running_suspension_and_its_roles_are_kept():
    service, members = setup(
        suspended=True,
        ends=NOW + 5 * DAY,
        suspendedRoles=[ROLE],
        active_category="quit",
    )
    result = run(service.flat(PLAYER, FlatType.oversub, body(), STAFF))
    assert result.ends == NOW + 8 * DAY
    assert stored(members)["suspendedRoles"] == [ROLE]


def test_a_role_regained_during_a_suspension_is_kept_with_the_first_ones():
    service, members = setup(
        suspended=True,
        ends=NOW + 5 * DAY,
        suspendedRoles=[ROLE],
        active_category="quit",
    )
    run(service.tier(PLAYER, TierCategory.quit, body(OTHER), STAFF))
    assert stored(members)["suspendedRoles"] == [ROLE, OTHER]


def test_a_minor_warning_changes_no_suspension():
    # v1 set suspended false and cleared the end on a warning.
    service, members = setup(suspended=True, ends=NOW + 3 * DAY, active_category="flat")
    result = run(service.tier(PLAYER, TierCategory.minor, body(), STAFF))
    assert result.is_warning_only and result.suspended
    doc = stored(members)
    assert (doc["suspended"], doc["ends"], doc["active_category"]) == (
        True,
        NOW + 3 * DAY,
        "flat",
    )
    fresh, others = setup()
    warned = run(fresh.tier(PLAYER, TierCategory.minor, body(ROLE), STAFF))
    assert (warned.suspended, warned.ends) == (False, None)
    assert "suspended" not in stored(others)


def test_the_ban_threshold_holds_the_suspension_until_staff_act():
    # v1 added no days, so the suspension ended the moment it began.
    service, members = setup(quit={"tier": 5, "decays": NOW + DAY})
    result = run(service.tier(PLAYER, TierCategory.quit, body(ROLE), STAFF))
    assert (result.is_ban_threshold, result.held, result.ends) == (True, True, None)
    assert run(service.due()).discord_ids == []
    with pytest.raises(SuspensionHeld):
        run(service.days(PLAYER, 3, STAFF))
    with pytest.raises(SuspensionHeld):
        run(service.flat(PLAYER, FlatType.comp, body(), STAFF))
    cleared = run(service.unsuspend(PLAYER, "appeal upheld", STAFF))
    assert (cleared.cleared, cleared.suspended_roles) == (True, [ROLE])


def test_a_tier_during_a_hold_keeps_a_regained_role_and_is_no_new_threshold():
    # Each further tier pinged moderators and restarted the appeal, as if newly held.
    service, members = setup(
        suspended=True,
        ends=None,
        suspendedRoles=[ROLE],
        active_category="extreme",
        extreme={"tier": 2, "decays": NOW + DAY},
    )
    result = run(service.tier(PLAYER, TierCategory.extreme, body(OTHER), STAFF))
    assert (result.held, result.is_ban_threshold, result.tier) == (True, False, 2)
    assert stored(members)["suspendedRoles"] == [ROLE, OTHER]


def test_tiers_decay_once_for_each_full_period_and_keep_their_clock():
    service, members = setup(
        quit={"tier": 3, "decays": NOW - 100 * DAY},
        major={"tier": 2, "decays": NOW - 10 * DAY},
    )
    run(service.flat(PLAYER, FlatType.comp, body(), STAFF))
    doc = stored(members)
    assert doc["quit"] == {"tier": 1, "decays": NOW + 80 * DAY}
    assert doc["major"] == {"tier": 1, "decays": NOW + 80 * DAY}


def test_a_new_infraction_escalates_from_the_decayed_tier():
    # v1 decayed only on a read, so the next infraction counted stale tiers.
    service, _ = setup(quit={"tier": 4, "decays": NOW - 400 * DAY})
    result = run(service.tier(PLAYER, TierCategory.quit, body(), STAFF))
    assert (result.tier, result.days_added) == (1, 1)


def test_a_category_taken_over_restarts_its_clock():
    # Its served time is not clean time, even when another suspension replaces it.
    service, members = setup(
        suspended=True,
        ends=NOW + DAY,
        active_category="major",
        major={"tier": 2, "decays": NOW - 10 * DAY},
    )
    run(service.flat(PLAYER, FlatType.comp, body(), STAFF))
    assert stored(members)["major"] == {"tier": 2, "decays": NOW + 90 * DAY}
    run(service.unsuspend(PLAYER, None, STAFF))
    assert stored(members)["major"]["tier"] == 2


def test_a_warning_on_an_overdue_suspension_reports_it_over():
    service, _ = setup(suspended=True, ends=NOW - DAY, active_category="flat")
    result = run(service.tier(PLAYER, TierCategory.minor, body(), STAFF))
    assert (result.is_warning_only, result.suspended) == (True, False)


def test_the_category_being_served_does_not_decay():
    service, members = setup(
        suspended=True,
        ends=NOW + DAY,
        active_category="major",
        major={"tier": 2, "decays": NOW - 10 * DAY},
    )
    run(service.flat(PLAYER, FlatType.comp, body(), STAFF))
    assert stored(members)["major"]["tier"] == 2


def test_nothing_decays_during_a_hold_and_every_clock_restarts_after_it():
    # A banned member's other tiers kept decaying, and would all drop once lifted.
    service, members = setup(
        suspended=True,
        ends=None,
        active_category="extreme",
        extreme={"tier": 2, "decays": NOW - 2000 * DAY},
        quit={"tier": 4, "decays": NOW - 400 * DAY},
    )
    assert run(service.record(PLAYER)).tiers[TierCategory.quit] == 4
    run(service.unsuspend(PLAYER, "appeal upheld", STAFF))
    doc = stored(members)
    assert doc["quit"] == {"tier": 4, "decays": NOW + 90 * DAY}
    assert doc["extreme"] == {"tier": 2, "decays": NOW + 1460 * DAY}


def test_days_need_a_suspension_with_an_end():
    # v1 marked an unsuspended member suspended without touching their roles.
    service, members = setup()
    with pytest.raises(NoSuspension):
        run(service.days(PLAYER, 3, STAFF))
    assert members["suspensions"].docs == []


def test_days_added_stack_on_the_end_and_days_removed_come_off_it():
    service, _ = setup(suspended=True, ends=NOW + 2 * DAY, active_category="quit")
    assert run(service.days(PLAYER, 3, STAFF)).ends == NOW + 5 * DAY
    shorter = run(service.days(PLAYER, -4, STAFF))
    assert (shorter.ends, shorter.suspended) == (NOW + DAY, True)
    ended = run(service.days(PLAYER, -2, STAFF))
    assert (ended.ends, ended.suspended) == (NOW - DAY, False)


def test_a_tier_is_removed_only_while_the_member_is_not_suspended():
    service, members = setup(
        suspended=True, ends=NOW + DAY, quit={"tier": 2, "decays": NOW + DAY}
    )
    with pytest.raises(StillSuspended):
        run(service.remove_tier(PLAYER, TierCategory.quit, STAFF))
    stored(members)["suspended"] = False
    removed = run(service.remove_tier(PLAYER, TierCategory.quit, STAFF))
    assert (removed.tier, removed.was_changed) == (1, True)
    run(service.remove_tier(PLAYER, TierCategory.quit, STAFF))
    assert (
        run(service.remove_tier(PLAYER, TierCategory.quit, STAFF)).was_changed is False
    )


def test_each_tier_ladder_has_a_duration_for_every_tier_below_the_threshold():
    for category in TierCategory:
        cap = service_v2.TIER_CAPS[category.value]
        if category == TierCategory.lagger:
            assert service_v2.TIER_DURATIONS["lagger"] == []
            assert "lagger" not in service_v2.DECAY_DAYS
            continue
        assert len(service_v2.TIER_DURATIONS[category.value]) == cap - 1
        assert service_v2.DECAY_DAYS[category.value] > 0


def test_lagger_strikes_tag_at_three_and_never_suspend_or_decay():
    service, members = setup(suspended=True, ends=NOW + DAY, active_category="quit")
    first = run(service.tier(PLAYER, TierCategory.lagger, body(), STAFF))
    assert (first.tier, first.days_added, first.is_ban_threshold) == (1, 0, False)
    assert (first.suspended, first.ends) == (True, NOW + DAY)
    run(service.tier(PLAYER, TierCategory.lagger, body(), STAFF))
    third = run(service.tier(PLAYER, TierCategory.lagger, body(), STAFF))
    assert (third.tier, third.is_ban_threshold) == (3, True)
    doc = stored(members)
    assert doc["lagger"] == {"tier": 3, "decays": None}
    assert (doc["suspended"], doc["ends"], doc["active_category"]) == (
        True,
        NOW + DAY,
        "quit",
    )
    fourth = run(service.tier(PLAYER, TierCategory.lagger, body(), STAFF))
    assert (fourth.tier, fourth.is_ban_threshold) == (3, True)
    held, _ = setup(
        suspended=True,
        ends=None,
        active_category="extreme",
        lagger={"tier": 2, "decays": None},
    )
    tagged = run(held.tier(PLAYER, TierCategory.lagger, body(), STAFF))
    assert (tagged.is_ban_threshold, tagged.held, tagged.suspended) == (
        True,
        True,
        True,
    )
    assert (
        service_v2.decay(SuspensionDocument.model_validate(doc), NOW + 1000 * DAY) == {}
    )


def test_a_lagger_strike_comes_off_even_while_suspended_and_shows_in_the_record():
    service, members = setup(
        suspended=True,
        ends=NOW + DAY,
        active_category="quit",
        lagger={"tier": 3, "decays": None},
    )
    removed = run(service.remove_tier(PLAYER, TierCategory.lagger, STAFF))
    assert (removed.tier, removed.was_changed) == (2, True)
    assert stored(members)["lagger"] == {"tier": 2, "decays": None}
    record = run(service.record(PLAYER))
    assert (record.suspended, record.held, record.active_category) == (
        True,
        False,
        "quit",
    )
    assert (record.tiers[TierCategory.lagger], record.lagger_tagged) == (2, False)
    assert run(service.record("999999999999999999")).tiers[TierCategory.quit] == 0


def test_the_record_reads_tiers_as_decayed_without_writing():
    service, members = setup(quit={"tier": 4, "decays": NOW - 400 * DAY})
    record = run(service.record(PLAYER))
    assert (record.tiers[TierCategory.quit], record.suspended) == (0, False)
    assert stored(members)["quit"]["tier"] == 4


def test_the_laggers_list_names_every_strike_with_the_tagged_first():
    service, members = setup(lagger={"tier": 1, "decays": None})
    other = {"discord_id": "999999999999999903", "suspended": False, "ends": None}
    other |= {c.value: dict(ZERO) for c in TierCategory}
    other["lagger"] = {"tier": 3, "decays": None}
    members["suspensions"].docs.append(other)
    listed = run(service.laggers()).members
    assert [(m.discord_id, m.strikes, m.tagged) for m in listed] == [
        ("999999999999999903", 3, True),
        (PLAYER, 1, False),
    ]


def test_a_rejoin_says_whether_the_member_wears_the_lagger_tag():
    service, _ = setup(lagger={"tier": 3, "decays": None})
    applied = run(service.apply_pending(PLAYER, []))
    assert (applied.applied, applied.lagger_tagged) == (False, True)


def test_a_removal_that_changes_nothing_is_not_logged_as_a_write(caplog):
    service, _ = setup(quit={"tier": 1, "decays": NOW + DAY})
    with caplog.at_level("INFO", logger="app.features.infractions.service_v2"):
        run(service.remove_tier(PLAYER, TierCategory.quit, STAFF))
        run(service.remove_tier(PLAYER, TierCategory.quit, STAFF))
    assert [r.message for r in caplog.records if "remove-tier" in r.message] == [
        f"Infraction remove-tier discord_id={PLAYER} actor={STAFF} category=quit tier=0"
    ]


def test_unsuspend_clears_once_and_a_repeat_still_returns_the_roles():
    # An answer lost on the way back must not lose the member's roles.
    service, members = setup(suspended=True, ends=NOW + DAY, suspendedRoles=[ROLE])
    first = run(service.unsuspend(PLAYER, None, STAFF))
    second = run(service.unsuspend(PLAYER, None, STAFF))
    assert (first.cleared, first.suspended_roles) == (True, [ROLE])
    assert (second.cleared, second.suspended_roles) == (False, [ROLE])
    assert (stored(members)["suspended"], stored(members)["ends"]) == (False, None)


def test_a_repeat_says_whether_the_record_is_still_suspended():
    # A lost answer is asked again; only a record no longer suspended gives roles back.
    service, _ = setup(suspended=True, ends=NOW + DAY, suspendedRoles=[ROLE])
    assert run(service.expire(PLAYER)).suspended is True
    assert run(service.unsuspend(PLAYER, None, STAFF)).suspended is False
    assert run(service.expire(PLAYER)).suspended is False


def test_release_restarts_the_served_category_clock():
    # Time served is not clean time, so a long hold does not decay on release.
    service, members = setup(
        suspended=True,
        ends=None,
        active_category="quit",
        quit={"tier": 6, "decays": NOW - 180 * DAY},
    )
    run(service.unsuspend(PLAYER, "appeal", STAFF))
    assert stored(members)["quit"] == {"tier": 6, "decays": NOW + 90 * DAY}


def test_due_lists_what_has_ended_and_when_the_next_one_ends():
    service, members = setup(suspended=True, ends=NOW - DAY)
    later = {
        "discord_id": "223456789012345678",
        "suspended": True,
        "ends": NOW + 2 * DAY,
    }
    soon = {"discord_id": "323456789012345678", "suspended": True, "ends": NOW + DAY}
    held = {"discord_id": "423456789012345678", "suspended": True, "ends": None}
    members["suspensions"].docs += [later, soon, held]
    due = run(service.due())
    assert (due.discord_ids, due.next_ends) == ([PLAYER], NOW + DAY)


def test_expire_ends_a_due_suspension_once():
    # The timer and the sweep could both expire a member, posting twice.
    service, _ = setup(suspended=True, ends=NOW - DAY, suspendedRoles=[ROLE])

    async def both():
        return await asyncio.gather(service.expire(PLAYER), service.expire(PLAYER))

    results = run(both())
    assert sorted(r.cleared for r in results) == [False, True]
    assert [r.suspended_roles for r in results] == [[ROLE], [ROLE]]


def test_a_hold_never_expires():
    service, members = setup(suspended=True, ends=None, active_category="extreme")
    assert run(service.expire(PLAYER)).cleared is False
    assert stored(members)["suspended"] is True


def test_an_overdue_suspension_counts_as_over():
    # Auth already reads it as over; days and tiers now agree before the sweep runs.
    service, _ = setup(
        suspended=True, ends=NOW - DAY, quit={"tier": 2, "decays": NOW + DAY}
    )
    with pytest.raises(NoSuspension):
        run(service.days(PLAYER, 3, STAFF))
    assert run(service.remove_tier(PLAYER, TierCategory.quit, STAFF)).tier == 1


def test_expire_leaves_a_suspension_that_was_just_extended():
    service, members = setup(suspended=True, ends=NOW + DAY)
    assert run(service.expire(PLAYER)).cleared is False
    assert stored(members)["suspended"] is True


def test_joining_keeps_the_roles_removed_and_closes_the_pending_entry():
    # v1 removed the roles on joining but never stored them, so none came back.
    service, members = setup(suspended=True, ends=NOW + DAY, suspendedRoles=[])
    run(service.set_pending(PLAYER, FlatType.smurf, "alt", STAFF))
    applied = run(service.apply_pending(PLAYER, [ROLE]))
    assert (applied.applied, applied.punishment_type, applied.ends) == (
        True,
        "smurf",
        NOW + DAY,
    )
    assert stored(members)["suspendedRoles"] == [ROLE]
    assert members["suspensions_due"].docs == []


def test_rejoining_while_suspended_applies_again_without_a_pending_entry():
    # Leaving and rejoining shed the Suspended role, since only an entry applied.
    service, members = setup(
        suspended=True,
        ends=NOW + DAY,
        suspendedRoles=[ROLE],
        active_category="quit",
    )
    applied = run(service.apply_pending(PLAYER, [OTHER, ROLE]))
    assert (applied.applied, applied.punishment_type, applied.reason) == (
        True,
        "quit",
        None,
    )
    assert (applied.ends, applied.held) == (NOW + DAY, False)
    assert stored(members)["suspendedRoles"] == [ROLE, OTHER]


def test_a_hold_applies_again_on_joining():
    service, _ = setup(suspended=True, ends=None, active_category="extreme")
    applied = run(service.apply_pending(PLAYER, []))
    assert (applied.applied, applied.held, applied.ends) == (True, True, None)


def test_a_member_without_a_record_who_joins_is_left_alone():
    service, members = setup()
    applied = run(service.apply_pending(PLAYER, [ROLE]))
    assert (applied.applied, applied.punishment_type) == (False, None)
    assert members["suspensions"].docs == []


def test_a_pending_entry_written_while_joining_is_kept(monkeypatch):
    service, members = setup(suspended=True, ends=NOW + DAY)
    due = members["suspensions_due"]
    due.docs.append({"_id": PLAYER, "punishment_type": "smurf", "created_at": NOW})
    older = {"_id": PLAYER, "punishment_type": "quit", "created_at": NOW - DAY}

    async def read_before_the_newer_write(flt, projection=None, sort=None):
        return dict(older)

    monkeypatch.setattr(due, "find_one", read_before_the_newer_write)
    assert run(service.apply_pending(PLAYER, [ROLE])).applied is True
    assert [d["punishment_type"] for d in due.docs] == ["smurf"]


def test_a_pending_entry_stored_without_a_time_is_still_closed():
    service, members = setup(suspended=True, ends=NOW + DAY)
    members["suspensions_due"].docs.append({"_id": PLAYER, "punishment_type": "quit"})
    assert run(service.apply_pending(PLAYER, [ROLE])).applied is True
    assert members["suspensions_due"].docs == []


def test_a_pending_entry_whose_suspension_ended_is_closed_without_applying():
    service, members = setup(suspended=False, ends=None)
    run(service.set_pending(PLAYER, TierCategory.quit, None, STAFF))
    assert run(service.apply_pending(PLAYER, [ROLE])).applied is False
    assert members["suspensions_due"].docs == []
    assert "suspendedRoles" not in stored(members)


def test_two_infractions_at_once_both_count():
    # v1 read then wrote, so one of two concurrent infractions was lost.
    service, members = setup(quit={"tier": 1, "decays": NOW + DAY})

    async def both():
        return await asyncio.gather(
            service.tier(PLAYER, TierCategory.quit, body(), STAFF),
            service.tier(PLAYER, TierCategory.quit, body(), STAFF),
        )

    tiers = sorted(r.tier for r in run(both()))
    assert tiers == [2, 3]
    assert stored(members)["quit"]["tier"] == 3


def test_two_first_infractions_at_once_make_one_record():
    service, members = setup()

    async def both():
        return await asyncio.gather(
            service.flat(PLAYER, FlatType.comp, body(), STAFF),
            service.flat(PLAYER, FlatType.comp, body(), STAFF),
        )

    run(both())
    assert len(members["suspensions"].docs) == 1
    assert stored(members)["ends"] == NOW + 14 * DAY


def test_a_record_that_keeps_moving_answers_a_retryable_conflict(monkeypatch):
    service, members = setup(suspended=True, ends=NOW + DAY)
    original = members["suspensions"].update_one

    async def moved(flt, update, upsert=False):
        stored(members)["rev"] = (stored(members).get("rev") or 0) + 1
        return await original(flt, update, upsert)

    monkeypatch.setattr(members["suspensions"], "update_one", moved)
    with pytest.raises(ChangeConflict) as caught:
        run(service.days(PLAYER, 1, STAFF))
    assert (caught.value.status_code, caught.value.retryable) == (409, True)
    assert stored(members)["ends"] == NOW + DAY


def http(monkeypatch, members_client):
    monkeypatch.setattr(
        dependencies.settings, "lj_service_token", SecretStr("lj-token")
    )
    app.dependency_overrides[dependencies.get_database] = lambda: members_client
    return TestClient(app, raise_server_exceptions=False)


def test_the_routes_need_the_token_and_staff_writes_need_the_actor(monkeypatch):
    client_db = defaultdict(lambda: defaultdict(Collection))
    try:
        client = http(monkeypatch, client_db)
        token = {"Authorization": "Bearer lj-token"}
        url = f"/api/v2/infractions/{PLAYER}/flat/comp"
        denied = client.post(url, json={})
        no_actor = client.post(url, headers=token, json={})
        bad_id = client.post(
            "/api/v2/infractions/42/flat/comp",
            headers={**token, "X-Actor-Discord-Id": STAFF},
            json={},
        )
        done = client.post(url, headers={**token, "X-Actor-Discord-Id": STAFF}, json={})
        no_body = client.post(
            f"/api/v2/infractions/{PLAYER}/unsuspend",
            headers={**token, "X-Actor-Discord-Id": STAFF},
        )
        loose_days = client.post(
            f"/api/v2/infractions/{PLAYER}/days",
            headers={**token, "X-Actor-Discord-Id": STAFF},
            json={"days": True},
        )
        expired = client.post(f"/api/v2/infractions/{PLAYER}/expire", headers=token)
    finally:
        app.dependency_overrides.clear()
    assert denied.status_code == 401
    assert denied.json()["detail"]["error"]["code"] == "UNAUTHORIZED"
    assert no_actor.status_code == bad_id.status_code == 422
    assert done.status_code == 200 and done.json()["ends"] is not None
    assert no_body.status_code == 200 and no_body.json()["cleared"] is True
    assert loose_days.status_code == 422
    assert expired.json() == {
        "discord_id": PLAYER,
        "cleared": False,
        "suspended": False,
        "suspended_roles": [],
    }


def test_a_refusal_answers_in_the_envelope_with_the_correlation_id(monkeypatch):
    client_db = defaultdict(lambda: defaultdict(Collection))
    try:
        client = http(monkeypatch, client_db)
        answer = client.post(
            f"/api/v2/infractions/{PLAYER}/days",
            headers={"Authorization": "Bearer lj-token", "X-Actor-Discord-Id": STAFF},
            json={"days": 2},
        )
        zero = client.post(
            f"/api/v2/infractions/{PLAYER}/days",
            headers={"Authorization": "Bearer lj-token", "X-Actor-Discord-Id": STAFF},
            json={"days": 0},
        )
    finally:
        app.dependency_overrides.clear()
    error = answer.json()["detail"]["error"]
    assert (answer.status_code, error["code"], error["retryable"]) == (
        409,
        "NOT_SUSPENDED",
        False,
    )
    assert re.fullmatch(r"[0-9a-f]{12}", error["correlation_id"])
    assert zero.status_code == 422


# Every code v2 answers with. The bot words each one, so a new code must be
# added here and to the bot together.
V2_CODES = {"CONFLICT", "NOT_SUSPENDED", "STILL_SUSPENDED", "SUSPENSION_HELD"}
# Answered by the app around the routes, not by an infraction error.
TRANSPORT_CODES = {
    "INTERNAL",
    "INVALID_REQUEST",
    "LJ_SERVICE_MISCONFIGURED",
    "UNAUTHORIZED",
    "UNAVAILABLE",
}


def test_v2_answers_only_its_pinned_codes():
    feature = Path(__file__).resolve().parents[2] / "app" / "features" / "infractions"
    codes = {}
    for node in ast.parse((feature / "errors.py").read_text()).body:
        if isinstance(node, ast.ClassDef):
            for call in ast.walk(node):
                for kw in getattr(call, "keywords", []):
                    if kw.arg == "code" and isinstance(kw.value, ast.Constant):
                        codes[node.name] = kw.value.value
    raised = set()
    for module in ("service_v2", "router_v2"):
        for n in ast.walk(ast.parse((feature / f"{module}.py").read_text())):
            if isinstance(n, ast.Raise) and n.exc is not None:
                assert isinstance(n.exc, ast.Call) and isinstance(n.exc.func, ast.Name)
                raised.add(n.exc.func.id)
    assert {codes[n] for n in raised} == V2_CODES
    core = Path(__file__).resolve().parents[2] / "app" / "core"
    literals = {
        n.value
        for f in ("dependencies.py", "errors.py")
        for n in ast.walk(ast.parse((core / f).read_text()))
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    }
    assert TRANSPORT_CODES <= literals
