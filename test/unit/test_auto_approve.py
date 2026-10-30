"""A report needing no staff is approved 48 hours after its upload."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId

from app.features.matches.auto_approve import auto_approve, is_complete
from app.features.matches.errors import ApprovingError, NotFoundError

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
LEADERS = {"LEADER_A", "LEADER_B"}
CIVS = {"CIVILIZATION_A"}


def seat(discord_id="100000000000000001", **over):
    return {
        "discord_id": discord_id,
        "leader": "LEADER_A",
        "civ": "CIVILIZATION_A",
        **over,
    }


def report(*players, game="civ6", **over):
    return {"game": game, "players": list(players), "contest_report_list": [], **over}


def test_a_complete_report_needs_no_staff():
    assert is_complete(
        report(seat(), seat("100000000000000002", leader="LEADER_B")), LEADERS, CIVS
    )


@pytest.mark.parametrize(
    "player",
    [
        seat("-1"),
        seat(quit=True),
        seat(is_sub=True),
        seat(subbed_out=True),
        seat(leader="LEADER_UNKNOWN"),
    ],
)
def test_a_seat_staff_must_look_at_holds_it_back(player):
    assert not is_complete(report(seat("100000000000000002"), player), LEADERS, CIVS)


def test_a_contest_or_an_unknown_civ7_civ_holds_it_back():
    contested = report(seat(), contest_report_list=[{"contestor_discord_id": "1"}])
    assert not is_complete(contested, LEADERS, CIVS)
    assert not is_complete(
        report(seat(civ="CIVILIZATION_X"), game="civ7"), LEADERS, CIVS
    )
    assert is_complete(report(seat(), game="civ7"), LEADERS, CIVS)


class Queries:
    def __init__(self, docs, unpublished=()):
        self.docs, self.unpublished = docs, list(unpublished)

    async def find_unpublished_by(self, approver, *, since, limit):
        self.since = since
        return self.unpublished

    async def pending_uploaded_before(self, cutoff):
        self.cutoff = cutoff
        for doc in self.docs:
            yield doc


class Service:
    def __init__(self, docs, refuse=(), unpublished=(), gone=()):
        self.q = Queries(docs, unpublished)
        self.refuse, self.gone = set(refuse), set(gone)
        self.approved = []

    async def approve_match(self, match_id, approver, *, expect=None):
        if match_id in self.refuse:
            raise ApprovingError("This report is being approved.")
        if match_id in self.gone:
            raise NotFoundError("Match not found")
        self.approved.append((match_id, approver, expect))
        return {"match_id": match_id}


class CivData:
    async def fetch(self, edition):
        return {
            "leaders": [{"token": t} for t in LEADERS],
            "civs": [{"token": t} for t in CIVS],
        }


def test_due_reports_are_approved_as_the_bot_only_as_checked_and_one_refused_waits():
    ok, held, busy, gone = ObjectId(), ObjectId(), ObjectId(), ObjectId()
    docs = [
        {"_id": ok, **report(seat())},
        {"_id": held, **report(seat(quit=True))},
        {"_id": busy, **report(seat())},
        {"_id": gone, **report(seat())},
    ]
    svc = Service(docs, refuse={str(busy)}, gone={str(gone)})
    out = asyncio.run(auto_approve(svc, CivData(), "999", NOW))
    assert out == [{"match_id": str(ok)}]
    # The claim holds only while the seats are as checked and nobody contested.
    assert svc.approved == [
        (str(ok), "999", {"players": [seat()], "contest_report_list": []})
    ]
    assert svc.q.cutoff.generation_time == NOW - timedelta(hours=48)


def test_its_own_unpublished_approvals_are_answered_again_first():
    lost = ObjectId()
    svc = Service([{"_id": ObjectId(), **report(seat())}], unpublished=[{"_id": lost}])
    out = asyncio.run(auto_approve(svc, CivData(), "999", NOW))
    assert out[0] == {"match_id": str(lost)} and len(out) == 2
    assert svc.approved[0] == (str(lost), "999", None)
    assert svc.q.since == NOW - timedelta(days=7)


def test_held_reports_do_not_block_newer_ones_and_a_call_approves_five_at_most():
    held = [{"_id": ObjectId(), **report(seat(quit=True))} for _ in range(6)]
    ready = [{"_id": ObjectId(), **report(seat())} for _ in range(7)]
    svc = Service(held + ready)
    out = asyncio.run(auto_approve(svc, CivData(), "999", NOW))
    assert [m["match_id"] for m in out] == [str(d["_id"]) for d in ready[:5]]
