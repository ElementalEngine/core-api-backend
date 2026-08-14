"""Fixture tests for the post-migration reconciliation arithmetic (D86).

D60 leaves the three transactional flows with no CI coverage, and D86 offers
these as the cheap partial mitigation: the calculation itself is trusted even
though the transaction is not. Pure -- no database (D59, D60).

Fixture 7 is the one that proves the test can fail. Without it, 1-6 all pass
on a query that returns "no divergence" unconditionally.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

from app.features.ratings.reconciliation import EPSILON, reconcile

INITIAL_MU = 1250.0
T0 = datetime(2026, 8, 1, tzinfo=UTC)

FFA = "civ6_lifetime_stats.rt_ffa"
SEASON = "civ6_season_stats.rt_ffa"
COMBINED = "civ6_lifetime_stats.rt_combined"


def ev(player_id, scope, mu_before, mu_after, minute=0, event_type="approve"):
    return {
        "event_type": event_type,
        "occurred_at": T0 + timedelta(minutes=minute),
        "player_id": player_id,
        "scope": scope,
        "mu_before": mu_before,
        "mu_after": mu_after,
    }


def test_fixture_1_three_approves_no_reverts():
    events = [
        ev(1, FFA, 1250.0, 1263.0, 0),
        ev(1, FFA, 1263.0, 1250.5, 1),
        ev(1, FFA, 1250.5, 1272.0, 2),
    ]
    assert reconcile(events, {(1, FFA): 1272.0}, initial_mu=INITIAL_MU) == []


def test_fixture_2_revert_nets_out():
    """A query filtering to event_type 'approve' would report every reverted
    match as divergence. Summing all events nets them naturally."""
    events = [
        ev(1, FFA, 1250.0, 1263.0, 0),
        ev(1, FFA, 1263.0, 1280.0, 1),
        ev(1, FFA, 1280.0, 1263.0, 2, "revert"),
    ]
    assert reconcile(events, {(1, FFA): 1263.0}, initial_mu=INITIAL_MU) == []


def test_fixture_3_reset_carries_its_movement():
    events = [
        ev(1, FFA, 1250.0, 1300.0, 0),
        ev(1, FFA, 1300.0, INITIAL_MU, 1, "reset"),
        ev(1, FFA, INITIAL_MU, 1275.0, 2),
    ]
    assert reconcile(events, {(1, FFA): 1275.0}, initial_mu=INITIAL_MU) == []


def test_fixture_3b_reset_leaves_no_stat_document():
    """A reset deletes the document. Missing is initial_mu, not an error --
    otherwise every reset player diverges by exactly their whole rating."""
    events = [
        ev(1, FFA, 1250.0, 1300.0, 0),
        ev(1, FFA, 1300.0, INITIAL_MU, 1, "reset"),
    ]
    assert reconcile(events, {(1, FFA): None}, initial_mu=INITIAL_MU) == []


def test_fixture_4_player_with_zero_events_is_skipped():
    assert reconcile([], {(9, FFA): 1400.0}, initial_mu=INITIAL_MU) == []


def test_fixture_5_placeholder_player_is_skipped():
    """Placeholders have neither side of the equation; the ledger writes skip
    them by the same guard."""
    assert reconcile([ev(-1, FFA, 1250.0, 1300.0)], {}, initial_mu=INITIAL_MU) == []


def test_fixture_6_scopes_reconcile_independently():
    """One approve writes three stat documents per player. Summing across
    scopes produces a number that means nothing."""
    events = [
        ev(1, FFA, 1250.0, 1263.0, 0),
        ev(1, SEASON, 1250.0, 1244.0, 0),
        ev(1, COMBINED, 1250.0, 1281.0, 0),
    ]
    actual = {(1, FFA): 1263.0, (1, SEASON): 1244.0, (1, COMBINED): 1281.0}
    assert reconcile(events, actual, initial_mu=INITIAL_MU) == []


def test_fixture_7_broken_set_is_reported():
    """D86 Rule 1: a test must be able to fail for the reason it exists."""
    events = [
        ev(1, FFA, 1250.0, 1263.0, 0),
        ev(1, FFA, 1263.0, 1280.0, 1),
    ]
    found = reconcile(events, {(1, FFA): 1275.0}, initial_mu=INITIAL_MU)
    assert len(found) == 1
    assert found[0].player_id == 1 and found[0].scope == FFA
    assert abs(found[0].amount - 5.0) < EPSILON
    assert found[0].event_count == 2


def test_fixture_8_float_accumulation_sets_epsilon():
    """The worst accumulation found over 3,600 trials, reproduced exactly.
    It must be non-zero -- a fixture that never accumulates sets nothing."""
    random.seed(7)
    mu = INITIAL_MU
    events = []
    for i in range(1000):
        nxt = mu + random.uniform(-137.0, 137.0)
        events.append(ev(1, FFA, mu, nxt, i))
        mu = nxt

    baseline = events[0]["mu_before"]
    summed = sum(e["mu_after"] - e["mu_before"] for e in events)
    accumulation = abs(baseline + summed - mu)

    assert accumulation > 0.0, "no accumulation: this fixture proves nothing"
    assert accumulation < EPSILON / 100, f"EPSILON too tight for {accumulation}"
    assert reconcile(events, {(1, FFA): mu}, initial_mu=INITIAL_MU) == []
