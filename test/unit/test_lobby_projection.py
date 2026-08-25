"""D73's visibility table, row by row (D86 Rule 3, D167).

Rule 3 calls censoring the only place where a defect is silent and
adversarial, so this is table-driven from the spec's own rows rather than
written case by case, and every row asserts what is HIDDEN as well as what
is shown -- a projection that returned the document untouched would pass a
suite that only checked the visible half.

⚠ The blind rows carry `pool` AND `pool_appearances`. Censoring `pick` alone
leaks by elimination; leaving the union of all pools in place leaks the same
way one level up, because pools are disjoint (spec section 4).
"""

from __future__ import annotations

import pytest

from app.features.lobbies.projection import project_lobby

ALICE, BOB, CAROL = "alice", "bob", "carol"
OBSERVER = None
STRANGER = "nobody"


def lobby(phase, draft_mode="standard", revealed_at=None):
    return {
        "_id": "L1",
        "phase": phase,
        "draft_mode": draft_mode,
        "revealed_at": revealed_at,
        "pool_appearances": ["LEADER_A", "LEADER_B", "LEADER_C"],
        "seats": [
            {
                "seat_index": i,
                "discord_id": who,
                "ballot": {"map_type": "pangaea"},
                "pool": [f"LEADER_{who.upper()}"],
                "pick": f"LEADER_{who.upper()}",
            }
            for i, who in enumerate((ALICE, BOB, CAROL))
        ],
    }


def seat_of(projected, who):
    return next(s for s in projected["seats"] if s["discord_id"] == who)


def holders_of(projected, field):
    return {s["discord_id"] for s in projected["seats"] if field in s}


# (phase, draft_mode, revealed_at, viewer, who may see a ballot, who a pool)
TABLE = [
    ("lobby", "standard", None, ALICE, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("settings", "standard", None, ALICE, {ALICE}, {ALICE, BOB, CAROL}),
    ("settings", "standard", None, BOB, {BOB}, {ALICE, BOB, CAROL}),
    ("settings", "standard", None, OBSERVER, set(), {ALICE, BOB, CAROL}),
    ("settings", "standard", None, STRANGER, set(), {ALICE, BOB, CAROL}),
    ("settings", "blind", None, ALICE, {ALICE}, {ALICE, BOB, CAROL}),
    ("bans", "standard", None, ALICE, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("bans", "standard", None, OBSERVER, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("draft", "standard", None, OBSERVER, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("draft", "snake", None, OBSERVER, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("draft", "cwc", None, OBSERVER, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("draft", "blind", None, ALICE, {ALICE, BOB, CAROL}, {ALICE}),
    ("draft", "blind", None, CAROL, {ALICE, BOB, CAROL}, {CAROL}),
    ("draft", "blind", None, OBSERVER, {ALICE, BOB, CAROL}, set()),
    ("draft", "blind", None, STRANGER, {ALICE, BOB, CAROL}, set()),
    ("draft", "blind", "T", OBSERVER, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("complete", "blind", None, OBSERVER, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
    ("cancelled", "blind", None, OBSERVER, {ALICE, BOB, CAROL}, {ALICE, BOB, CAROL}),
]


@pytest.mark.parametrize(
    "phase,mode,revealed,viewer,ballots,pools",
    TABLE,
    ids=[
        f"{r[0]}-{r[1]}-{r[3] or 'observer'}-{'revealed' if r[2] else 'hidden'}"
        for r in TABLE
    ],
)
def test_d73_row(phase, mode, revealed, viewer, ballots, pools):
    projected = project_lobby(lobby(phase, mode, revealed), viewer)
    assert holders_of(projected, "ballot") == ballots
    assert holders_of(projected, "pool") == pools
    # `pick` travels with `pool` -- censoring one without the other is the
    # elimination leak the spec names.
    assert holders_of(projected, "pick") == pools


def test_blind_draft_also_hides_the_union_of_every_pool():
    # The leak one level up: disjoint pools make pool_appearances the union,
    # so a viewer knowing it and their own pool knows the rest.
    hidden = project_lobby(lobby("draft", "blind"), ALICE)
    assert "pool_appearances" not in hidden
    shown = project_lobby(lobby("draft", "blind", revealed_at="T"), ALICE)
    assert shown["pool_appearances"] == ["LEADER_A", "LEADER_B", "LEADER_C"]


def test_settings_shows_participation_not_preference():
    projected = project_lobby(lobby("settings"), OBSERVER)
    assert projected["ballots_submitted"] == 3
    assert holders_of(projected, "ballot") == set()


def test_ballots_submitted_counts_only_submitted_ones():
    source = lobby("settings")
    source["seats"][1]["ballot"] = None
    del source["seats"][2]["ballot"]
    assert project_lobby(source, OBSERVER)["ballots_submitted"] == 1


def test_a_seat_sees_its_own_ballot_contents_not_merely_the_key():
    projected = project_lobby(lobby("settings"), ALICE)
    assert seat_of(projected, ALICE)["ballot"] == {"map_type": "pangaea"}


def test_projection_never_mutates_the_stored_document():
    source = lobby("draft", "blind")
    project_lobby(source, ALICE)
    assert source["pool_appearances"] == ["LEADER_A", "LEADER_B", "LEADER_C"]
    assert all("pool" in seat for seat in source["seats"])
    assert all("pick" in seat for seat in source["seats"])


def test_projection_adds_no_keys_beyond_the_participation_count():
    source = lobby("settings")
    added = set(project_lobby(source, ALICE)) - set(source)
    assert added == {"ballots_submitted"}
