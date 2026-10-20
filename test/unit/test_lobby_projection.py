"""The visibility table, row by row.

Every combination of phase, draft mode and viewer, checked against what the
table says that viewer may see. Blind draft has an explicit case asserting the
pool is absent, not merely the pick.

Governed by D73, D86, D167, D235.
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
        "settings": {"draft_mode": draft_mode},
        "revealed_at": revealed_at,
        "pool_appearances": ["LEADER_A", "LEADER_B", "LEADER_C"],
        "seats": [
            {
                "seat_index": i,
                "discord_id": who,
                "ballot": {"map_type": "pangaea"},
                "pool": [f"LEADER_{who.upper()}"],
                "pick": f"LEADER_{who.upper()}",
                "bans": {"leader_keys": [f"LEADER_{who.upper()}"], "civ_keys": []},
            }
            for i, who in enumerate((ALICE, BOB, CAROL))
        ],
    }


def seat_of(projected, who):
    return next(s for s in projected["seats"] if s["discord_id"] == who)


def holders_of(projected, field):
    return {s["discord_id"] for s in projected["seats"] if field in s}


ALL = {ALICE, BOB, CAROL}

# (phase, draft_mode, revealed_at, viewer, who may see a ballot, a pool, a pick)
TABLE = [
    ("lobby", "standard", None, ALICE, ALL, ALL, ALL),
    ("settings", "standard", None, ALICE, {ALICE}, ALL, ALL),
    ("settings", "standard", None, BOB, {BOB}, ALL, ALL),
    ("settings", "standard", None, OBSERVER, set(), ALL, ALL),
    ("settings", "standard", None, STRANGER, set(), ALL, ALL),
    ("settings", "blind", None, ALICE, {ALICE}, ALL, ALL),
    ("bans", "standard", None, ALICE, ALL, ALL, ALL),
    ("bans", "standard", None, OBSERVER, ALL, ALL, ALL),
    ("draft", "standard", None, ALICE, ALL, ALL, {ALICE}),
    ("draft", "standard", None, OBSERVER, ALL, ALL, set()),
    ("draft", "snake", None, OBSERVER, ALL, ALL, set()),
    ("draft", "cwc", None, OBSERVER, ALL, ALL, set()),
    ("draft", "blind", None, ALICE, ALL, {ALICE}, {ALICE}),
    ("draft", "blind", None, CAROL, ALL, {CAROL}, {CAROL}),
    ("draft", "blind", None, OBSERVER, ALL, set(), set()),
    ("draft", "blind", None, STRANGER, ALL, set(), set()),
    ("draft", "blind", "T", OBSERVER, ALL, ALL, set()),
    ("complete", "standard", None, OBSERVER, ALL, ALL, ALL),
    ("complete", "blind", None, OBSERVER, ALL, ALL, ALL),
    ("cancelled", "blind", None, OBSERVER, ALL, ALL, ALL),
]


@pytest.mark.parametrize(
    "phase,mode,revealed,viewer,ballots,pools,picks",
    TABLE,
    ids=[
        f"{r[0]}-{r[1]}-{r[3] or 'observer'}-{'revealed' if r[2] else 'hidden'}"
        for r in TABLE
    ],
)
def test_d73_row(phase, mode, revealed, viewer, ballots, pools, picks):
    projected = project_lobby(lobby(phase, mode, revealed), viewer)
    assert holders_of(projected, "ballot") == ballots
    assert holders_of(projected, "pool") == pools
    # A pick is never shown before the pool it came from: hiding the pool
    # but not the pick is the elimination leak the spec names.
    assert holders_of(projected, "pick") == picks
    assert picks <= pools


def test_a_pick_is_withheld_but_its_ready_flag_is_not():
    # Players still see who has locked in -- only the choice waits.
    source = lobby("draft")
    for seat in source["seats"]:
        seat["ready"] = True
        seat["civ_pick"] = "CIVILIZATION_X"
    projected = project_lobby(source, ALICE)
    assert holders_of(projected, "civ_pick") == {ALICE}
    assert all(seat["ready"] for seat in projected["seats"])


def test_bans_submitted_together_stay_private_until_they_resolve():
    assert holders_of(project_lobby(lobby("bans"), ALICE), "bans") == {ALICE}
    assert holders_of(project_lobby(lobby("bans"), OBSERVER), "bans") == set()
    # Banning in turns is public: each ban lands as it is made.
    in_turns = {**lobby("bans"), "ban_order": [ALICE, BOB]}
    assert holders_of(project_lobby(in_turns, ALICE), "bans") == ALL
    assert holders_of(project_lobby(lobby("draft"), ALICE), "bans") == ALL


def test_an_uncensored_view_keeps_everything():
    projected = project_lobby(lobby("draft", "blind"), OBSERVER, censored=False)
    assert holders_of(projected, "pick") == ALL
    assert holders_of(projected, "pool") == ALL
    assert "pool_appearances" in projected


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
