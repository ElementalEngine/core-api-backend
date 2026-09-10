"""What a finished lobby contributes to `lobby_stats`.

⚠ These counters are a permanent record that nothing recomputes. A double
count or a wrong denominator is invisible and lasts forever -- the same class
of problem D60's ledger reconciliation exists for.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.stats import contributions

BANNED = {"leader": ["L_BAN"], "civ": []}


def lobby(mode, **rest):
    return {
        "phase": "complete",
        "bans": BANNED,
        "settings": {"draft_mode": mode},
        **rest,
    }


def test_a_seat_pick_counts_once_and_its_pool_counts_as_offered():
    out = contributions(
        lobby(
            "standard",
            seats=[
                {"discord_id": "a", "pick": "L1", "pool": ["L1", "L2"]},
                {"discord_id": "b", "pick": "L3", "pool": ["L3", "L4"]},
            ],
        )
    )
    assert out["L1"] == {"picks": 1, "bans": 0, "pool_appearances": 1}
    assert out["L2"] == {"picks": 0, "bans": 0, "pool_appearances": 1}


def test_a_banned_token_never_appears_in_a_pool():
    # ⚠ The denominator's integrity: a banned leader was never offered, so it
    # must not count as one nobody picked.
    out = contributions(lobby("standard", seats=[{"discord_id": "a", "pool": ["L1"]}]))
    assert out["L_BAN"] == {"picks": 0, "bans": 1, "pool_appearances": 0}


def test_random_contributes_no_picks_and_no_appearances():
    # ⚠ Nobody chose and nobody was offered. Counting an assignment as a pick
    # makes pick rate noise for every random game; the bans were real votes
    # and still count.
    out = contributions(
        lobby(
            "random",
            seats=[
                {"discord_id": "a", "pick": "L1"},
                {"discord_id": "b", "pick": "L2"},
            ],
        )
    )
    assert set(out) == {"L_BAN"}


def test_cwc_reads_the_team_and_the_shared_pool():
    # D199 and D201: picks live on teams[], and the pool is one shared list.
    out = contributions(
        lobby(
            "cwc",
            pool=["L1", "L2", "L3"],
            teams=[
                {"team_index": 0, "leaders": ["L1"], "civs": []},
                {"team_index": 1, "leaders": ["L2"], "civs": []},
            ],
            seats=[{"discord_id": "a"}, {"discord_id": "b"}],
        )
    )
    assert out["L1"]["picks"] == 1
    assert out["L2"]["picks"] == 1
    assert out["L3"] == {"picks": 0, "bans": 0, "pool_appearances": 1}


def test_the_shared_pool_counts_once_not_once_per_captain():
    # A token was offered to the draft once, however many turns it survived.
    out = contributions(
        lobby(
            "cwc",
            pool=["L1"],
            teams=[{"team_index": 0, "leaders": [], "civs": []}],
            seats=[{"discord_id": "a"}, {"discord_id": "b"}],
        )
    )
    assert out["L1"]["pool_appearances"] == 1


def test_civ7_counts_the_leader_and_the_civ_separately():
    out = contributions(
        lobby(
            "snake",
            seats=[
                {
                    "discord_id": "a",
                    "pick": "L1",
                    "civ_pick": "C1",
                    "pool": ["L1"],
                    "civ_pool": ["C1", "C2"],
                }
            ],
        )
    )
    assert out["L1"]["picks"] == 1
    assert out["C1"]["picks"] == 1
    assert out["C2"]["pool_appearances"] == 1


@pytest.mark.parametrize("phase", ["cancelled", "draft", "bans", "settings", "lobby"])
def test_only_a_completed_lobby_contributes(phase):
    # ⚠ A cancelled draft dealt real pools and landed real bans, but no game
    # happened, and stats describe what people play.
    out = contributions(
        {
            "phase": phase,
            "bans": BANNED,
            "settings": {"draft_mode": "standard"},
            "seats": [{"discord_id": "a", "pick": "L1", "pool": ["L1"]}],
        }
    )
    assert out == {}


def test_an_unseated_seat_contributes_nothing():
    out = contributions(
        lobby("standard", seats=[{"seat_index": 1, "discord_id": None, "pool": ["L9"]}])
    )
    assert "L9" not in out


def test_a_lobby_with_nothing_in_it_contributes_nothing():
    assert contributions({"phase": "complete"}) == {}
