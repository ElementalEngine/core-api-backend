"""What a finished lobby contributes to `lobby_stats`.

These counters are a permanent record that nothing recomputes. A double
count is invisible and lasts forever -- the same class of problem ledger
reconciliation exists for.

Governed by D60.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.stats import contributions

BANNED = {"leader": ["L_BAN"], "civ": ["C_BAN"]}


def lobby(**rest):
    return {
        "phase": "complete",
        "bans": BANNED,
        "settings": {"draft_mode": "standard"},
        **rest,
    }


def test_each_landed_ban_counts_once():
    assert contributions(lobby()) == {"L_BAN": {"bans": 1}, "C_BAN": {"bans": 1}}


def test_picks_and_pools_are_not_counted():
    out = contributions(
        lobby(
            pool=["L3"],
            teams=[{"team_index": 0, "leaders": ["L4"], "civs": ["C4"]}],
            seats=[
                {
                    "discord_id": "a",
                    "pick": "L1",
                    "civ_pick": "C1",
                    "pool": ["L1", "L2"],
                    "civ_pool": ["C1", "C2"],
                }
            ],
        )
    )
    assert set(out) == {"L_BAN", "C_BAN"}


@pytest.mark.parametrize("phase", ["cancelled", "draft", "bans", "settings", "lobby"])
def test_only_a_completed_lobby_contributes(phase):
    # A cancelled draft landed real bans, but no game happened, and stats
    # describe what people play.
    assert contributions(lobby(phase=phase)) == {}


def test_a_lobby_with_no_bans_contributes_nothing():
    assert contributions({"phase": "complete"}) == {}
    assert contributions(lobby(bans={"leader": [], "civ": []})) == {}
