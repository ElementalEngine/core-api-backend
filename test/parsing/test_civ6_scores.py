"""Scores come from the save's own score history, and set the starting order.

What should break this file: a score read for the wrong seat, an order that
ignores who was knocked out, or a history that does not line up being used.
"""

import os

import pytest

from app.features.matches.parsers import civ6
from app.features.matches.parsers.civ6_scores import final_scores

_SAVES = os.path.join(os.path.dirname(__file__), "..", "data", "civ6TestSaves")


def _read(name):
    path = os.path.join(_SAVES, name)
    if not os.path.exists(path):
        pytest.skip(f"{name} not present")
    with open(path, "rb") as f:
        return f.read()


def _order(name):
    players = civ6.parse_civ6_save(_read(name))["players"]
    return [
        (p["placement"], p["user_name"], p["score"], p["player_alive"])
        for p in sorted(players, key=lambda p: (p["placement"], p["team"]))
    ]


def test_an_ffa_is_ordered_by_score_then_by_who_was_knocked_out_last():
    assert _order("10playerFFA.Civ6Save") == [
        (0, "R1sky Business", 1417, True),
        (1, "Under", 1361, True),
        (2, "Alex", 1344, True),
        (3, "The Largest Goku Black", 968, True),
        (4, "Toilet the Small", 955, True),
        (5, "Crax_is_Bax", 494, True),
        # Out on turns 104, 99, 91 and 80: a higher score does not lift Saratoga.
        (6, "iLLmatic", 376, False),
        (7, "Munch [icon_barbarian]", 289, False),
        (8, "Saratoga", 304, False),
        (9, "General Zalyzhnii", 199, False),
    ]


def test_a_team_places_by_its_members_total_and_spectators_are_left_out():
    order = _order("realtimeTeamer.Civ6Save")
    assert len(order) == 8
    totals = {}
    for place, _, score, _ in order:
        totals[place] = totals.get(place, 0) + score
    assert totals == {0: 1181, 1: 1156}


def test_a_first_turn_save_keeps_its_team_order():
    players = civ6.parse_civ6_save(_read("5team.Civ6Save"))["players"]
    assert {p["score"] for p in players} == {0}
    assert all(p["placement"] == p["team"] for p in players)


def test_a_history_that_does_not_line_up_is_not_used():
    data = _read("10playerFFA.Civ6Save")
    alive = [True, True, True, False, True, False, True, True, False, False]
    assert final_scores(data, alive, 106) is not None
    # A seat the save calls alive whose history stopped early, one civ too
    # many, or a turn past the save's: each leaves the save without scores.
    assert final_scores(data, [True] * 10, 106) is None
    assert final_scores(data, alive + [True], 106) is None
    assert final_scores(data, alive, 100) is None
    assert final_scores(data[:200_000], alive, 106) is None
    assert final_scores(b"CIV6", alive, 106) is None
