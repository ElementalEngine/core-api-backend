"""Turn order for snake and CWC.

⚠ The failure this prevents is silent: without it `PUT /picks` accepts a pick
from any seat at any moment, which is correct for three of five modes and
wrong for two. Nothing errors -- the draft just stops being a draft.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.turns import (
    CWC_PICK_ORDER,
    cwc_order,
    is_turn_ordered,
    snake_order,
    whose_turn,
)

PLAYERS = ["a", "b", "c", "d"]


def test_one_round_is_a_single_pass_in_seat_order():
    # civ6 drafts leaders alone, so this is the whole of snake there.
    assert snake_order(PLAYERS, 1) == ["a", "b", "c", "d"]


def test_the_second_round_runs_backwards():
    # ⚠ The snake is the reversal BETWEEN rounds, not within one. It is what
    # stops the first seat taking the best of both leader and civ.
    assert snake_order(PLAYERS, 2) == ["a", "b", "c", "d", "d", "c", "b", "a"]


def test_every_player_picks_once_per_round():
    order = snake_order(PLAYERS, 2)
    assert len(order) == 8
    assert all(order.count(player) == 2 for player in PLAYERS)


def test_no_rounds_is_no_turns():
    assert snake_order(PLAYERS, 0) == []


@pytest.mark.parametrize(
    ("picks", "expected"),
    [
        (4, ["cap0", "cap1", "cap1", "cap0"]),
        (6, ["cap0", "cap1", "cap1", "cap0", "cap1", "cap0"]),
    ],
)
def test_cwc_follows_mites_table(picks, expected):
    # ⚠ Carried verbatim: it is not a plain alternation and not derivable
    # from a rule, which is why it is a table.
    assert cwc_order(["cap0", "cap1"], picks) == expected


def test_the_cwc_table_is_balanced():
    # Sixteen entries, eight each -- otherwise one team drafts more often.
    assert len(CWC_PICK_ORDER) == 16
    assert CWC_PICK_ORDER.count(0) == CWC_PICK_ORDER.count(1) == 8


@pytest.mark.parametrize("captains", [["only"], ["a", "b", "c"], []])
def test_cwc_needs_exactly_two_captains(captains):
    with pytest.raises(ValueError):
        cwc_order(captains, 4)


def test_a_draft_longer_than_the_table_is_refused_not_wrapped():
    # Wrapping would silently give a longer draft an ordering nobody chose.
    with pytest.raises(ValueError):
        cwc_order(["cap0", "cap1"], len(CWC_PICK_ORDER) + 1)


def test_whose_turn_walks_the_order_and_then_stops():
    order = snake_order(PLAYERS, 2)
    assert [whose_turn(order, i) for i in (0, 3, 4, 7)] == ["a", "d", "d", "a"]
    assert whose_turn(order, 8) is None
    assert whose_turn(order, -1) is None


@pytest.mark.parametrize(
    ("mode", "ordered"),
    [
        ("snake", True),
        ("cwc", True),
        ("standard", False),
        ("blind", False),
        ("random", False),
        (None, False),
    ],
)
def test_only_snake_and_cwc_are_turn_ordered(mode, ordered):
    assert is_turn_ordered(mode) is ordered
