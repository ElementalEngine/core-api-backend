"""Turn order. CWC is the only mode that has turns.

⚠ Without this, a pick is accepted from any seat at any moment -- and nothing
errors, the draft just stops being a draft.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.turns import CWC_PICK_ORDER, cwc_order, whose_turn

CAPTAINS = ["cap0", "cap1"]


@pytest.mark.parametrize(
    ("turns", "expected"),
    [
        (4, ["cap0", "cap1", "cap1", "cap0"]),
        (8, ["cap0", "cap1", "cap1", "cap0", "cap1", "cap0", "cap0", "cap1"]),
    ],
)
def test_the_order_follows_the_table(turns, expected):
    # ⚠ Carried verbatim from Mite and confirmed against civup: not a plain
    # alternation and not derivable from a rule, which is why it is a table.
    assert cwc_order(CAPTAINS, turns) == expected


@pytest.mark.parametrize("team_size", [2, 4, 6, 8])
def test_neither_team_ever_acts_more_often_than_the_other(team_size):
    # ⚠ Bans and picks each slice to team_size * 2, so an uneven slice would
    # hand one team an extra ban AND an extra pick.
    order = cwc_order(CAPTAINS, team_size * 2)
    assert order.count("cap0") == order.count("cap1") == team_size


def test_the_table_itself_is_balanced():
    assert len(CWC_PICK_ORDER) == 16
    assert CWC_PICK_ORDER.count(0) == CWC_PICK_ORDER.count(1) == 8


@pytest.mark.parametrize("captains", [["only"], ["a", "b", "c"], []])
def test_cwc_needs_exactly_two_captains(captains):
    with pytest.raises(ValueError):
        cwc_order(captains, 4)


def test_a_draft_longer_than_the_table_is_refused_not_wrapped():
    # Wrapping would give a longer draft an ordering nobody chose. The table
    # covers 8v8, which is past the largest team CPL plays.
    with pytest.raises(ValueError):
        cwc_order(CAPTAINS, len(CWC_PICK_ORDER) + 1)


def test_whose_turn_walks_the_order_and_then_stops():
    order = cwc_order(CAPTAINS, 4)
    assert [whose_turn(order, i) for i in (0, 1, 3)] == ["cap0", "cap1", "cap0"]
    assert whose_turn(order, 4) is None
    assert whose_turn(order, -1) is None
