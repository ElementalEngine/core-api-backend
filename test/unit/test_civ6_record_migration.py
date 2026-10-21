"""Entry 10's correction of civ6 player records, and the tally rebuild's reading
of whatever it leaves behind.

Production holds three shapes the parser no longer writes. Each case here is
one of them, measured on prod: the correction must reach tokens where the
mapping allows, and leave alone what carries no information.
"""

from __future__ import annotations

import pytest

from scripts.migrate_civ6_leader_field import correction
from scripts.rebuild_civ_tallies import read_tokens

ROWS = [
    {"token": "LEADER_TRAJAN", "name": "RomeTrajan", "civ": "CIVILIZATION_ROME"},
    {"token": "LEADER_JFD_OLYMPIAS", "name": "Olympias"},
]
BY_TOKEN = {r["token"]: r for r in ROWS}
BY_NAME = {r["name"]: r for r in ROWS}


@pytest.mark.parametrize(
    "civ,leader,expected",
    [
        # leader token in civ, no leader
        ("LEADER_TRAJAN", None, ("CIVILIZATION_ROME", "LEADER_TRAJAN")),
        ("LEADER_JFD_OLYMPIAS", None, None),
        ("LEADER_UNSEEN", None, None),
        # name key in both fields
        ("RomeTrajan", "RomeTrajan", ("CIVILIZATION_ROME", "LEADER_TRAJAN")),
        ("Olympias", "Olympias", ("LEADER_JFD_OLYMPIAS", None)),
        ("Unseen", "Unseen", None),
        # no information
        ("Unknown", None, None),
        ("None", "None", None),
        ("", None, None),
        # already corrected
        ("CIVILIZATION_ROME", "LEADER_TRAJAN", None),
    ],
)
def test_each_shape_reaches_tokens_or_is_left(civ, leader, expected):
    assert correction(civ, leader, BY_NAME, BY_TOKEN) == expected


def test_a_leader_without_a_civ_lands_in_the_shape_a_later_run_resumes():
    first = correction("Olympias", "Olympias", BY_NAME, BY_TOKEN)
    assert first == ("LEADER_JFD_OLYMPIAS", None)
    with_civ = {**BY_TOKEN, "LEADER_JFD_OLYMPIAS": {**ROWS[1], "civ": "CIVILIZATION_X"}}
    assert correction(*first, BY_NAME, with_civ) == (
        "CIVILIZATION_X",
        "LEADER_JFD_OLYMPIAS",
    )


def test_a_second_run_changes_nothing():
    fixed = correction("RomeTrajan", "RomeTrajan", BY_NAME, BY_TOKEN)
    assert fixed is not None
    assert correction(*fixed, BY_NAME, BY_TOKEN) is None


@pytest.mark.parametrize(
    "player,expected",
    [
        (
            {"civ": "CIVILIZATION_ROME", "leader": "LEADER_TRAJAN"},
            ("CIVILIZATION_ROME", "LEADER_TRAJAN"),
        ),
        ({"civ": "LEADER_JFD_OLYMPIAS", "leader": None}, (None, "LEADER_JFD_OLYMPIAS")),
        ({"civ": "Unknown"}, (None, None)),
        ({"civ": "None", "leader": "None"}, (None, None)),
        ({"civ": "RomeTrajan", "leader": "RomeTrajan"}, (None, None)),
        ({}, (None, None)),
    ],
)
def test_the_rebuild_counts_tokens_and_nothing_else(player, expected):
    assert read_tokens(player) == expected
