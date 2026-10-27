from __future__ import annotations

import pytest

from app.features.match_setups.bans import ban_caps, capped, voted_bans


def test_civ6_caps_leaders_at_twenty_and_has_no_civ_bans():
    assert ban_caps("civ6", None) == {"leader": 20, "civ": 0}


@pytest.mark.parametrize("age", [None, "none", "None"])
def test_civ7_allows_ten_civ_bans_when_no_starting_age_is_chosen(age):
    assert ban_caps("civ7", age) == {"leader": 10, "civ": 10}


@pytest.mark.parametrize("age", ["antiquity", "exploration", "modern"])
def test_civ7_drops_to_three_civ_bans_once_an_age_is_chosen(age):
    # Read from the settings the previous phase resolved, never from the
    # request -- the first place one phase's outcome bounds the next.
    assert ban_caps("civ7", age) == {"leader": 10, "civ": 3}


def test_the_cap_drops_every_key_tied_at_the_boundary():
    counts = {f"L{i}": 7 for i in range(14)} | {"T1": 6, "T2": 6, "T3": 6}
    landed = capped(counts, list(counts), 15)
    assert len(landed) == 14
    assert not {"T1", "T2", "T3"} & set(landed)


def test_every_key_tied_above_the_cap_lands_nothing():
    flat = {f"F{i}": 6 for i in range(20)}
    assert capped(flat, list(flat), 15) == []


def test_ties_below_the_cap_are_harmless():
    counts = {"a": 6, "b": 6, "c": 6}
    assert capped(counts, list(counts), 15) == ["a", "b", "c"]


def test_the_cap_keeps_the_most_voted():
    counts = {"low": 5, "high": 9, "mid": 7}
    assert capped(counts, list(counts), 2) == ["high", "mid"]


def test_a_zero_cap_lands_nothing():
    assert capped({"a": 99}, ["a"], 0) == []


PLAYERS = {"a", "b", "c", "d"}


def test_a_voted_ban_needs_a_majority_of_all_the_players():
    votes = {"gandhi": ["a", "b"], "trajan": ["a", "b", "c"]}
    assert voted_bans(votes, PLAYERS, ["gandhi", "trajan"], 20) == ["trajan"]
    assert voted_bans(votes, PLAYERS - {"d"}, ["gandhi", "trajan"], 20) == [
        "trajan",
        "gandhi",
    ]


def test_only_players_and_bannable_leaders_count_and_the_room_caps_them():
    votes = {
        "gandhi": ["a", "b", "x", "y"],
        "taken": ["a", "b", "c"],
        "trajan": ["a", "b", "c", "d"],
        "rome": ["a", "b", "c"],
    }
    allowed = ["gandhi", "trajan", "rome"]
    assert voted_bans(votes, PLAYERS, allowed, 20) == ["trajan", "rome"]
    assert voted_bans(votes, PLAYERS, allowed, 1) == ["trajan"]
    assert voted_bans(votes, PLAYERS, allowed, 0) == []
    assert voted_bans({}, set(), allowed, 20) == []
