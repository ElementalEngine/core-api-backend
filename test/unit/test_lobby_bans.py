"""The ban tally.

Like the settings tally, this is arithmetic that runs once per lobby and
writes a result nobody re-derives. Unlike it, a mistake here removes leaders
from a pool people are about to draft from.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.bans import ban_caps, capped, resolve_bans


def seat(player, leaders=(), civs=()):
    return {
        "seat_index": 0,
        "discord_id": player,
        "bans": {"leader_keys": list(leaders), "civ_keys": list(civs)},
    }


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


def test_a_key_lands_only_at_a_majority_of_all_seats():
    # Of ALL seats, not of submitters: a timeout creates non-submitters and
    # must not make bans easier to land.
    four = [seat("a", ["gandhi"]), seat("b", ["gandhi"]), seat("c"), seat("d")]
    assert resolve_bans(four, "civ6", None)["leader"] == []
    three = [seat("a", ["gandhi"]), seat("b", ["gandhi"]), seat("c")]
    assert resolve_bans(three, "civ6", None)["leader"] == ["gandhi"]


def test_an_unseated_seat_is_not_counted_in_the_threshold():
    seats = [
        seat("a", ["gandhi"]),
        seat("b", ["gandhi"]),
        {"seat_index": 2, "discord_id": None},
    ]
    assert resolve_bans(seats, "civ6", None)["leader"] == ["gandhi"]


def test_civ6_lands_no_civ_bans_even_when_they_are_submitted():
    seats = [seat("a", civs=["rome"]), seat("b", civs=["rome"])]
    assert resolve_bans(seats, "civ6", None)["civ"] == []


def test_leaders_and_civs_are_tallied_independently():
    seats = [
        seat("a", ["gandhi"], ["rome"]),
        seat("b", ["gandhi"], ["egypt"]),
        seat("c", ["trajan"], ["rome"]),
    ]
    landed = resolve_bans(seats, "civ7", "none")
    assert landed["leader"] == ["gandhi"]
    assert landed["civ"] == ["rome"]


def test_the_cap_drops_every_key_tied_at_the_boundary():
    # D72 verbatim: seventeen clear the threshold, the cap is fifteen, and
    # ranks 15-17 all sit on six votes -- all three drop, fourteen land.
    counts = {f"L{i}": 7 for i in range(14)} | {"T1": 6, "T2": 6, "T3": 6}
    landed = capped(counts, list(counts), 15)
    assert len(landed) == 14
    assert not {"T1", "T2", "T3"} & set(landed)


def test_every_key_tied_above_the_cap_lands_nothing():
    # The rule taken to its end. Twenty leaders on identical counts and a cap
    # of fifteen: all twenty sit at the boundary, so none land. Deliberate --
    # it errs toward the larger pool, which is what the rule is for.
    flat = {f"F{i}": 6 for i in range(20)}
    assert capped(flat, list(flat), 15) == []


def test_ties_below_the_cap_are_harmless():
    counts = {"a": 6, "b": 6, "c": 6}
    assert capped(counts, list(counts), 15) == ["a", "b", "c"]


def test_the_cap_keeps_the_most_voted():
    # The boundary only drops when something BEYOND the cap ties with it.
    # `mid` is the boundary here and nothing ties with it, so it lands --
    # dropping it unconditionally would shrink every capped result by one.
    counts = {"low": 5, "high": 9, "mid": 7}
    assert capped(counts, list(counts), 2) == ["high", "mid"]


def test_a_zero_cap_lands_nothing():
    assert capped({"a": 99}, ["a"], 0) == []


def test_nobody_banning_anything_lands_nothing():
    seats = [seat("a"), seat("b")]
    assert resolve_bans(seats, "civ7", "none") == {"leader": [], "civ": []}


def test_an_empty_lobby_lands_nothing():
    assert resolve_bans([], "civ6", None) == {"leader": [], "civ": []}
