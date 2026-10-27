from __future__ import annotations

import pytest

from app.features.match_setups.tally import seeded_index, voted_settings

SEED = "652f1a2b3c4d5e6f7a8b9c0d"
PLAYERS = {"x", "y", "z"}


def question(qid, options, default):
    return {
        "id": qid,
        "title": qid,
        "options": [{"id": o, "emoji": "x", "label": o} for o in options],
        "default_option_id": default,
    }


MAP = question("map", ["a", "b", "c"], "a")
SPEED = question("speed", ["fast", "slow"], "slow")


def test_the_option_most_players_chose_wins_and_an_unvoted_one_defaults():
    votes = {"map": {"b": ["x", "y"], "c": ["z"]}}
    assert voted_settings([MAP, SPEED], votes, PLAYERS, SEED) == {
        "map": "b",
        "speed": "slow",
    }


def test_a_player_may_back_several_options_but_counts_once_on_each():
    votes = {"map": {"a": ["x", "x"], "b": ["x", "y"]}}
    assert voted_settings([MAP], votes, PLAYERS, SEED) == {"map": "b"}


def test_only_players_count_and_retired_options_never_win():
    votes = {"map": {"c": ["spectator", "bot"], "retired": ["x", "y"], "b": ["z"]}}
    assert voted_settings([MAP], votes, PLAYERS, SEED) == {"map": "b"}


def test_a_tie_breaks_the_same_way_every_time_whatever_the_vote_order():
    one = {"map": {"a": ["x"], "b": ["y"]}}
    other = {"map": {"b": ["y"], "a": ["x"]}}
    picks = {voted_settings([MAP], v, PLAYERS, SEED)["map"] for v in [one, other] * 5}
    assert len(picks) == 1 and picks <= {"a", "b"}


def test_two_games_do_not_always_break_a_tie_the_same_way():
    picks = {seeded_index(f"game{n}", "map", 3) for n in range(40)}
    assert picks == {0, 1, 2}


@pytest.mark.parametrize("count", [1, 2, 3, 7, 15])
def test_the_seed_always_indexes_inside_the_tied_options(count):
    assert 0 <= seeded_index(SEED, "map", count) < count
