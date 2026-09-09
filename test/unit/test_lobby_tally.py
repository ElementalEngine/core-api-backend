"""The settings tally.

⚠ This is arithmetic that can be wrong while every other test passes: it runs
once per lobby, writes a result nobody re-derives, and a mistake looks like a
game people merely disagreed with.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.tally import resolve_settings, seeded_index

LOBBY = "652f1a2b3c4d5e6f7a8b9c0d"


def question(qid, options, default, **extra):
    return {
        "id": qid,
        "title": qid,
        "options": [{"id": o, "emoji": "x", "label": o} for o in options],
        "default_option_id": default,
        **extra,
    }


MAP = question("map", ["a", "b", "c"], "a", max_selections=2)
SPEED = question("speed", ["fast", "slow"], "slow")
BALLOT = [MAP, SPEED]


def seat(player, **ballot):
    return {"seat_index": 0, "discord_id": player, "ballot": ballot or None}


def test_the_option_with_most_votes_wins():
    seats = [
        seat("x", map="b", speed="fast"),
        seat("y", map="b", speed="slow"),
        seat("z", map="c", speed="fast"),
    ]
    assert resolve_settings(seats, BALLOT, LOBBY) == {"map": "b", "speed": "fast"}


def test_both_halves_of_an_approval_are_counted():
    # ⚠ `a|b` is TWO votes, not half a vote each and not just the first. With
    # only the first counted this is a 1-1 tie and the tie-break decides;
    # b winning outright is what proves the second selection landed.
    seats = [seat("x", map="a|b", speed="fast"), seat("y", map="b", speed="fast")]
    assert resolve_settings(seats, BALLOT, LOBBY)["map"] == "b"


def test_a_question_not_answered_by_everyone_locks_to_its_default():
    # ⚠ v1's rule, carried deliberately (D191): stragglers decide nothing.
    # x voted for `b` and gets `a` anyway, because y never answered.
    seats = [seat("x", map="b", speed="fast"), seat("y", speed="fast")]
    assert resolve_settings(seats, BALLOT, LOBBY) == {"map": "a", "speed": "fast"}


def test_an_empty_lobby_resolves_entirely_to_defaults():
    assert resolve_settings([], BALLOT, LOBBY) == {"map": "a", "speed": "slow"}


def test_an_unseated_seat_is_not_a_voter():
    # Otherwise a gap would make unanimity unreachable and every question
    # would default forever (section 4: gaps are legal).
    seats = [seat("x", map="b", speed="fast"), {"seat_index": 1, "discord_id": None}]
    assert resolve_settings(seats, BALLOT, LOBBY)["map"] == "b"


def test_an_option_the_catalogue_no_longer_offers_cannot_win():
    # The catalogue changes by release (D192) and a lobby can be mid-vote
    # across one. A retired option must not win on votes cast before it went.
    seats = [seat("x", map="retired", speed="fast")]
    assert resolve_settings(seats, BALLOT, LOBBY)["map"] == "a"


def test_a_tie_resolves_the_same_way_every_time():
    seats = [seat("x", map="a", speed="fast"), seat("y", map="b", speed="slow")]
    answers = {resolve_settings(seats, BALLOT, LOBBY)["map"] for _ in range(50)}
    assert len(answers) == 1


def test_a_tie_does_not_depend_on_the_order_votes_arrived():
    # ⚠ The property the whole tie-break exists for. Without sorting the tied
    # options the answer follows dict iteration order -- stable inside one
    # process, different after a restart, and impossible to dispute.
    forward = [seat("x", map="a", speed="fast"), seat("y", map="b", speed="slow")]
    reversed_ = [seat("y", speed="slow", map="b"), seat("x", speed="fast", map="a")]
    assert resolve_settings(forward, BALLOT, LOBBY) == resolve_settings(
        reversed_, BALLOT, LOBBY
    )


def test_two_lobbies_do_not_always_break_a_tie_the_same_way():
    # Seeding on the lobby is pointless if every lobby lands on index 0.
    picks = {seeded_index(f"lobby{n}", "map", 3) for n in range(40)}
    assert picks == {0, 1, 2}


def test_the_seed_is_stable_for_one_lobby_and_question():
    assert seeded_index(LOBBY, "map", 3) == seeded_index(LOBBY, "map", 3)


@pytest.mark.parametrize("count", [1, 2, 3, 7, 15])
def test_the_seed_always_indexes_inside_the_tied_options(count):
    assert 0 <= seeded_index(LOBBY, "map", count) < count


def test_every_question_gets_an_answer():
    # A phase advancing on "all submitted" cannot leave a setting unresolved.
    seats = [seat("x", map="b")]
    assert set(resolve_settings(seats, BALLOT, LOBBY)) == {"map", "speed"}
