from __future__ import annotations

import pytest

from app.features.match_setups.tally import resolve_settings, seeded_index

DRAFT_ID = "652f1a2b3c4d5e6f7a8b9c0d"


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
    assert resolve_settings(seats, BALLOT, DRAFT_ID) == {"map": "b", "speed": "fast"}


def test_both_halves_of_an_approval_are_counted():
    seats = [seat("x", map="a|b", speed="fast"), seat("y", map="b", speed="fast")]
    assert resolve_settings(seats, BALLOT, DRAFT_ID)["map"] == "b"


def test_a_question_not_answered_by_everyone_locks_to_its_default():
    seats = [seat("x", map="b", speed="fast"), seat("y", speed="fast")]
    assert resolve_settings(seats, BALLOT, DRAFT_ID) == {"map": "a", "speed": "fast"}


def test_an_empty_vote_resolves_entirely_to_defaults():
    assert resolve_settings([], BALLOT, DRAFT_ID) == {"map": "a", "speed": "slow"}


def test_an_unseated_seat_is_not_a_voter():
    # Otherwise a gap would make unanimity unreachable and every question
    # would default forever (gaps are legal).
    seats = [seat("x", map="b", speed="fast"), {"seat_index": 1, "discord_id": None}]
    assert resolve_settings(seats, BALLOT, DRAFT_ID)["map"] == "b"


def test_an_option_the_catalogue_no_longer_offers_cannot_win():
    seats = [seat("x", map="retired", speed="fast")]
    assert resolve_settings(seats, BALLOT, DRAFT_ID)["map"] == "a"


def test_a_tie_resolves_the_same_way_every_time():
    seats = [seat("x", map="a", speed="fast"), seat("y", map="b", speed="slow")]
    answers = {resolve_settings(seats, BALLOT, DRAFT_ID)["map"] for _ in range(50)}
    assert len(answers) == 1


def test_a_tie_does_not_depend_on_the_order_votes_arrived():
    forward = [seat("x", map="a", speed="fast"), seat("y", map="b", speed="slow")]
    reversed_ = [seat("y", speed="slow", map="b"), seat("x", speed="fast", map="a")]
    assert resolve_settings(forward, BALLOT, DRAFT_ID) == resolve_settings(
        reversed_, BALLOT, DRAFT_ID
    )


def test_two_drafts_do_not_always_break_a_tie_the_same_way():
    # Seeding on the draft is pointless if every draft lands on index 0.
    picks = {seeded_index(f"draft{n}", "map", 3) for n in range(40)}
    assert picks == {0, 1, 2}


@pytest.mark.parametrize("count", [1, 2, 3, 7, 15])
def test_the_seed_always_indexes_inside_the_tied_options(count):
    assert 0 <= seeded_index(DRAFT_ID, "map", count) < count


def test_every_question_gets_an_answer():
    # A phase advancing on "all submitted" cannot leave a setting unresolved.
    seats = [seat("x", map="b")]
    assert set(resolve_settings(seats, BALLOT, DRAFT_ID)) == {"map", "speed"}
