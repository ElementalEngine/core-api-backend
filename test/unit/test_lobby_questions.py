"""The settings ballot catalogue.

⚠ Two defects in authored data are invisible until a lobby is mid-vote: a
`default_option_id` naming no option, so the tally locks a setting to an id
nothing recognises, and a duplicate question id, so one ballot key silently
overwrites another. Both are cheap to assert and expensive to find later.
"""

from __future__ import annotations

import pytest

from app.features.lobbies.questions import (
    EDITIONS,
    UnknownBallot,
    question_ids,
    questions_for,
)

GAME_TYPES = ("ffa", "duel", "teamer")
EVERY_BALLOT = [(e, g) for e in EDITIONS for g in GAME_TYPES]


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_every_default_names_a_real_option(edition, game_type):
    for question in questions_for(edition, game_type):
        ids = [option["id"] for option in question["options"]]
        assert question["default_option_id"] in ids, question["id"]


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_question_ids_are_unique_within_a_ballot(edition, game_type):
    asked = [question["id"] for question in questions_for(edition, game_type)]
    assert len(asked) == len(set(asked))


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_every_question_is_answerable(edition, game_type):
    # A question with one option is a statement, and one with none cannot be
    # voted on at all -- either would stall a phase that advances on "all
    # submitted" (section 7).
    for question in questions_for(edition, game_type):
        assert len(question["options"]) >= 2, question["id"]
        assert question["title"], question["id"]
        for option in question["options"]:
            assert option["label"], f"{question['id']}/{option['id']}"


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_draft_mode_is_asked_last_and_exactly_once(edition, game_type):
    asked = [question["id"] for question in questions_for(edition, game_type)]
    assert asked[-1] == "draft_mode"
    assert asked.count("draft_mode") == 1


@pytest.mark.parametrize("edition", EDITIONS)
def test_duel_offers_standard_and_random_only(edition):
    # ⚠ D193, and a deliberate divergence from v1 in both directions: Mite
    # hands duel the FFA list, snake and blind included, and the spec's mode
    # matrix said standard and CWC. Neither is what duel plays.
    modes = questions_for(edition, "duel")[-1]
    assert [option["id"] for option in modes["options"]] == ["standard", "random"]


@pytest.mark.parametrize("edition", EDITIONS)
def test_cwc_is_offered_to_teamer_and_nowhere_else(edition):
    for game_type in GAME_TYPES:
        offered = {o["id"] for o in questions_for(edition, game_type)[-1]["options"]}
        assert ("cwc" in offered) is (game_type == "teamer"), game_type


@pytest.mark.parametrize("edition", EDITIONS)
def test_random_is_offered_everywhere(edition):
    # It skips the draft phase entirely rather than running turns (D193), so
    # it is the one mode every game type can always fall back to.
    for game_type in GAME_TYPES:
        offered = {o["id"] for o in questions_for(edition, game_type)[-1]["options"]}
        assert "random" in offered, game_type


def test_multi_select_is_capped_below_the_option_count():
    # `map` is the only multi-select in either edition. A cap at or above the
    # option count would let a seat approve everything, which is not a vote.
    capped = [
        (e, g, q)
        for e, g in EVERY_BALLOT
        for q in questions_for(e, g)
        if "max_selections" in q
    ]
    assert capped, "the fixture must find a multi-select or this asserts nothing"
    for edition, game_type, question in capped:
        assert 2 <= question["max_selections"] < len(question["options"]), (
            f"{edition}/{game_type}/{question['id']}"
        )


def test_the_catalogue_hands_out_a_copy():
    # Cached for the process lifetime (D171's reason): a caller that mutated a
    # question would change what every later lobby is asked.
    questions_for("civ6", "ffa")[0]["title"] = "MUTATED"
    assert questions_for("civ6", "ffa")[0]["title"] != "MUTATED"


def test_question_ids_matches_the_ballot():
    assert question_ids("civ6", "teamer") == {
        q["id"] for q in questions_for("civ6", "teamer")
    }


@pytest.mark.parametrize(
    ("edition", "game_type"), [("civ5", "ffa"), ("civ6", "battle-royale")]
)
def test_an_unknown_ballot_is_refused(edition, game_type):
    with pytest.raises(UnknownBallot):
        questions_for(edition, game_type)
