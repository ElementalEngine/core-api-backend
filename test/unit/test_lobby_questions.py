from __future__ import annotations

import pytest

from app.features.lobbies.questions import (
    EDITIONS,
    UnknownBallot,
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
    for question in questions_for(edition, game_type):
        assert len(question["options"]) >= 2, question["id"]
        assert question["title"], question["id"]
        for option in question["options"]:
            assert option["label"], f"{question['id']}/{option['id']}"


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_draft_mode_is_asked_last_and_only_where_it_is_voted_on(edition, game_type):
    # A teamer picks standard or cwc when the lobby is created, so its ballot
    # has no draft-mode question at all.
    asked = [question["id"] for question in questions_for(edition, game_type)]
    if game_type == "teamer":
        assert "draft_mode" not in asked
        return
    assert asked[-1] == "draft_mode"
    assert asked.count("draft_mode") == 1


@pytest.mark.parametrize("edition", EDITIONS)
def test_duel_offers_standard_and_random_only(edition):
    modes = questions_for(edition, "duel")[-1]
    assert [option["id"] for option in modes["options"]] == ["standard", "random"]


@pytest.mark.parametrize("edition", EDITIONS)
def test_no_ballot_offers_cwc(edition):
    # cwc is chosen when the lobby is made, so no ballot offers it. `cwc` is
    # also a civ6 timer option, so only the draft-mode question is checked.
    for game_type in GAME_TYPES:
        modes = [
            q for q in questions_for(edition, game_type) if q["id"] == "draft_mode"
        ]
        for question in modes:
            assert "cwc" not in {o["id"] for o in question["options"]}, game_type


@pytest.mark.parametrize("edition", EDITIONS)
def test_random_is_offered_wherever_the_mode_is_voted_on(edition):
    for game_type in ("ffa", "duel"):
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
    questions_for("civ6", "ffa")[0]["title"] = "MUTATED"
    assert questions_for("civ6", "ffa")[0]["title"] != "MUTATED"


@pytest.mark.parametrize(
    ("edition", "game_type"), [("civ5", "ffa"), ("civ6", "battle-royale")]
)
def test_an_unknown_ballot_is_refused(edition, game_type):
    with pytest.raises(UnknownBallot):
        questions_for(edition, game_type)
