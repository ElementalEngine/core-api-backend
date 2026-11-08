from __future__ import annotations

import pytest

from app.core.constants import EDITIONS
from app.features.match_setups.questions import (
    UnknownBallot,
    preset,
    questions_for,
    settings_for,
)
from app.features.match_setups.schemas import SettingQuestion

GAME_TYPES = ("ffa", "duel", "teamer")
EVERY_BALLOT = [(e, g) for e in EDITIONS for g in GAME_TYPES]


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_every_default_names_a_real_option_and_only_the_map_has_none(
    edition, game_type
):
    for question in questions_for(edition, game_type):
        ids = [option["id"] for option in question["options"]]
        if question["id"] == "map":
            assert question["default_option_id"] is None
        else:
            assert question["default_option_id"] in ids, question["id"]


def test_a_lobby_starts_at_the_defaults_and_a_vote_asks_all_but_host_only():
    ffa = questions_for("civ6", "ffa")
    lobby = {s["id"]: s["value"] for s in preset(ffa, host_only=False)}
    assert list(lobby) == [q["id"] for q in ffa]
    assert lobby["map"] == "vote"
    assert all(s["by"] == "default" for s in preset(ffa, host_only=False))
    assert preset(ffa, host_only=True) == []
    teamer = questions_for("civ6", "teamer")
    assert preset(teamer, host_only=True) == [
        {"id": "wonders", "value": "abundant", "by": "default"}
    ]


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
def test_draft_mode_is_asked_once_and_last(edition, game_type):
    asked = [question["id"] for question in questions_for(edition, game_type)]
    assert asked[-1] == "draft_mode"
    assert asked.count("draft_mode") == 1


@pytest.mark.parametrize("edition", EDITIONS)
def test_teamers_offer_standard_and_random_only(edition):
    modes = questions_for(edition, "teamer")[-1]
    assert [option["id"] for option in modes["options"]] == ["standard", "random"]


@pytest.mark.parametrize("edition", EDITIONS)
@pytest.mark.parametrize("game_type", ["ffa", "duel"])
def test_ffas_and_duels_may_also_draft_blind(edition, game_type):
    modes = questions_for(edition, game_type)[-1]
    assert [o["id"] for o in modes["options"]] == ["standard", "random", "blind"]


def test_the_catalogue_hands_out_a_copy():
    questions_for("civ6", "ffa")[0]["title"] = "MUTATED"
    assert questions_for("civ6", "ffa")[0]["title"] != "MUTATED"


@pytest.mark.parametrize(
    ("edition", "game_type"), [("civ5", "ffa"), ("civ6", "battle-royale")]
)
def test_an_unknown_ballot_is_refused(edition, game_type):
    with pytest.raises(UnknownBallot):
        questions_for(edition, game_type)


def test_settings_are_the_ballot_without_its_draft_mode():
    ffa = [question["id"] for question in settings_for("civ6", "ffa")]
    assert ffa == [q["id"] for q in questions_for("civ6", "ffa")][:-1]
    assert {"map", "disasters", "huts"} <= set(ffa)
    huts = next(q for q in settings_for("civ6", "ffa") if q["id"] == "huts")
    assert [o["label"] for o in huts["options"]] == ["1x", "1.25x", "1.5x", "1.75x"]
    assert "draft_mode" not in [q["id"] for q in settings_for("civ6", "teamer")]


def test_civ6_asks_trade_only_in_an_ffa_and_friends_not_in_a_duel():
    ffa = [q["id"] for q in settings_for("civ6", "ffa")]
    duel = [q["id"] for q in settings_for("civ6", "duel")]
    assert ffa[0] == "friends_allies" and ffa[-1] == "trade_forbidden"
    assert duel == [i for i in ffa if i not in ("friends_allies", "trade_forbidden")]


def test_a_civ6_teamer_votes_on_the_ffa_maps():
    teamer = {q["id"]: q for q in settings_for("civ6", "teamer")}
    ffa = {q["id"]: q for q in settings_for("civ6", "ffa")}
    assert teamer["map"] == ffa["map"]
    assert len(teamer["map"]["options"]) == 16
    assert "riverlands" in [o["id"] for o in teamer["map"]["options"]]


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_only_trade_forbidden_waits_for_a_standard_draft(edition, game_type):
    served = [
        SettingQuestion.model_validate(q).model_dump()
        for q in questions_for(edition, game_type)
    ]
    flagged = [q["id"] for q in served if q["standard_draft_only"]]
    wanted = ["trade_forbidden"] if (edition, game_type) == ("civ6", "ffa") else []
    assert flagged == wanted
    if wanted:
        trade = served[[q["id"] for q in served].index("trade_forbidden")]
        assert [o["id"] for o in trade["options"]] == ["yes", "no"]
        assert trade["default_option_id"] == "no"


def _one_emoji(emoji: str) -> bool:
    plain = emoji.replace("️", "")
    return len(plain) == 1 or (len(plain) == 2 and plain[1] == "⃣")


@pytest.mark.parametrize(("edition", "game_type"), EVERY_BALLOT)
def test_every_option_is_one_reaction_of_its_own(edition, game_type):
    # A vote counts each option by its emoji as a reaction.
    for question in questions_for(edition, game_type):
        emojis = [option["emoji"] for option in question["options"]]
        assert all(_one_emoji(e) for e in emojis), (question["id"], emojis)
        assert len(set(emojis)) == len(emojis), question["id"]
