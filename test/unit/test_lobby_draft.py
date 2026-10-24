from __future__ import annotations

import random

import pytest
from pydantic import ValidationError

from app.features.lobbies.draft import DraftRefused, deal_draft
from app.features.lobbies.questions import command_settings
from app.features.lobbies.schemas import DealRequest

LEADERS = [f"LEADER_{n}" for n in range(30)]
ANTIQUITY = [f"CIVILIZATION_A{n}" for n in range(6)]
EXPLORATION = [f"CIVILIZATION_E{n}" for n in range(6)]
CIV7 = {
    "leaders": [{"token": t} for t in LEADERS],
    "civs": [{"token": t, "age_pool": "AGE_ANTIQUITY"} for t in ANTIQUITY]
    + [{"token": t, "age_pool": "AGE_EXPLORATION"} for t in EXPLORATION],
}
CIV6 = {"leaders": [{"token": t} for t in LEADERS], "civs": []}


def civ6(**fields):
    return DealRequest(edition="civ6", game_type="ffa", **fields)


def civ7(**fields):
    return DealRequest(edition="civ7", game_type="ffa", **fields)


def dealt(civ_data, request):
    return deal_draft(civ_data, request, random.Random(7))


def test_every_draft_gets_the_count_asked_and_no_leader_twice():
    out = dealt(CIV6, civ6(drafts=6, leaders=4))
    pools = [pool["leaders"] for pool in out["pools"]]
    assert [len(pool) for pool in pools] == [4] * 6
    flat = [token for pool in pools for token in pool]
    assert len(flat) == len(set(flat))
    assert all(pool["civs"] == [] for pool in out["pools"])


def test_banned_leaders_are_never_dealt():
    banned = LEADERS[:20]
    out = dealt(CIV6, civ6(drafts=2, leaders=5, leader_bans=banned))
    assert not {t for p in out["pools"] for t in p["leaders"]} & set(banned)
    assert out["bans"]["leader"] == banned


def test_too_few_leaders_after_bans_says_how_many_remain():
    with pytest.raises(
        DraftRefused, match="20 remain after bans and 6 pools of 4 need 24"
    ):
        dealt(CIV6, civ6(drafts=6, leaders=4, leader_bans=LEADERS[:10]))


def test_a_starting_age_deals_only_its_civs():
    out = dealt(CIV7, civ7(drafts=2, leaders=3, civs=3, starting_age="AGE_ANTIQUITY"))
    assert {c for p in out["pools"] for c in p["civs"]} <= set(ANTIQUITY)
    assert [len(p["civs"]) for p in out["pools"]] == [3, 3]


def test_civs_repeat_across_drafts_only_when_there_are_too_few():
    enough = dealt(CIV7, civ7(drafts=4, leaders=2, civs=3))
    flat = [c for p in enough["pools"] for c in p["civs"]]
    assert len(flat) == len(set(flat))
    short = dealt(CIV7, civ7(drafts=4, leaders=2, civs=3, starting_age="AGE_ANTIQUITY"))
    assert all(len(set(p["civs"])) == 3 for p in short["pools"])


def test_a_civ_outside_the_age_cannot_be_banned():
    with pytest.raises(DraftRefused, match="CIVILIZATION_E0"):
        dealt(
            CIV7,
            civ7(
                drafts=2,
                leaders=2,
                civs=2,
                starting_age="AGE_ANTIQUITY",
                civ_bans=["CIVILIZATION_E0"],
            ),
        )


def test_the_lobby_s_ban_caps_hold():
    with pytest.raises(DraftRefused, match="At most 20 leader bans"):
        dealt(CIV6, civ6(drafts=2, leaders=2, leader_bans=LEADERS[:21]))
    with pytest.raises(DraftRefused, match="At most 3 civ bans"):
        dealt(
            CIV7,
            civ7(
                drafts=2,
                leaders=2,
                civs=1,
                starting_age="AGE_ANTIQUITY",
                civ_bans=ANTIQUITY[:4],
            ),
        )


def test_an_unknown_leader_cannot_be_banned():
    with pytest.raises(DraftRefused, match="LEADER_NOBODY"):
        dealt(CIV6, civ6(drafts=2, leaders=2, leader_bans=["LEADER_NOBODY"]))


@pytest.mark.parametrize(
    "fields",
    [
        {"edition": "civ6", "game_type": "ffa", "drafts": 2, "leaders": 2, "civs": 2},
        {"edition": "civ7", "game_type": "ffa", "drafts": 2, "leaders": 2},
        {"edition": "civ7", "game_type": "ffa", "drafts": 11, "leaders": 2, "civs": 2},
        {"edition": "civ6", "game_type": "duel", "drafts": 3, "leaders": 2},
        {"edition": "civ6", "game_type": "ffa", "drafts": 15, "leaders": 2},
    ],
)
def test_a_request_that_does_not_fit_its_edition_is_refused(fields):
    with pytest.raises(ValidationError):
        DealRequest(**fields)


def test_the_command_offers_every_civ6_setting_once_with_huts():
    ids = [question["id"] for question in command_settings("civ6")]
    assert len(ids) == len(set(ids)) == 14
    assert {"map", "disasters", "huts", "wonders", "byc_mode"} <= set(ids)
    assert "draft_mode" not in ids
    huts = next(q for q in command_settings("civ6") if q["id"] == "huts")
    assert [o["label"] for o in huts["options"]] == ["1x", "1.25x", "1.5x", "1.75x"]
    assert len(command_settings("civ7")) == 12
