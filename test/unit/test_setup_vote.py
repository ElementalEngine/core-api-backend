from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest
from bson import ObjectId

from app.features.match_setups.lobby import (
    LobbyRefused,
    NotTheHost,
    finished,
    picked,
    revealed,
)
from app.features.match_setups.pools import BLIND_POOL
from app.features.match_setups.questions import questions_for, seeded_index, winner
from app.features.match_setups.vote import (
    VOTE_LIMIT,
    new_vote,
    vote_counted,
    vote_tallied,
)

LEADERS = [f"LEADER_{n:02}" for n in range(40)]
CIVS = [f"CIVILIZATION_{n:02}" for n in range(12)]
NOW = datetime(2026, 10, 28, 12, tzinfo=UTC)


def voting(edition="civ6", mode="ffa", players=("a", "b", "c"), **fields):
    made = new_vote(
        **{
            "edition": edition,
            "mode": mode,
            "age": None,
            "host_id": "a",
            "guild_id": "g",
            "channel_id": "c",
            "players": list(players),
            "bans": [],
            "code": "K7Q2DM",
            "now": NOW,
            "questions": questions_for(edition, mode),
            **fields,
        }
    )
    return {**made, "_id": ObjectId("652f1a2b3c4d5e6f7a8b9c0d")}


def tally(vote, settings=None, bans=None, civs=(), actor="a"):
    return {
        **vote,
        **vote_tallied(
            vote,
            actor,
            setting_votes=settings or {},
            ban_votes=bans or {},
            questions=questions_for(vote["edition"], vote["mode"]),
            leaders=LEADERS,
            civs=list(civs),
            now=NOW,
            rng=random.Random(3),
        ),
    }


def test_a_vote_opens_at_once_for_ten_minutes_with_its_starter_bans_standing():
    made = voting(
        players=["a", "b", "c", "a"],
        bans=[{"token": "LEADER_01", "kind": "leader"}],
    )
    assert (made["command"], made["phase"], made["deadline"]) == (
        "vote",
        "voting",
        NOW + VOTE_LIMIT,
    )
    assert [p["discord_id"] for p in made["players"]] == ["a", "b", "c"]
    assert (made["settings"], made["draft_mode"]) == ([], None)
    assert made["bans"] == [{"token": "LEADER_01", "kind": "leader", "by": "host"}]


@pytest.mark.parametrize(
    ("fields", "refusal"),
    [
        ({"players": ["a"]}, "2 to 14 players"),
        ({"players": ["a", "b"]}, "an FFA is three or more"),
        ({"mode": "duel"}, "a duel is two players"),
        ({"mode": "teamer"}, "two teams of two or more"),
    ],
)
def test_a_vote_is_refused_when_its_players_do_not_fit(fields, refusal):
    with pytest.raises(LobbyRefused, match=refusal):
        voting(**fields)


def test_a_setting_goes_to_the_most_player_reactions_and_any_tie_is_fixed():
    question = {"id": "huts", "options": [{"id": "x1"}, {"id": "x2"}, {"id": "x3"}]}
    players = {"a", "b", "c"}
    assert (
        winner(question, {"x2": ["a", "b"], "x3": ["c", "watcher", "w2"]}, players, "s")
        == "x2"
    )
    tie = winner(question, {"x3": ["a"], "x2": ["b"]}, players, "s")
    assert tie == ["x2", "x3"][seeded_index("s", "huts", 2)]
    assert winner(question, {"x2": ["b"], "x3": ["a"]}, players, "s") == tie
    unvoted = winner(question, {"x3": ["watcher"]}, players, "s")
    assert unvoted == ["x1", "x2", "x3"][seeded_index("s", "huts", 3)]


def test_a_setting_a_majority_backs_is_won_at_once_and_stays_won():
    made = voting(players=["a", "b", "c", "d"])
    count = lambda v, settings: {  # noqa: E731
        **v,
        **vote_counted(
            v,
            "a",
            setting_votes=settings,
            ban_votes={},
            questions=questions_for("civ6", "ffa"),
            leaders=LEADERS,
            civs=[],
        ),
    }
    two = count(made, {"huts": {"x1_5": ["a", "b"]}})
    assert two["settings"] == []
    three = count(two, {"huts": {"x1_5": ["a", "b", "c"], "x1": ["d"]}})
    assert three["settings"] == [{"id": "huts", "value": "x1_5", "by": "vote"}]
    taken_back = count(three, {"huts": {"x1": ["a", "b", "c", "d"]}})
    assert taken_back["settings"] == three["settings"]
    done = tally(taken_back, settings={"huts": {"x1": ["a", "b", "c", "d"]}})
    assert {"id": "huts", "value": "x1_5", "by": "vote"} in done["settings"]
    assert [s["id"] for s in done["settings"]] == [
        q["id"] for q in questions_for("civ6", "ffa")
    ]


def test_the_count_settles_every_setting_lands_bans_and_deals_a_standard_draft():
    made = voting()
    done = tally(
        made,
        settings={
            "huts": {"x1_5": ["a", "b"], "x1": ["c"]},
            "draft_mode": {"standard": ["a"]},
        },
        bans={"LEADER_05": ["a", "b"], "LEADER_06": ["a"]},
    )
    by_id = {s["id"]: s for s in done["settings"]}
    assert by_id["huts"] == {"id": "huts", "value": "x1_5", "by": "vote"}
    assert len(by_id) == len(questions_for("civ6", "ffa"))
    assert [b["token"] for b in done["bans"]] == ["LEADER_05"]
    assert [(p["player_id"], len(p["leaders"]), p["civs"]) for p in done["pools"]] == [
        ("a", 13, []),
        ("b", 13, []),
        ("c", 13, []),
    ]
    assert (done["status"], done["deadline"]) == ("done", None)
    with pytest.raises(NotTheHost):
        tally(made, actor="b")


def test_a_teamer_deals_one_pool_a_team_and_random_one_leader_a_player():
    made = voting(mode="teamer", players=["a", "b", "c", "d"])
    standard = tally(made, settings={"draft_mode": {"standard": ["a"]}})
    assert [
        (p["player_id"], p["team"], len(p["leaders"])) for p in standard["pools"]
    ] == [(None, 1, 20), (None, 2, 20)]
    random_draft = tally(made, settings={"draft_mode": {"random": ["a", "b", "c"]}})
    assert [(p["player_id"], len(p["leaders"])) for p in random_draft["pools"]] == [
        ("a", 1),
        ("b", 1),
        ("c", 1),
        ("d", 1),
    ]
    civ7 = voting(edition="civ7", mode="teamer", players=["a", "b", "c", "d"])
    standard_civ7 = tally(civ7, settings={"draft_mode": {"standard": ["a"]}}, civs=CIVS)
    assert [len(p["civs"]) for p in standard_civ7["pools"]] == [7, 7]
    random_civ7 = tally(civ7, settings={"draft_mode": {"random": ["a"]}}, civs=CIVS)
    assert [len(p["civs"]) for p in random_civ7["pools"]] == [1, 1, 1, 1]


def test_civ7_deals_civs_beside_leaders_and_bans_civs_within_their_cap():
    made = voting(edition="civ7", mode="duel", players=["a", "b"])
    bans = {"LEADER_00": ["a", "b"], "CIVILIZATION_00": ["a", "b"]}
    standard = {"draft_mode": {"standard": ["a"]}}
    done = tally(made, settings=standard, bans=bans, civs=CIVS)
    assert [(b["token"], b["kind"]) for b in done["bans"]] == [
        ("LEADER_00", "leader"),
        ("CIVILIZATION_00", "civ"),
    ]
    assert [(len(p["leaders"]), len(p["civs"])) for p in done["pools"]] == [
        (19, 4),
        (19, 4),
    ]
    assert not {"CIVILIZATION_00"} & {c for p in done["pools"] for c in p["civs"]}
    counted = vote_counted(
        made,
        "a",
        setting_votes={},
        ban_votes={"CIVILIZATION_03": ["a", "b"]},
        questions=questions_for("civ7", "ffa"),
        leaders=LEADERS,
        civs=CIVS,
    )
    assert counted["bans"] == [
        {"token": "CIVILIZATION_03", "kind": "civ", "by": "vote"}
    ]


def test_a_civ7_blind_draft_picks_a_leader_and_a_civ_and_the_rest_are_auto_picked():
    made = voting(edition="civ7", mode="duel", players=["a", "b"])
    picking = tally(made, settings={"draft_mode": {"blind": ["a", "b"]}}, civs=CIVS)
    assert (picking["phase"], picking["status"]) == ("picking", "open")
    assert all(len(p["leaders"]) <= BLIND_POOL for p in picking["pools"])
    mine = picking["pools"][0]
    with pytest.raises(LobbyRefused, match="pick a civ"):
        picked(picking, "a", mine["leaders"][0])
    with pytest.raises(LobbyRefused, match="not a player in this vote"):
        picked(picking, "z", mine["leaders"][0], mine["civs"][0])
    one = {**picking, **picked(picking, "a", mine["leaders"][0], mine["civs"][0])}
    shown = revealed(one, "a", random.Random(1))
    assert [(p["player_id"], p["by"]) for p in shown["picks"]] == [
        ("a", "player"),
        ("b", "auto"),
    ]
    assert shown["picks"][1]["civ"] in picking["pools"][1]["civs"]


def test_a_vote_finishes_once_per_player_and_speaks_of_itself():
    made = voting()
    done = {**made, **finished(made, "b")}
    assert done["vote"]["finished"] == ["b"]
    with pytest.raises(LobbyRefused, match="not a player in this vote"):
        finished(made, "z")
    with pytest.raises(LobbyRefused, match="the vote is closed"):
        finished({**made, "status": "done"}, "b")
