from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest

from app.features.match_setups.lobby import (
    SHAPES,
    LobbyRefused,
    NotTheHost,
    cancel_reason,
    dealt,
    draft_groups,
    host_bans,
    host_settings,
    joined,
    left,
    moved,
    new_lobby,
)
from app.features.match_setups.questions import settings_for
from app.features.match_setups.schemas import LobbyShape

LINK = "steam://joinlobby/289070/109775241000000000/76561198000000000"
LEADERS = [f"LEADER_{n:02}" for n in range(40)]


def hosted(**fields):
    return new_lobby(
        **{
            "shape": "ffa-8",
            "draft_mode": "standard",
            "host_id": "host",
            "guild_id": "g",
            "channel_id": "c",
            "steam_link": LINK,
            "rules": None,
            "now": datetime.now(UTC),
            **fields,
        }
    )


def lobby(shape="ffa-8", draft_mode="standard", players=()):
    made = hosted(shape=shape, draft_mode=draft_mode)
    for player in players:
        made["players"] = joined(made, player)
    return made


def test_every_shape_the_request_offers_is_known():
    assert set(SHAPES) == set(LobbyShape.__args__)


def test_a_lobby_opens_with_its_host_seated_and_refuses_another_game_s_link():
    assert lobby()["players"] == [{"discord_id": "host", "team": None}]
    assert lobby("teamer-3v3")["players"] == [{"discord_id": "host", "team": 1}]
    for link in (
        "steam://joinlobby/1295660/1/76561198000000000",
        f"{LINK}\n",
        "https://x.example",
    ):
        with pytest.raises(LobbyRefused, match="not a Civ6 lobby link"):
            hosted(steam_link=link)


def test_joining_fills_the_emptiest_team_and_stops_at_the_last_seat():
    teamer = lobby("teamer-2v2", players=["a", "b", "c"])
    assert [p["team"] for p in teamer["players"]] == [1, 2, 1, 2]
    with pytest.raises(LobbyRefused, match="full"):
        joined(teamer, "d")
    with pytest.raises(LobbyRefused, match="already in this lobby"):
        joined(lobby(players=["a"]), "a")


def test_a_player_leaves_but_the_host_cancels_instead():
    assert [p["discord_id"] for p in left(lobby(players=["a"]), "a")] == ["host"]
    with pytest.raises(LobbyRefused, match="host cancels"):
        left(lobby(), "host")
    with pytest.raises(LobbyRefused, match="not in this lobby"):
        left(lobby(), "a")


def test_only_the_host_moves_players_and_only_onto_a_team_with_room():
    teamer = lobby("teamer-2v2", players=["a", "b"])
    assert {p["discord_id"]: p["team"] for p in moved(teamer, "host", "b", 2)}["b"] == 2
    with pytest.raises(NotTheHost):
        moved(teamer, "a", "b", 2)
    with pytest.raises(LobbyRefused, match="team 2 is full"):
        moved(lobby("teamer-2v2", players=["a", "b", "c"]), "host", "b", 2)
    with pytest.raises(LobbyRefused, match="already on team 1"):
        moved(teamer, "host", "b", 1)
    with pytest.raises(LobbyRefused, match="only a teamer"):
        moved(lobby(players=["a"]), "host", "a", 1)


def test_the_host_fixes_known_settings_in_catalogue_order():
    questions = settings_for("civ6", "ffa")
    fixed = host_settings(
        lobby(), "host", questions, {"huts": "x1_5", "map": "pangea_classic"}
    )
    assert [s["id"] for s in fixed] == [
        q["id"] for q in questions if q["id"] in {"huts", "map"}
    ]
    assert all(s["by"] == "host" for s in fixed)
    with pytest.raises(LobbyRefused, match="huts has no option"):
        host_settings(lobby(), "host", questions, {"huts": "x9"})
    with pytest.raises(NotTheHost):
        host_settings(lobby(), "a", questions, {})


def test_host_bans_land_once_each():
    bans = host_bans(lobby(), "host", ["LEADER_01", "LEADER_01", "LEADER_02"])
    assert bans == [
        {"token": "LEADER_01", "kind": "leader", "by": "host"},
        {"token": "LEADER_02", "kind": "leader", "by": "host"},
    ]


@pytest.mark.parametrize(
    ("shape", "players", "refusal"),
    [
        ("ffa-8", [], "at least two"),
        ("duel", [], "two players"),
        ("teamer-3v3", ["a", "b"], "same size"),
    ],
)
def test_a_lobby_starts_only_when_it_can(shape, players, refusal):
    with pytest.raises(LobbyRefused, match=refusal):
        draft_groups(lobby(shape, players=players), "host")


def test_three_a_side_in_a_four_a_side_lobby_may_start():
    teamer = lobby("teamer-4v4", players=["a", "b", "c", "d", "e"])
    assert draft_groups(teamer, "host") == [["host", "b", "d"], ["a", "c", "e"]]


def test_standard_splits_the_leaders_left_after_bans_evenly():
    ffa = lobby(players=["a", "b"])
    ffa["bans"] = host_bans(ffa, "host", LEADERS[:4])
    pools = dealt(ffa, draft_groups(ffa, "host"), LEADERS, random.Random(3))
    assert [p["player_id"] for p in pools] == ["host", "a", "b"]
    assert [len(p["leaders"]) for p in pools] == [12, 12, 12]
    dealt_out = {t for p in pools for t in p["leaders"]}
    assert len(dealt_out) == 36 and not dealt_out & set(LEADERS[:4])


def test_a_teamer_drafts_by_team_and_random_gives_each_player_one():
    teamer = lobby("teamer-2v2", players=["a", "b", "c"])
    pools = dealt(teamer, draft_groups(teamer, "host"), LEADERS, random.Random(3))
    assert [(p["player_id"], p["team"], len(p["leaders"])) for p in pools] == [
        (None, 1, 20),
        (None, 2, 20),
    ]
    rolled = lobby("teamer-2v2", "random", players=["a", "b", "c"])
    picks = dealt(rolled, draft_groups(rolled, "host"), LEADERS, random.Random(3))
    assert [(p["player_id"], p["team"]) for p in picks] == [
        ("host", 1),
        ("b", 1),
        ("a", 2),
        ("c", 2),
    ]
    assert len({p["leaders"][0] for p in picks}) == 4


def test_the_host_or_staff_cancel_and_a_closed_lobby_takes_no_change():
    assert cancel_reason(lobby(), "host", staff=False) == "host"
    assert cancel_reason(lobby(), "mod", staff=True) == "staff"
    with pytest.raises(NotTheHost):
        cancel_reason(lobby(), "a", staff=False)
    closed = {**lobby(), "status": "done"}
    with pytest.raises(LobbyRefused, match="closed"):
        joined(closed, "a")
