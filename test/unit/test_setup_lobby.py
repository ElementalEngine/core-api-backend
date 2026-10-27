from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest
from bson import ObjectId

from app.features.match_setups.lobby import (
    SHAPES,
    LobbyRefused,
    NotTheHost,
    cancel_reason,
    dealt,
    draft_groups,
    edited,
    finished,
    joined,
    left,
    new_lobby,
    tallied,
    vote_opened,
    vote_placed,
    went_live,
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
            "now": datetime.now(UTC),
            **fields,
        }
    )


def lobby(shape="ffa-8", draft_mode="standard", players=()):
    made = {
        **hosted(shape=shape, draft_mode=draft_mode),
        "live": True,
        "_id": ObjectId(),
    }
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


def edit(made, actor="host", **fields):
    return edited(
        made,
        actor,
        **{
            "shape": made["shape"],
            "questions": settings_for("civ6", made["mode"]),
            "settings": {},
            "bans": [],
            "teams": {},
            "removed": [],
            **fields,
        },
    )


def test_a_lobby_waits_in_setup_until_its_host_opens_it():
    setup = hosted(steam_link=None)
    assert (setup["live"], setup["steam_link"]) == (False, None)
    with pytest.raises(LobbyRefused, match="not open to players yet"):
        joined(setup, "a")
    with pytest.raises(LobbyRefused, match="not open to players yet"):
        draft_groups(setup, "host")
    with pytest.raises(NotTheHost):
        went_live(setup, "a", "m")
    assert went_live(setup, "host", "m") == {"live": True, "message_id": "m"}
    with pytest.raises(LobbyRefused, match="already open"):
        went_live({**setup, "live": True}, "host", "m")


def test_an_edit_resizes_fixes_settings_and_bans_in_one_change():
    changes = edit(
        lobby(players=["a", "b"]),
        shape="ffa-12",
        settings={"huts": "x1_5", "map": "pangea_classic"},
        bans=["LEADER_01", "LEADER_01", "LEADER_02"],
    )
    assert (changes["shape"], changes["seats"]) == ("ffa-12", 12)
    assert [s["id"] for s in changes["settings"]] == [
        q["id"] for q in settings_for("civ6", "ffa") if q["id"] in {"huts", "map"}
    ]
    assert [b["token"] for b in changes["bans"]] == ["LEADER_01", "LEADER_02"]
    with pytest.raises(LobbyRefused, match="huts has no option"):
        edit(lobby(), settings={"huts": "x9"})
    with pytest.raises(NotTheHost):
        edit(lobby(players=["a"]), actor="a")


def test_an_edit_keeps_the_mode_and_room_for_everyone_seated():
    with pytest.raises(LobbyRefused, match="not its mode"):
        edit(lobby(), shape="duel")
    with pytest.raises(LobbyRefused, match="not its mode"):
        edit(lobby("teamer-4v4"), shape="teamer-2v2v2v2")
    with pytest.raises(LobbyRefused, match="10 players do not fit in 8 seats"):
        edit(lobby("ffa-10", players=list("abcdefghi")), shape="ffa-8")
    assert edit(lobby("teamer-2v2"), shape="teamer-3v3")["seats"] == 6


def test_the_host_moves_and_removes_players_but_not_themself():
    teamer = lobby("teamer-2v2", players=["a", "b"])
    moved = edit(teamer, teams={"b": 2}, removed=["a"])["players"]
    assert moved == [{"discord_id": "host", "team": 1}, {"discord_id": "b", "team": 2}]
    with pytest.raises(LobbyRefused, match="team 2 has more than 2"):
        edit(lobby("teamer-2v2", players=["a", "b", "c"]), teams={"b": 2})
    with pytest.raises(LobbyRefused, match="host cannot be removed"):
        edit(teamer, removed=["host"])
    with pytest.raises(LobbyRefused, match="has left the lobby"):
        edit(teamer, removed=["z"])
    with pytest.raises(LobbyRefused, match="only a teamer"):
        edit(lobby(players=["a"]), teams={"a": 1})


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
    ffa["bans"] = edit(ffa, bans=LEADERS[:4])["bans"]
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


NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
FFA = settings_for("civ6", "ffa")


def voting(made):
    return {**made, **vote_opened(made, "host", NOW)}


def tally(made, settings_votes=None, ban_votes=None, actor="host"):
    return tallied(
        made,
        actor,
        questions=FFA,
        settings_votes=settings_votes or {},
        ban_votes=ban_votes or {},
        leaders=LEADERS,
        rng=random.Random(3),
    )


def test_starting_locks_the_seats_for_a_ten_minute_vote():
    made = voting(lobby(players=["a"]))
    assert made["vote"] == {
        "ends_at": datetime(2026, 10, 27, 12, 10, tzinfo=UTC),
        "channel_id": None,
        "messages": {},
        "finished": [],
    }
    for locked in (
        lambda: joined(made, "b"),
        lambda: left(made, "a"),
        lambda: edit(made),
        lambda: vote_opened(made, "host", NOW),
    ):
        with pytest.raises(LobbyRefused, match="voting"):
            locked()
    with pytest.raises(NotTheHost):
        vote_opened(lobby(players=["a"]), "a", NOW)
    with pytest.raises(LobbyRefused, match="at least two"):
        vote_opened(lobby(), "host", NOW)


def test_the_vote_is_posted_once_and_each_player_finishes_once():
    made = voting(lobby(players=["a"]))
    placed = {**made, **vote_placed(made, "host", "thread", {"map": "m1"})}
    assert placed["vote"]["messages"] == {"map": "m1"}
    with pytest.raises(LobbyRefused, match="already posted"):
        vote_placed(placed, "host", "thread", {})
    done = {**placed, **finished(placed, "a")}
    assert done["vote"]["finished"] == ["a"]
    with pytest.raises(LobbyRefused, match="already finished"):
        finished(done, "a")
    with pytest.raises(LobbyRefused, match="only the lobby's players"):
        finished(done, "z")
    with pytest.raises(LobbyRefused, match="not voting"):
        finished(lobby(players=["a"]), "a")


def test_the_vote_fills_what_the_host_left_open_and_adds_bans():
    made = lobby(players=["a", "b"])
    made.update(edit(made, settings={"huts": "x1_5"}, bans=LEADERS[:2]))
    made = voting(made)
    counted = tally(
        made,
        {"huts": {"x1": ["host", "a", "b"]}, "map": {"pangea_classic": ["a", "b"]}},
        {"LEADER_05": ["host", "a"], "LEADER_06": ["a"], "LEADER_00": ["host", "a"]},
    )
    chosen = {s["id"]: (s["value"], s["by"]) for s in counted["settings"]}
    assert [s["id"] for s in counted["settings"]] == [q["id"] for q in FFA]
    assert chosen["huts"] == ("x1_5", "host")
    assert chosen["map"] == ("pangea_classic", "vote")
    defaults = {q["id"]: q["default_option_id"] for q in FFA}
    assert chosen["duration"] == (defaults["duration"], "vote")
    assert [(b["token"], b["by"]) for b in counted["bans"]] == [
        ("LEADER_00", "host"),
        ("LEADER_01", "host"),
        ("LEADER_05", "vote"),
    ]
    dealt_out = {t for p in counted["pools"] for t in p["leaders"]}
    assert counted["status"] == "done"
    assert not dealt_out & {"LEADER_00", "LEADER_01", "LEADER_05"}


def test_only_the_host_counts_a_running_vote():
    made = voting(lobby(players=["a"]))
    with pytest.raises(NotTheHost):
        tally(made, actor="a")
    with pytest.raises(LobbyRefused, match="not voting"):
        tally(lobby(players=["a"]))
