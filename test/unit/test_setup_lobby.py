from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from app.features.match_setups.lobby import (
    BLIND_POOL,
    CODE_LETTERS,
    SHAPES,
    LobbyRefused,
    NotTheHost,
    cancel_reason,
    dealt,
    edited,
    finished,
    joined,
    left,
    lobby_code,
    new_lobby,
    picked,
    revealed,
    started,
    tallied,
    vote_placed,
    went_live,
)
from app.features.match_setups.questions import questions_for
from app.features.match_setups.schemas import LobbyShape

LINK = "steam://joinlobby/289070/109775241000000000/76561198000000000"
LEADERS = [f"LEADER_{n:02}" for n in range(40)]
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


def hosted(shape="ffa-8", **fields):
    return new_lobby(
        **{
            "shape": shape,
            "host_id": "host",
            "guild_id": "g",
            "channel_id": "c",
            "steam_link": LINK,
            "code": "K7Q2DM",
            "now": NOW,
            **fields,
        }
    )


def apply(made, changes):
    return {**made, **changes}


def questions(made):
    return questions_for("civ6", made["mode"])


def every_setting(made, **chosen):
    return {q["id"]: q["default_option_id"] for q in questions(made)} | chosen


def lobby(shape="ffa-8", players=(), draft_mode="standard"):
    made = hosted(shape)
    made = apply(made, edit(made, settings=every_setting(made, draft_mode=draft_mode)))
    made = apply(made, went_live(made, "host", "m", questions(made)))
    for player in players:
        made = apply(made, joined(made, player, NOW))
    return made


def edit(made, actor="host", **fields):
    settings = {s["id"]: s["value"] for s in made["settings"]}
    return edited(
        made,
        actor,
        **{
            "shape": made["shape"],
            "questions": questions(made),
            "settings": settings,
            "bans": [],
            "teams": {},
            "removed": [],
            **fields,
        },
    )


def test_every_shape_the_request_offers_is_known():
    assert set(SHAPES) == set(LobbyShape.__args__)


def test_a_lobby_opens_in_setup_with_no_setting_set():
    made = hosted()
    assert (made["phase"], made["code"], made["draft_mode"]) == (
        "setup",
        "K7Q2DM",
        None,
    )
    assert made["settings"] == []
    assert made["players"] == [{"discord_id": "host", "team": None}]
    assert hosted("teamer-3v3")["players"] == [{"discord_id": "host", "team": 1}]
    for link in ("steam://joinlobby/1295660/1/76561198000000000", f"{LINK}\n"):
        with pytest.raises(LobbyRefused, match="not a Civ6 lobby link"):
            hosted(steam_link=link)


def test_a_code_is_six_easily_read_letters():
    codes = {lobby_code(random.Random(n)) for n in range(50)}
    assert len(codes) == 50
    assert all(len(c) == 6 and set(c) <= set(CODE_LETTERS) for c in codes)
    assert not set("01IOL") & set(CODE_LETTERS)


def test_a_lobby_opens_to_players_only_once_every_setting_is_set():
    setup = hosted(steam_link=None)
    with pytest.raises(LobbyRefused, match="not open to players yet"):
        joined(setup, "a", NOW)
    half = apply(setup, edit(setup, settings={"huts": "x1", "map": "pangea_classic"}))
    assert [s["id"] for s in half["settings"]] == ["map", "huts"]
    assert half["draft_mode"] is None
    with pytest.raises(LobbyRefused, match="set every setting first: Official"):
        went_live(half, "host", "m", questions(half))
    ready = apply(half, edit(half, settings=every_setting(half, draft_mode="blind")))
    assert ready["draft_mode"] == "blind"
    with pytest.raises(NotTheHost):
        went_live(ready, "a", "m", questions(ready))
    assert went_live(ready, "host", "m", questions(ready)) == {
        "phase": "live",
        "message_id": "m",
    }
    with pytest.raises(LobbyRefused, match="already open"):
        went_live(lobby(), "host", "m", questions(ready))


def test_joining_fills_the_emptiest_team_and_the_last_seat_starts_the_vote():
    teamer = lobby("teamer-2v2", players=["a", "b"])
    assert [p["team"] for p in teamer["players"]] == [1, 2, 1]
    assert teamer["phase"] == "live"
    full = apply(teamer, joined(teamer, "c", NOW))
    assert [p["team"] for p in full["players"]] == [1, 2, 1, 2]
    assert (full["phase"], full["deadline"]) == ("voting", NOW + timedelta(minutes=3))
    assert full["vote"] == {"channel_id": None, "messages": {}, "finished": []}
    with pytest.raises(LobbyRefused, match="seats are locked"):
        joined(full, "d", NOW)
    with pytest.raises(LobbyRefused, match="already in this lobby"):
        joined(lobby(players=["a"]), "a", NOW)


def test_a_player_leaves_but_the_host_cancels_instead():
    assert [p["discord_id"] for p in left(lobby(players=["a"]), "a")] == ["host"]
    with pytest.raises(LobbyRefused, match="host cancels"):
        left(lobby(), "host")
    with pytest.raises(LobbyRefused, match="not in this lobby"):
        left(lobby(), "a")


def test_an_edit_saves_the_size_every_setting_the_draft_mode_and_bans():
    settings = {s["id"]: s["value"] for s in lobby()["settings"]}
    changes = edit(
        lobby(players=["a", "b"]),
        shape="ffa-12",
        settings={**settings, "huts": "x1_5", "draft_mode": "blind"},
        bans=["LEADER_01", "LEADER_01", "LEADER_02"],
    )
    assert (changes["shape"], changes["seats"], changes["draft_mode"]) == (
        "ffa-12",
        12,
        "blind",
    )
    assert {"id": "huts", "value": "x1_5", "by": "host"} in changes["settings"]
    assert [b["token"] for b in changes["bans"]] == ["LEADER_01", "LEADER_02"]
    with pytest.raises(LobbyRefused, match="huts has no option"):
        edit(lobby(), settings={**settings, "huts": "x9"})
    with pytest.raises(LobbyRefused, match="every setting needs one value"):
        edit(lobby(), settings={"huts": "x1"})
    with pytest.raises(LobbyRefused, match="draft_mode has no option blind"):
        edit(
            lobby("duel"),
            settings={s["id"]: s["value"] for s in lobby("duel")["settings"]}
            | {"draft_mode": "blind"},
        )
    with pytest.raises(NotTheHost):
        edit(lobby(players=["a"]), actor="a")


def test_an_edit_keeps_the_mode_and_room_for_everyone_seated():
    with pytest.raises(LobbyRefused, match="not its mode"):
        edit(lobby(), shape="duel")
    with pytest.raises(LobbyRefused, match="not its mode"):
        edit(lobby("teamer-4v4"), shape="teamer-2v2v2v2")
    with pytest.raises(LobbyRefused, match="9 players do not fit in 8 seats"):
        edit(lobby("ffa-10", players=list("abcdefgh")), shape="ffa-8")
    assert edit(lobby("teamer-2v2"), shape="teamer-3v3")["seats"] == 6


def test_the_host_moves_and_removes_players_but_not_themself():
    teamer = lobby("teamer-2v2", players=["a", "b"])
    moved = edit(teamer, teams={"b": 2}, removed=["a"])["players"]
    assert moved == [{"discord_id": "host", "team": 1}, {"discord_id": "b", "team": 2}]
    with pytest.raises(LobbyRefused, match="team 2 has more than 2"):
        edit(lobby("teamer-2v2", players=["a", "b"]), teams={"host": 2, "b": 2})
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
def test_the_host_starts_early_only_when_the_lobby_can_draft(shape, players, refusal):
    with pytest.raises(LobbyRefused, match=refusal):
        started(lobby(shape, players=players), "host", NOW)


def test_starting_locks_the_seats_for_a_three_minute_bans_vote():
    made = apply(lobby(players=["a"]), started(lobby(players=["a"]), "host", NOW))
    assert (made["phase"], made["deadline"]) == ("voting", NOW + timedelta(minutes=3))
    for locked in (
        lambda: joined(made, "b", NOW),
        lambda: left(made, "a"),
        lambda: edit(made),
        lambda: started(made, "host", NOW),
    ):
        with pytest.raises(LobbyRefused, match="seats are locked"):
            locked()
    with pytest.raises(NotTheHost):
        started(lobby(players=["a"]), "a", NOW)
    four = lobby("teamer-4v4", players=["a", "b", "c", "d", "e"])
    assert started(four, "host", NOW)["phase"] == "voting"


def voting(made):
    return apply(made, started(made, "host", NOW))


def test_the_vote_is_posted_once_and_each_player_finishes_once():
    made = voting(lobby(players=["a"]))
    placed = apply(made, vote_placed(made, "host", "v", {"bans": "m1"}))
    assert placed["vote"] == {
        "channel_id": "v",
        "messages": {"bans": "m1"},
        "finished": [],
    }
    with pytest.raises(LobbyRefused, match="already posted"):
        vote_placed(placed, "host", "v", {})
    done = apply(placed, finished(placed, "a"))
    assert done["vote"]["finished"] == ["a"]
    with pytest.raises(LobbyRefused, match="already finished"):
        finished(done, "a")
    with pytest.raises(LobbyRefused, match="only the lobby's players"):
        finished(done, "z")
    with pytest.raises(LobbyRefused, match="not voting"):
        finished(lobby(players=["a"]), "a")


def tally(made, ban_votes=None, actor="host"):
    rng = random.Random(3)
    return tallied(
        made, actor, ban_votes=ban_votes or {}, leaders=LEADERS, now=NOW, rng=rng
    )


def test_the_vote_adds_bans_with_a_majority_then_the_draft_is_dealt():
    made = lobby(players=["a", "b"])
    made = voting(apply(made, edit(made, bans=LEADERS[:2])))
    counted = tally(
        made,
        {"LEADER_05": ["host", "a"], "LEADER_06": ["a"], "LEADER_00": ["host", "a"]},
    )
    assert [(b["token"], b["by"]) for b in counted["bans"]] == [
        ("LEADER_00", "host"),
        ("LEADER_01", "host"),
        ("LEADER_05", "vote"),
    ]
    assert (counted["status"], counted["deadline"]) == ("done", None)
    assert [len(p["leaders"]) for p in counted["pools"]] == [12, 12, 12]
    dealt_out = {t for p in counted["pools"] for t in p["leaders"]}
    assert len(dealt_out) == 36 and not dealt_out & {
        "LEADER_00",
        "LEADER_01",
        "LEADER_05",
    }
    with pytest.raises(NotTheHost):
        tally(made, actor="a")
    with pytest.raises(LobbyRefused, match="not voting"):
        tally(lobby(players=["a"]))


def test_three_teams_of_two_fill_in_turn_and_draft_by_team():
    trio = lobby("teamer-2v2v2", players=["a", "b", "c", "d"])
    assert [p["team"] for p in trio["players"]] == [1, 2, 3, 1, 2]
    full = apply(trio, joined(trio, "e", NOW))
    assert (full["phase"], [p["team"] for p in full["players"]][-1]) == ("voting", 3)
    pools = tally(voting(lobby("teamer-2v2v2", players=["a", "b"])))["pools"]
    assert [(p["team"], len(p["leaders"])) for p in pools] == [
        (1, 13),
        (2, 13),
        (3, 13),
    ]


def test_a_teamer_drafts_by_team_and_random_gives_each_player_one():
    teamer = lobby("teamer-2v2", players=["a", "b", "c"])
    pools = tally(teamer)["pools"]
    assert [(p["player_id"], p["team"], len(p["leaders"])) for p in pools] == [
        (None, 1, 20),
        (None, 2, 20),
    ]
    rolled = lobby("teamer-2v2", players=["a", "b", "c"], draft_mode="random")
    picks = tally(rolled)["pools"]
    assert [(p["player_id"], p["team"]) for p in picks] == [
        ("host", 1),
        ("b", 1),
        ("a", 2),
        ("c", 2),
    ]
    assert len({p["leaders"][0] for p in picks}) == 4


def test_a_blind_draft_deals_at_most_a_menu_each_then_each_player_locks_in():
    duo = voting(lobby(players=["a"], draft_mode="blind"))
    assert [len(p["leaders"]) for p in dealt(duo, [["host"], ["a"]], LEADERS)] == [
        20,
        20,
    ]
    many = [f"L{n}" for n in range(80)]
    assert [len(p["leaders"]) for p in dealt(duo, [["host"], ["a"]], many)] == [
        BLIND_POOL,
        BLIND_POOL,
    ]
    blind = apply(duo, tally(duo))
    assert (blind["status"], blind["phase"]) == ("open", "picking")
    assert blind["deadline"] == NOW + timedelta(minutes=3)
    mine = blind["pools"][1]["leaders"]
    first = apply(blind, picked(blind, "a", mine[0]))
    assert first["picks"] == [{"player_id": "a", "leader": mine[0], "by": "player"}]
    with pytest.raises(LobbyRefused, match="already locked in"):
        picked(first, "a", mine[1])
    with pytest.raises(LobbyRefused, match="not in your pool"):
        picked(blind, "a", blind["pools"][0]["leaders"][0])
    with pytest.raises(LobbyRefused, match="only the lobby's players pick"):
        picked(blind, "z", mine[0])
    with pytest.raises(LobbyRefused, match="not picking"):
        picked(duo, "a", mine[0])


def test_the_picks_are_revealed_together_and_a_missing_one_is_auto_picked():
    blind = voting(lobby(players=["a"], draft_mode="blind"))
    blind = apply(blind, tally(blind))
    mine = blind["pools"][1]["leaders"]
    blind = apply(blind, picked(blind, "a", mine[2]))
    shown = revealed(blind, "host", random.Random(1))
    host_pick, a_pick = shown["picks"]
    assert a_pick == {"player_id": "a", "leader": mine[2], "by": "player"}
    assert host_pick["by"] == "auto"
    assert host_pick["leader"] in blind["pools"][0]["leaders"]
    assert (shown["status"], shown["deadline"]) == ("done", None)
    with pytest.raises(NotTheHost):
        revealed(blind, "a")


def test_the_host_or_staff_cancel_and_a_closed_lobby_takes_no_change():
    assert cancel_reason(lobby(), "host", staff=False) == "host"
    assert cancel_reason(lobby(), "mod", staff=True) == "staff"
    with pytest.raises(NotTheHost):
        cancel_reason(lobby(), "a", staff=False)
    closed = {**lobby(), "status": "done"}
    with pytest.raises(LobbyRefused, match="closed"):
        joined(closed, "a", NOW)
