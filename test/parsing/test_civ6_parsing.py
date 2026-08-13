import os

import pytest

from app.features.matches.parsers import civ6

# S1 item 2 quarantine. Nothing is fixed here; the fix is S4's (D83).
# Both failures are player ORDER, not wrong values: the parser groups players
# by team -- Confirmed against teamer.Civ6Save (4v4) and 5team.Civ6Save (5x2)
# -- while these fixtures interleave in slot order. 10playerFFA passes because
# in an FFA the two orders coincide.
# The fixtures are stale, not the parser: this order has been live for the
# whole rating history and D66 freezes it. S4 reorders the fixtures.
# REMOVAL TRIGGER: S4 closes.
_PLAYER_ORDER = pytest.mark.xfail(strict=True, reason="team-grouped vs slot order. S4.")


def _test_parse_civ6_save(
    file_path,
    expected_game,
    expected_turn,
    expected_mode,
    expected_map_type,
    expected_players,
):
    # Path to a test Civ6 save file
    test_save_path = os.path.join(os.path.dirname(__file__), file_path)
    assert os.path.exists(test_save_path), f"Save file not found: {test_save_path}"

    with open(test_save_path, "rb") as f:
        buffer = f.read()
    # Parse the save file
    result = civ6.parse_civ6_save(buffer)

    # Assert the result is a dict and contains expected keys (customize as needed)
    assert isinstance(result, dict)
    assert "players" in result
    assert result["game"] == expected_game
    assert result["turn"] == expected_turn
    assert result["game_mode"] == expected_mode
    assert result["map_type"] == expected_map_type

    assert len(result["players"]) == len(expected_players)
    for i, expected in enumerate(expected_players):
        player = result["players"][i]
        assert player["steam_id"] == expected["steam_id"]
        assert player["user_name"] == expected["user_name"]
        assert player["civ"] == expected["civ"]
        assert player["team"] == expected["team"]
        assert player["player_alive"] == expected["player_alive"]


@_PLAYER_ORDER
def test_parse_civ6_save_teamer():
    expected_players = [
        {
            "steam_id": "Calcifer",
            "user_name": "Calcifer",
            "civ": "LEADER_DIDO",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_WILHELMINA",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_VICTORIA",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_POUNDMAKER",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_SULEIMAN",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_TOMYRIS",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_GENGHIS_KHAN",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_SHAKA",
            "team": 1,
            "player_alive": True,
        },
    ]
    _test_parse_civ6_save(
        file_path="../data/civ6TestSaves/teamer.Civ6Save",
        expected_game="civ6",
        expected_turn=51,
        expected_mode="teamer",
        expected_map_type="Seven_Seas",
        expected_players=expected_players,
    )


def test_parse_civ6_save_10player_ffa():
    expected_players = [
        {
            "steam_id": "76561198135758328",
            "user_name": "Crax_is_Bax",
            "civ": "LEADER_HAMMURABI",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": "76561197977357019",
            "user_name": "Under",
            "civ": "LEADER_GITARJA",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": "76561199579973763",
            "user_name": "R1sky Business",
            "civ": "LEADER_NZINGA_MBANDE",
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": "76561198045494817",
            "user_name": "Munch [icon_barbarian]",
            "civ": "LEADER_CHANDRAGUPTA",
            "team": 3,
            "player_alive": False,
        },
        {
            "steam_id": "76561198807592368",
            "user_name": "Alex",
            "civ": "LEADER_JADWIGA",
            "team": 4,
            "player_alive": True,
        },
        {
            "steam_id": "76561199353918757",
            "user_name": "General Zalyzhnii",
            "civ": "LEADER_T_ROOSEVELT",
            "team": 5,
            "player_alive": False,
        },
        {
            "steam_id": "76561199097315291",
            "user_name": "The Largest Goku Black",
            "civ": "LEADER_LADY_SIX_SKY",
            "team": 6,
            "player_alive": True,
        },
        {
            "steam_id": "76561199792001404",
            "user_name": "Toilet the Small",
            "civ": "LEADER_PHILIP_II",
            "team": 7,
            "player_alive": True,
        },
        {
            "steam_id": "76561198899804192",
            "user_name": "Saratoga",
            "civ": "LEADER_VICTORIA_ALT",
            "team": 8,
            "player_alive": False,
        },
        {
            "steam_id": "76561198076034741",
            "user_name": "iLLmatic",
            "civ": "LEADER_BARBAROSSA",
            "team": 9,
            "player_alive": False,
        },
    ]
    _test_parse_civ6_save(
        file_path="../data/civ6TestSaves/10playerFFA.Civ6Save",
        expected_game="civ6",
        expected_turn=106,
        expected_mode="ffa",
        expected_map_type="Pangaea",
        expected_players=expected_players,
    )


@_PLAYER_ORDER
def test_parse_civ6_save_5team():
    excepted_players = [
        {
            "steam_id": "",
            "user_name": "",
            "civ": "LEADER_ALEXANDER",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_NADER_SHAH",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_KUBLAI_KHAN_MONGOLIA",
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_JOAO_III",
            "team": 3,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_AMANITORE",
            "team": 4,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_SHAKA",
            "team": 4,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_VICTORIA_ALT",
            "team": 3,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_MONTEZUMA",
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_TAMAR",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "LEADER_YONGLE",
            "team": 0,
            "player_alive": True,
        },
    ]
    _test_parse_civ6_save(
        file_path="../data/civ6TestSaves/5team.Civ6Save",
        expected_game="civ6",
        expected_turn=1,
        expected_mode="teamer",
        expected_map_type="Pangaea",
        expected_players=excepted_players,
    )
