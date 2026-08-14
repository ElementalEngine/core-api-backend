import os

from app.features.matches.parsers import civ6

# Players are returned grouped by team, not in slot order. These expectations
# match that order deliberately -- it has been live for the whole rating
# history and D66 freezes it (§4 item 14). No civ assertion: the field holds a
# LEADER_* token until Entry 10 Half A moves it, S5 (D131).


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
        assert player["team"] == expected["team"]
        assert player["player_alive"] == expected["player_alive"]


def test_parse_civ6_save_teamer():
    expected_players = [
        {
            "steam_id": "Calcifer",
            "user_name": "Calcifer",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
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
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": "76561197977357019",
            "user_name": "Under",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": "76561199579973763",
            "user_name": "R1sky Business",
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": "76561198045494817",
            "user_name": "Munch [icon_barbarian]",
            "team": 3,
            "player_alive": False,
        },
        {
            "steam_id": "76561198807592368",
            "user_name": "Alex",
            "team": 4,
            "player_alive": True,
        },
        {
            "steam_id": "76561199353918757",
            "user_name": "General Zalyzhnii",
            "team": 5,
            "player_alive": False,
        },
        {
            "steam_id": "76561199097315291",
            "user_name": "The Largest Goku Black",
            "team": 6,
            "player_alive": True,
        },
        {
            "steam_id": "76561199792001404",
            "user_name": "Toilet the Small",
            "team": 7,
            "player_alive": True,
        },
        {
            "steam_id": "76561198899804192",
            "user_name": "Saratoga",
            "team": 8,
            "player_alive": False,
        },
        {
            "steam_id": "76561198076034741",
            "user_name": "iLLmatic",
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


def test_parse_civ6_save_5team():
    expected_players = [
        {
            "steam_id": "",
            "user_name": "",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 3,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 3,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 4,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "team": 4,
            "player_alive": True,
        },
    ]

    _test_parse_civ6_save(
        file_path="../data/civ6TestSaves/5team.Civ6Save",
        expected_game="civ6",
        expected_turn=1,
        expected_mode="teamer",
        expected_map_type="Pangaea",
        expected_players=expected_players,
    )
