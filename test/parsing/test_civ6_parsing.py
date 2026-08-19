import os

from app.features.matches.parsers import civ6

# Players are returned grouped by team, not in slot order. These expectations
# match that order deliberately -- it has been live for the whole rating
# history and D66 freezes it (§4 item 14). leader and civ are asserted in
# their Entry 10 Half A shape: LEADER_* in leader, CIVILIZATION_* in civ,
# both read straight from the save (D136).


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
        assert player["leader"] == expected["leader"]
        assert player["civ"] == expected["civ"]
        assert player["team"] == expected["team"]
        assert player["player_alive"] == expected["player_alive"]


def test_parse_civ6_save_teamer():
    expected_players = [
        {
            "steam_id": "Calcifer",
            "user_name": "Calcifer",
            "leader": "LEADER_DIDO",
            "civ": "CIVILIZATION_PHOENICIA",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_POUNDMAKER",
            "civ": "CIVILIZATION_CREE",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_TOMYRIS",
            "civ": "CIVILIZATION_SCYTHIA",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_GENGHIS_KHAN",
            "civ": "CIVILIZATION_MONGOLIA",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_WILHELMINA",
            "civ": "CIVILIZATION_NETHERLANDS",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_VICTORIA",
            "civ": "CIVILIZATION_ENGLAND",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_SULEIMAN",
            "civ": "CIVILIZATION_OTTOMAN",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_SHAKA",
            "civ": "CIVILIZATION_ZULU",
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
            "leader": "LEADER_HAMMURABI",
            "civ": "CIVILIZATION_BABYLON_STK",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": "76561197977357019",
            "user_name": "Under",
            "leader": "LEADER_GITARJA",
            "civ": "CIVILIZATION_INDONESIA",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": "76561199579973763",
            "user_name": "R1sky Business",
            "leader": "LEADER_NZINGA_MBANDE",
            "civ": "CIVILIZATION_KONGO",
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": "76561198045494817",
            "user_name": "Munch [icon_barbarian]",
            "leader": "LEADER_CHANDRAGUPTA",
            "civ": "CIVILIZATION_INDIA",
            "team": 3,
            "player_alive": False,
        },
        {
            "steam_id": "76561198807592368",
            "user_name": "Alex",
            "leader": "LEADER_JADWIGA",
            "civ": "CIVILIZATION_POLAND",
            "team": 4,
            "player_alive": True,
        },
        {
            "steam_id": "76561199353918757",
            "user_name": "General Zalyzhnii",
            "leader": "LEADER_T_ROOSEVELT",
            "civ": "CIVILIZATION_AMERICA",
            "team": 5,
            "player_alive": False,
        },
        {
            "steam_id": "76561199097315291",
            "user_name": "The Largest Goku Black",
            "leader": "LEADER_LADY_SIX_SKY",
            "civ": "CIVILIZATION_MAYA",
            "team": 6,
            "player_alive": True,
        },
        {
            "steam_id": "76561199792001404",
            "user_name": "Toilet the Small",
            "leader": "LEADER_PHILIP_II",
            "civ": "CIVILIZATION_SPAIN",
            "team": 7,
            "player_alive": True,
        },
        {
            "steam_id": "76561198899804192",
            "user_name": "Saratoga",
            "leader": "LEADER_VICTORIA_ALT",
            "civ": "CIVILIZATION_ENGLAND",
            "team": 8,
            "player_alive": False,
        },
        {
            "steam_id": "76561198076034741",
            "user_name": "iLLmatic",
            "leader": "LEADER_BARBAROSSA",
            "civ": "CIVILIZATION_GERMANY",
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
            "leader": "LEADER_ALEXANDER",
            "civ": "CIVILIZATION_MACEDON",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_YONGLE",
            "civ": "CIVILIZATION_CHINA",
            "team": 0,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_NADER_SHAH",
            "civ": "CIVILIZATION_PERSIA",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_TAMAR",
            "civ": "CIVILIZATION_GEORGIA",
            "team": 1,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_KUBLAI_KHAN_MONGOLIA",
            "civ": "CIVILIZATION_MONGOLIA",
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_MONTEZUMA",
            "civ": "CIVILIZATION_AZTEC",
            "team": 2,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_JOAO_III",
            "civ": "CIVILIZATION_PORTUGAL",
            "team": 3,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_VICTORIA_ALT",
            "civ": "CIVILIZATION_ENGLAND",
            "team": 3,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_AMANITORE",
            "civ": "CIVILIZATION_NUBIA",
            "team": 4,
            "player_alive": True,
        },
        {
            "steam_id": None,
            "user_name": None,
            "leader": "LEADER_SHAKA",
            "civ": "CIVILIZATION_ZULU",
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
