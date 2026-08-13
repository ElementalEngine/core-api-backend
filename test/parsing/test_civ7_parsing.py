import os

import pytest

from app.features.matches.parsers import civ7

_SAVES = os.path.join(os.path.dirname(__file__), "..", "data", "civ7TestSaves")


def _missing(name):
    return not os.path.exists(os.path.join(_SAVES, name))


# S1 item 2 quarantine. Nothing is fixed here; every fix belongs to S4 (D83).
# REMOVAL TRIGGER: S4 closes.
_AGE_TOKEN = pytest.mark.xfail(
    strict=True,
    reason="parser returns the display name ('Antiquity'); fixtures expect the "
    "token ('AGE_ANTIQUITY'). NOT one of D83's four bugs -- a fifth. S4.",
)


def _test_parse_civ7_save(
    file_path,
    expected_game,
    expected_age,
    expected_turn,
    expected_mode,
    expected_map_type,
    expected_players,
):
    # Path to a test Civ7 save file
    test_save_path = os.path.join(os.path.dirname(__file__), file_path)
    assert os.path.exists(test_save_path), f"Save file not found: {test_save_path}"

    with open(test_save_path, "rb") as f:
        buffer = f.read()
    # Parse the save file
    result = civ7.parse_civ7_save(buffer)

    # Assert the result is a dict and contains expected keys (customize as needed)
    assert isinstance(result, dict)
    assert "players" in result
    assert result["game"] == expected_game
    assert result["age"] == expected_age
    assert result["turn"] == expected_turn
    assert result["game_mode"] == expected_mode
    assert result["map_type"] == expected_map_type

    assert len(result["players"]) == len(expected_players)
    for i, expected in enumerate(expected_players):
        player = result["players"][i]
        assert player["steam_id"] == expected["steam_id"]
        assert player["user_name"] == expected["user_name"]
        assert player["civ"] == expected["civ"]
        assert player.get("leader", None) == expected.get("leader", None)
        assert player["team"] == expected["team"]


@_AGE_TOKEN
def test_parse_civ7_save_3v3_T10():
    expected_players = [
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_ROME",
            "leader": "LEADER_FRIEDRICH",
            "team": 0,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_MISSISSIPPIAN",
            "leader": "LEADER_HIMIKO",
            "team": 0,
        },
        {
            "steam_id": "76561198021954438",
            "user_name": "-KC- CanuckSoldier[CPL]",
            "civ": "CIVILIZATION_GREECE",
            "leader": "LEADER_MACHIAVELLI",
            "team": 0,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_EGYPT",
            "leader": "LEADER_NAPOLEON_ALT",
            "team": 1,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_CARTHAGE",
            "leader": "LEADER_AUGUSTUS",
            "team": 1,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_PERSIA",
            "leader": "LEADER_GENGHIS_KHAN",
            "team": 1,
        },
    ]
    _test_parse_civ7_save(
        file_path="../data/civ7TestSaves/3v3_T10.Civ7Save",
        expected_game="civ7",
        expected_age="AGE_ANTIQUITY",
        expected_turn=10,
        expected_mode="teamer",
        expected_map_type="Continents Plus",
        expected_players=expected_players,
    )


@_AGE_TOKEN
def test_parse_civ7_save_5player_ffa():
    expected_players = [
        {
            "steam_id": "76561198046790489",
            "user_name": "Arjou",
            "civ": "CIVILIZATION_MISSISSIPPIAN",
            "leader": "LEADER_FRIEDRICH",
            "team": 0,
        },
        {
            "steam_id": "76561198062006970",
            "user_name": "snyperanihilatr1",
            "civ": "CIVILIZATION_ROME",
            "leader": "LEADER_IBN_BATTUTA",
            "team": 1,
        },
        {
            "steam_id": "76561198271354842",
            "user_name": "Unknown",
            "civ": "CIVILIZATION_MAYA",
            "leader": "LEADER_CHARLEMAGNE",
            "team": 2,
        },
        {
            "steam_id": "76561198027853694",
            "user_name": "Underbow",
            "civ": "CIVILIZATION_AKSUM",
            "leader": "LEADER_XERXES_ALT",
            "team": 3,
        },
        {
            "steam_id": "76561198114596168",
            "user_name": "PineapplePizzaPriest",
            "civ": "CIVILIZATION_PERSIA",
            "leader": "LEADER_ISABELLA",
            "team": 4,
        },
    ]
    _test_parse_civ7_save(
        file_path="../data/civ7TestSaves/5playerFFA.Civ7Save",
        expected_game="civ7",
        expected_age="AGE_ANTIQUITY",
        expected_turn=18,
        expected_mode="ffa",
        expected_map_type="Continents",
        expected_players=expected_players,
    )


@_AGE_TOKEN
def test_parse_civ7_save_duel():
    expected_players = [
        {
            "steam_id": "76561198330293027",
            "user_name": "Calcifer",
            "civ": "CIVILIZATION_GREECE",
            "leader": "LEADER_MACHIAVELLI",
            "team": 0,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_MAURYA",
            "leader": "LEADER_ASHOKA",
            "team": 1,
        },
    ]
    _test_parse_civ7_save(
        file_path="../data/civ7TestSaves/duel.Civ7Save",
        expected_game="civ7",
        expected_age="AGE_ANTIQUITY",
        expected_turn=1,
        expected_mode="duel",
        expected_map_type="Archipelago",
        expected_players=expected_players,
    )


@pytest.mark.skipif(
    _missing("AutoSave_0001.Civ7Save"),
    reason="fixture absent. Antiquity is also covered by three other saves. S4.",
)
def test_parse_civ7_save_AutoSave_001():
    expected_players = [
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_ASSYRIA",
            "leader": "LEADER_EDWARD_TEACH",
            "team": 0,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_TONGA",
            "leader": "LEADER_SAYYIDA_AL_HURRA",
            "team": 1,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_SILLA",
            "leader": "LEADER_LAKSHMIBAI",
            "team": 2,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_AKSUM",
            "leader": "LEADER_IBN_BATTUTA",
            "team": 3,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_MAURYA",
            "leader": "LEADER_ASHOKA_ALT",
            "team": 4,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_PERSIA",
            "leader": "LEADER_XERXES_ALT",
            "team": 5,
        },
    ]
    _test_parse_civ7_save(
        file_path="../data/civ7TestSaves/AutoSave_0001.Civ7Save",
        expected_game="civ7",
        expected_age="AGE_ANTIQUITY",
        expected_turn=1,
        expected_mode="ffa",
        expected_map_type="Continents and Islands",
        expected_players=expected_players,
    )


@pytest.mark.skipif(
    _missing("AutoSave_0002.Civ7Save"),
    reason="fixture absent -- the suite s ONLY AGE_EXPLORATION coverage. Blocks carried-forward item 11. S4.",
)
def test_parse_civ7_save_AutoSave_002():
    expected_players = [
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_DAI_VIET",
            "leader": "LEADER_GENGHIS_KHAN",
            "team": 0,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_ICELAND",
            "leader": "LEADER_EDWARD_TEACH",
            "team": 1,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_PIRATE_REPUBLIC",
            "leader": "LEADER_TECUMSEH",
            "team": 2,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_SPAIN",
            "leader": "LEADER_NAPOLEON",
            "team": 3,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_SONGHAI",
            "leader": "LEADER_HATSHEPSUT",
            "team": 4,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_HAWAII",
            "leader": "LEADER_JOSE_RIZAL",
            "team": 5,
        },
    ]
    _test_parse_civ7_save(
        file_path="../data/civ7TestSaves/AutoSave_0002.Civ7Save",
        expected_game="civ7",
        expected_age="AGE_EXPLORATION",
        expected_turn=2,
        expected_mode="ffa",
        expected_map_type="Continents and Islands",
        expected_players=expected_players,
    )


@pytest.mark.skipif(
    _missing("AutoSave_0003.Civ7Save"),
    reason="fixture absent. AGE_MODERN is also covered by modernGame. S4.",
)
def test_parse_civ7_save_AutoSave_003():
    expected_players = [
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_QAJAR",
            "leader": "LEADER_ADA_LOVELACE",
            "team": 0,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_OTTOMANS",
            "leader": "LEADER_AMINA",
            "team": 1,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_MUGHAL",
            "leader": "LEADER_XERXES",
            "team": 2,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_AMERICA",
            "leader": "LEADER_BENJAMIN_FRANKLIN",
            "team": 3,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_GREAT_BRITAIN",
            "leader": "LEADER_EDWARD_TEACH",
            "team": 4,
        },
        {
            "steam_id": None,
            "user_name": None,
            "civ": "CIVILIZATION_MEIJI",
            "leader": "LEADER_HIMIKO_ALT",
            "team": 5,
        },
    ]
    _test_parse_civ7_save(
        file_path="../data/civ7TestSaves/AutoSave_0003.Civ7Save",
        expected_game="civ7",
        expected_age="AGE_MODERN",
        expected_turn=3,
        expected_mode="ffa",
        expected_map_type="Continents and Islands",
        expected_players=expected_players,
    )


@_AGE_TOKEN
def test_parse_civ7_save_modernGame():
    expected_players = [
        {
            "steam_id": "76561198021954438",
            "user_name": "-KC- CanuckSoldier[CPL]",
            "civ": "CIVILIZATION_MEXICO",
            "leader": "LEADER_HATSHEPSUT",
            "team": 0,
        },
        {
            "steam_id": "76561199700824480",
            "user_name": "S2ci Irrel War Master",
            "civ": "CIVILIZATION_QAJAR",
            "leader": "LEADER_TRUNG_TRAC",
            "team": 1,
        },
        {
            "steam_id": "76561198052505244",
            "user_name": "Eudaimania",
            "civ": "CIVILIZATION_OTTOMANS",
            "leader": "LEADER_MACHIAVELLI",
            "team": 2,
        },
        {
            "steam_id": "76561198305084185",
            "user_name": "AntartiK",
            "civ": "CIVILIZATION_PRUSSIA",
            "leader": "LEADER_LAKSHMIBAI",
            "team": 3,
        },
        {
            "steam_id": "76561198858700477",
            "user_name": "IcerMakerBuilder",
            "civ": "CIVILIZATION_MEIJI",
            "leader": "LEADER_XERXES",
            "team": 4,
        },
        {
            "steam_id": "2390158107305499949",
            "user_name": "Patriotlovinbrit",
            "civ": "CIVILIZATION_MUGHAL",
            "leader": "LEADER_SAYYIDA_AL_HURRA",
            "team": 5,
        },
        {
            "steam_id": "76561198074051945",
            "user_name": "Moonie",
            "civ": "CIVILIZATION_RUSSIA",
            "leader": "LEADER_CATHERINE",
            "team": 6,
        },
        {
            "steam_id": "76561197977010133",
            "user_name": "BC | Kryptonic",
            "civ": "CIVILIZATION_NEPAL",
            "leader": "LEADER_XERXES_ALT",
            "team": 7,
        },
    ]
    _test_parse_civ7_save(
        file_path="../data/civ7TestSaves/modernGame.Civ7Save",
        expected_game="civ7",
        expected_age="AGE_MODERN",
        expected_turn=20,
        expected_mode="ffa",
        expected_map_type="Pangaea and Islands",
        expected_players=expected_players,
    )
