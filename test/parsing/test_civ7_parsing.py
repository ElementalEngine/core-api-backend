import os
import struct

from app.features.matches.parsers import civ7

_SAVES = os.path.join(os.path.dirname(__file__), "..", "data", "civ7TestSaves")


def _test_parse_civ7_save(
    file_path,
    expected_game,
    expected_age,
    expected_turn,
    expected_mode,
    expected_map_type,
    expected_players,
):
    test_save_path = os.path.join(os.path.dirname(__file__), file_path)
    assert os.path.exists(test_save_path), f"Save file not found: {test_save_path}"

    with open(test_save_path, "rb") as f:
        buffer = f.read()
    result = civ7.parse_civ7_save(buffer)

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


def test_a_human_seat_without_a_leader_or_civ_is_kept():
    m = civ7.GAME_DATA_MARKERS
    array = civ7.ChunkType.ChunkArray
    data = {
        "group1": [],
        "group3": [
            {
                "type": array,
                "value": [
                    {"marker": m["LEADER_NAME"], "value": "LEADER_AUGUSTUS"},
                    {"marker": m["CIV_NAME"], "value": "CIVILIZATION_ROME"},
                    {"marker": m["USER_ID"], "value": "a@1"},
                ],
            },
            {"type": array, "value": [{"marker": m["USER_ID"], "value": "b@2"}]},
            {
                "type": array,
                "value": [{"marker": m["CIV_NAME"], "value": "CIVILIZATION_NONE"}],
            },
        ],
    }
    players = civ7.extract_player_info(civ7.parse_chunks(data))
    assert [(p["steam_id"], p["leader"], p["civ"]) for p in players] == [
        ("1", "LEADER_AUGUSTUS", "CIVILIZATION_ROME"),
        ("2", "LEADER_NONE", "CIVILIZATION_NONE"),
    ]


def _duel():
    with open(os.path.join(_SAVES, "duel.Civ7Save"), "rb") as f:
        return f.read()


def test_a_cloud_or_hotseat_separator_before_group_4_reads_the_same():
    # Those saves count chunks in the separator where realtime saves hold 0.
    raw = _duel()
    at = civ7.parse_raw(raw)["group3"][-1]["endOffset"]
    assert raw[at + 4 : at + 8] == bytes(4)
    chunk = bytes.fromhex("326673de") + struct.pack("<5I", 8, 0, 1, 4, 0)
    cloud = raw[: at + 4] + struct.pack("<I", 1) + chunk + raw[at + 8 :]
    assert civ7.parse_civ7_save(cloud) == civ7.parse_civ7_save(raw)


def test_an_unreadable_group_5_does_not_fail_the_parse():
    raw = _duel()
    end = civ7.parse_raw(raw)["group4"][-1]["endOffset"]
    damaged = raw[:end] + b"\xff" * 72 + raw[end + 72 :]
    assert civ7.parse_raw(damaged)["group5"] == []
    assert civ7.parse_civ7_save(damaged) == civ7.parse_civ7_save(raw)


def test_only_a_full_civ_is_a_seat():
    m = civ7.GAME_DATA_MARKERS

    def record(leader, civ, level):
        return {
            "type": civ7.ChunkType.ChunkArray,
            "value": [
                {"marker": m["LEADER_NAME"], "value": leader},
                {"marker": m["CIV_NAME"], "value": civ},
                {"marker": m["CIV_LEVEL"], "value": level},
            ],
        }

    data = {
        "group1": [],
        "group3": [
            record("LEADER_XERXES", "CIVILIZATION_CARTHAGE", civ7.FULL_CIV),
            record(
                "LEADER_INDEPENDENT",
                "CIVILIZATION_INDEPENDENT",
                "CIVILIZATION_LEVEL_INDEPENDENT",
            ),
            record(
                "LEADER_MINOR_CIV_DEFAULT",
                "CIVILIZATION_PLACEHOLDER_CITYSTATE",
                "CIVILIZATION_LEVEL_CITY_STATE",
            ),
        ],
    }
    players = civ7.extract_player_info(civ7.parse_chunks(data))
    assert [p["leader"] for p in players] == ["LEADER_XERXES"]


def test_a_nested_array_of_plain_values_parses():
    # Some saves list age crises as bare strings rather than records.
    def item(text):
        body = text.encode() + b"\0"
        return struct.pack("<IIH6x", 2, 0, len(body)) + body

    items = [item("ANTIQUITY_CRISIS_PLAGUE"), item("ANTIQUITY_CRISIS_REVOLT")]
    head = b"\x01\x02\x03\x04" + struct.pack("<IIQI", 30, 0, 0, len(items))
    blob = head + b"".join(items)
    chunk = civ7.parse_chunk(blob, 0)
    assert [[x["value"] for x in group] for group in chunk["value"]] == [
        ["ANTIQUITY_CRISIS_PLAGUE"],
        ["ANTIQUITY_CRISIS_REVOLT"],
    ]
    assert chunk["endOffset"] == len(blob)
