import zlib
from xml.etree import ElementTree
import struct
import argparse
from app.core.config import settings
import json


def determine_game_mode(players):
    teams = [p["team"] for p in players]
    unique_teams = set(teams)
    if len(players) == 2:
        return "duel"
    if len(unique_teams) == len(players):
        return "ffa"
    return "teamer"


def extract_player_info(root):
    players = []
    teams_dict = {}
    for p in root["parsed"]["CIVS"]:
        civ = p["LEADER_NAME"]["data"]
        if civ == "LEADER_SPECTATOR":
            continue
        team = int(p["TEAM_ID"]["data"]) if "TEAM_ID" in p else 0
        # normalize team ids to 0..n-1 where n is the number of teams
        if team not in teams_dict:
            teams_dict[team] = len(teams_dict)
        team = teams_dict[team]
        steam_id = p["USER_ID"]["data"].split("@")[-1] if "USER_ID" in p else None
        user_name = p["USER_ID"]["data"].split("@")[0] if "USER_ID" in p else None
        player_alive = bool(p["PLAYER_ALIVE"]["data"])
        players.append(
            {
                "steam_id": steam_id,
                "user_name": user_name,
                "civ": civ,
                "team": team,
                "player_alive": player_alive,
                "placement": team,
            }
        )
    players = sorted(players, key=lambda p: p["team"])
    return players


def extract_turn(root):
    return int(root["parsed"]["GAME_TURN"]["data"])


def extract_map_type(root):
    return root["parsed"]["MAP_FILE"]["data"][:-4]


def parse_civ6_save(file_bytes: bytes, version: str = "1.1"):
    root = parse(file_bytes)

    players = extract_player_info(root)
    turn = extract_turn(root)
    map_type = extract_map_type(root)
    game_mode = determine_game_mode(players)

    return {
        "game": "civ6",
        "turn": turn,
        "players": players,
        "game_mode": game_mode,
        "map_type": map_type,
        "parser_version": version,
    }


# Constants
START_ACTOR = bytes([0x58, 0xBA, 0x7F, 0x4C])
ZLIB_HEADER = bytes([0x78, 0x9C])
END_UNCOMPRESSED = bytes([0, 0, 1, 0])
COMPRESSED_DATA_END = bytes([0, 0, 0xFF, 0xFF])

GAME_DATA = {
    "GAME_TURN": bytes([0x9D, 0x2C, 0xE6, 0xBD]),
    "GAME_SPEED": bytes([0x99, 0xB0, 0xD9, 0x05]),
    "MOD_BLOCK_1": bytes([0x5C, 0xAE, 0x27, 0x84]),
    "MOD_BLOCK_2": bytes([0xC8, 0xD1, 0x8C, 0x1B]),
    "MOD_BLOCK_3": bytes([0x44, 0x7F, 0xD4, 0xFE]),
    "MOD_BLOCK_4": bytes([0xBB, 0x5E, 0x30, 0x88]),
    "MOD_ID": bytes([0x54, 0x5F, 0xC4, 0x04]),
    "MOD_TITLE": bytes([0x72, 0xE1, 0x34, 0x30]),
    "MAP_FILE": bytes([0x5A, 0x87, 0xD8, 0x63]),
    "MAP_SIZE": bytes([0x40, 0x5C, 0x83, 0x0B]),
}

SLOT_HEADERS = [
    bytes([0xC8, 0x9B, 0x5F, 0x65]),
    bytes([0x5E, 0xAB, 0x58, 0x12]),
    bytes([0xE4, 0xFA, 0x51, 0x8B]),
    bytes([0x72, 0xCA, 0x56, 0xFC]),
    bytes([0xD1, 0x5F, 0x32, 0x62]),
    bytes([0x47, 0x6F, 0x35, 0x15]),
    bytes([0xFD, 0x3E, 0x3C, 0x8C]),
    bytes([0x6B, 0x0E, 0x3B, 0xFB]),
    bytes([0xFA, 0x13, 0x84, 0x6B]),
    bytes([0x6C, 0x23, 0x83, 0x1C]),
    bytes([0xF4, 0x14, 0x18, 0xAA]),
    bytes([0x62, 0x24, 0x1F, 0xDD]),
]

ACTOR_DATA = {
    "USER_ID": bytes([0x9A, 0x24, 0x72, 0x8E]),
    "ACTOR_NAME": bytes([0x2F, 0x5C, 0x5E, 0x9D]),
    "LEADER_NAME": bytes([0x5F, 0x5E, 0xCD, 0xE8]),
    "ACTOR_TYPE": bytes([0xBE, 0xAB, 0x55, 0xCA]),
    "PLAYER_NAME": bytes([0xFD, 0x6B, 0xB9, 0xDA]),
    "PLAYER_PASSWORD": bytes([0x6C, 0xD1, 0x7C, 0x6E]),
    "PLAYER_ALIVE": bytes([0xA6, 0xDF, 0xA7, 0x62]),
    "IS_CURRENT_TURN": bytes([0xCB, 0x21, 0xB0, 0x7A]),
    "ACTOR_AI_HUMAN": bytes([0x95, 0xB9, 0x42, 0xCE]),  # 3 = Human, 1 = AI
    "ACTOR_DESCRIPTION": bytes([0x65, 0x19, 0x9B, 0xFF]),
    "TEAM_ID": bytes([0x54, 0xB4, 0x8A, 0x0D]),
}

MARKERS = {
    "START_ACTOR": START_ACTOR,
    "END_UNCOMPRESSED": END_UNCOMPRESSED,
    "COMPRESSED_DATA_END": COMPRESSED_DATA_END,
    "GAME_DATA": GAME_DATA,
    "ACTOR_DATA": ACTOR_DATA,
}

DATA_TYPES = {
    "BOOLEAN": 1,
    "INTEGER": 2,
    "STRING": 5,
    "UTF_STRING": 6,
    "ARRAY_START": 0x0A,
}

loggingEnabled = False


def log(message):
    if loggingEnabled:
        print(message)


def read_state(buffer, state=None):
    if state is None:
        state = {
            "pos": 0,
            "next4": buffer[0:4],
        }
    else:
        if state["pos"] >= len(buffer) - 4:
            return None
        state["next4"] = buffer[state["pos"] : state["pos"] + 4]
    return state


def parse_entry(buffer, state, dont_skip=False):
    while True:
        type_buffer = buffer[state["pos"] + 4 : state["pos"] + 8]
        marker = state["next4"]
        type_val = struct.unpack("<I", type_buffer)[0]
        result = {
            "marker": marker,
            "type": type_val,
        }
        state["pos"] += 8
        successful_parse = True

        if not dont_skip and (struct.unpack("<I", marker)[0] < 256 or type_val == 0):
            result["data"] = "SKIP"
        elif type_val == 0x18 or type_buffer[:2] == ZLIB_HEADER:
            result["data"] = "UNKNOWN COMPRESSED DATA"
            idx = buffer.find(COMPRESSED_DATA_END, state["pos"])
            state["pos"] = idx + 4 if idx != -1 else len(buffer)
            state["readCompressedData"] = True
        else:
            if type_val == DATA_TYPES["BOOLEAN"]:
                result["data"] = read_boolean(buffer, state)
            elif type_val == DATA_TYPES["INTEGER"]:
                result["data"] = read_int(buffer, state)
            elif type_val == DATA_TYPES["ARRAY_START"]:
                result["data"] = read_array_0a(buffer, state)
            elif type_val == 3:
                result["data"] = "UNKNOWN!"
                state["pos"] += 12
            elif type_val == 0x15:
                result["data"] = "UNKNOWN!"
                if buffer[state["pos"] : state["pos"] + 4] == bytes([0, 0, 0, 0x80]):
                    state["pos"] += 20
                else:
                    state["pos"] += 12
            elif type_val == 4 or type_val == DATA_TYPES["STRING"]:
                result["data"] = read_string(buffer, state)
            elif type_val == DATA_TYPES["UTF_STRING"]:
                result["data"] = read_utf_string(buffer, state)
            elif type_val == 0x14 or type_val == 0x0D:
                result["data"] = "UNKNOWN!"
                state["pos"] += 16
            elif type_val == 0x0B:
                result["data"] = read_array_0b(buffer, state)["data"]
            else:
                successful_parse = False
                state["pos"] -= 7
        if successful_parse:
            break
    return result


def read_string(buffer, state):
    orig_state = dict(state)
    result = None
    str_len_buf = buffer[state["pos"] : state["pos"] + 3] + bytes([0])
    str_len = struct.unpack("<I", str_len_buf)[0]
    state["pos"] += 2
    str_info = buffer[state["pos"] : state["pos"] + 6]
    if len(str_info) < 2:
        return "Error reading string: " + str(orig_state)
    if str_info[1] == 0 or str_info[1] == 0x20:
        state["pos"] += 10
        result = ""
    elif str_info[1] == 0x21:
        state["pos"] += 6
        null_term = buffer.find(b"\x00", state["pos"]) - state["pos"]
        result = buffer[state["pos"] : state["pos"] + null_term].decode(
            "utf-8", errors="replace"
        )
        state["pos"] += str_len
    if result is None:
        return "Error reading string: " + str(orig_state)
    return result


def read_utf_string(buffer, state):
    orig_state = dict(state)
    result = None
    str_len = struct.unpack("<H", buffer[state["pos"] : state["pos"] + 2])[0] * 2
    state["pos"] += 2
    if buffer[state["pos"] : state["pos"] + 6] == bytes([0, 0x21, 2, 0, 0, 0]):
        state["pos"] += 6
        result = buffer[state["pos"] : state["pos"] + str_len - 2].decode(
            "utf-16le", errors="replace"
        )
        state["pos"] += str_len
    if result is None:
        return "Error reading string: " + str(orig_state)
    return result


def read_boolean(buffer, state):
    state["pos"] += 8
    result = bool(buffer[state["pos"]])
    state["pos"] += 4
    return result


def read_int(buffer, state):
    state["pos"] += 8
    result = struct.unpack("<I", buffer[state["pos"] : state["pos"] + 4])[0]
    state["pos"] += 4
    return result


def read_array_0a(buffer, state):
    result = []
    state["pos"] += 8
    array_len = struct.unpack("<I", buffer[state["pos"] : state["pos"] + 4])[0]
    log("array length " + str(array_len))
    state["pos"] += 4
    for i in range(array_len):
        index = struct.unpack("<I", buffer[state["pos"] : state["pos"] + 4])[0]
        if index > array_len:
            log("Index outside bounds of array at " + hex(state["pos"]))
            return array_len
        log(f"reading array index {index} at {hex(state['pos'])}")
        state = read_state(buffer, state)
        info = parse_entry(buffer, state, True)
        result.append(info["data"])
    return result


def read_array_0b(buffer, state):
    orig_state = dict(state)
    result = {
        "data": [],
        "chunks": [],
    }
    result["chunks"].append(buffer[state["pos"] : state["pos"] + 8])
    state["pos"] += 8
    array_len = struct.unpack("<I", buffer[state["pos"] : state["pos"] + 4])[0]
    result["chunks"].append(buffer[state["pos"] : state["pos"] + 4])
    state["pos"] += 4
    for i in range(array_len):
        if buffer[state["pos"]] != 0x0A:
            return "Error reading array: " + str(orig_state)
        start_pos = state["pos"]
        state["pos"] += 16
        cur_data = {}
        result["data"].append(cur_data)
        while True:
            state = read_state(buffer, state)
            info = parse_entry(buffer, state)
            for key in GAME_DATA:
                if info["marker"] == GAME_DATA[key]:
                    cur_data[key] = info
            if info["data"] == "1":
                break
        result["chunks"].append(buffer[start_pos : state["pos"]])
    return result


def read_compressed_data(buffer, state):
    idx = buffer.find(COMPRESSED_DATA_END, state["pos"])
    data = buffer[state["pos"] + 4 : idx + 4]
    chunk_size = 64 * 1024
    chunks = []
    pos = 0
    while pos < len(data):
        chunks.append(data[pos : pos + chunk_size])
        pos += chunk_size + 4
    compressed_data = b"".join(chunks)
    return zlib.decompress(compressed_data)


def parse(buffer):
    parsed = {
        "ACTORS": [],
        "CIVS": [],
    }

    chunks = []
    chunk_start = 0
    cur_actor = None
    compressed = None

    state = read_state(buffer)

    if buffer[:4] != b"CIV6":
        raise Exception("Not a Civilization 6 save file. :(")

    # Find GAME_SPEED marker
    while state is not None:
        if state["next4"] == GAME_DATA["GAME_SPEED"]:
            break
        state["pos"] += 1
        state = read_state(buffer, state)

    chunks.append(buffer[chunk_start : state["pos"]])
    chunk_start = state["pos"]

    while state is not None:
        if state["next4"] == END_UNCOMPRESSED:
            break

        info = parse_entry(buffer, state)
        log(f"{chunk_start}/{hex(chunk_start)}: {info} {info['marker'].hex()}")

        def try_add_actor(key, marker):
            nonlocal cur_actor
            if info["marker"] == marker:
                cur_actor = {}
                cur_actor[key] = info
                parsed["ACTORS"].append(cur_actor)

        for marker in SLOT_HEADERS:
            try_add_actor("SLOT_HEADER", marker)

        if not cur_actor and info["marker"] == START_ACTOR:
            try_add_actor("START_ACTOR", START_ACTOR)
        elif info["marker"] == ACTOR_DATA["ACTOR_DESCRIPTION"]:
            cur_actor = None
        else:
            for key in GAME_DATA:
                if info["marker"] == GAME_DATA[key]:
                    if key in parsed:
                        suffix = 2
                        unique_key = f"{key}_{suffix}"
                        while unique_key in parsed:
                            suffix += 1
                            unique_key = f"{key}_{suffix}"
                        parsed[unique_key] = info
                    else:
                        parsed[key] = info
            if cur_actor:
                for key in ACTOR_DATA:
                    if info["marker"] == ACTOR_DATA[key]:
                        cur_actor[key] = info

        info["chunk"] = buffer[chunk_start : state["pos"]]
        chunks.append(info["chunk"])
        chunk_start = state["pos"]

        state = read_state(buffer, state)

    if state:
        chunks.append(buffer[state["pos"] :])

    # Find CIVS
    for cur_marker in SLOT_HEADERS:
        cur_civ = next(
            (
                actor
                for actor in parsed["ACTORS"]
                if actor.get("SLOT_HEADER")
                and actor["SLOT_HEADER"]["marker"] == cur_marker
                and actor.get("ACTOR_AI_HUMAN")
                and actor["ACTOR_AI_HUMAN"]["data"] != 2
                and actor.get("ACTOR_TYPE")
                and actor["ACTOR_TYPE"]["data"] == "CIVILIZATION_LEVEL_FULL_CIV"
                and actor.get("ACTOR_NAME")
            ),
            None,
        )
        if cur_civ:
            parsed["CIVS"].append(cur_civ)
            parsed["ACTORS"].remove(cur_civ)

    # Remove incomplete actors
    for actor in parsed["ACTORS"][:]:
        if not actor.get("ACTOR_TYPE") or not actor.get("ACTOR_NAME"):
            parsed["ACTORS"].remove(actor)

    return {
        "parsed": parsed,
        "chunks": chunks,
        "compressed": compressed,
    }


# CLI usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Civ6 save file.")
    parser.add_argument("filename", type=str, help="Save file to parse")
    parser.add_argument("output", nargs="?", default=None, help="Output file")
    parser.add_argument(
        "--outputCompressed", action="store_true", help="Output compressed data"
    )
    parser.add_argument("--simple", action="store_true", help="Simplify output")
    args = parser.parse_args()

    if not args.filename:
        print("Please pass the filename as the argument to the script.")
    else:
        with open(args.filename, "rb") as f:
            buffer = f.read()
        result = parse_civ6_save(buffer)
        print(json.dumps(result, indent=4))
