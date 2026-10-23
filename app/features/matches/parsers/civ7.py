import argparse
import json
import struct
import sys
from typing import Any

loggingEnabled = False


def log(message):
    if loggingEnabled:
        print(message)


GAME_DATA_MARKERS = {
    "GAME_TURN": bytes([0x9D, 0x2C, 0xE6, 0xBD]),
    "GAME_AGE": bytes([0x84, 0x84, 0xC6, 0xD0]),
    "LEADER_NAME": bytes([0x0F, 0xFB, 0x8C, 0xC1]),
    "CIV_NAME": bytes([0x76, 0x97, 0x40, 0xDE]),
    "USER_ID": bytes([0x20, 0x61, 0xF1, 0x26]),
    "MAP_TYPE": bytes([0x27, 0x60, 0x4C, 0x58]),
    "TEAM_ID": bytes([0xDE, 0xF6, 0x2D, 0x9B]),
    "CIV_LEVEL": bytes([0x7A, 0x9C, 0xA0, 0x19]),
}

# City-states and independents carry a leader and civ too; only a full civ is a seat.
FULL_CIV = "CIVILIZATION_LEVEL_FULL_CIV"


class ChunkType:
    Unknown_1 = 1
    Utf8String = 2
    Utf16String = 3
    Number32 = 8
    Unknown_9 = 9
    Unknown_10 = 10
    Unknown_11 = 11
    Unknown_12 = 12
    Unknown_17 = 17
    ChunkArray = 29
    NestedArray = 30
    Unknown_32 = 32
    Unknown_long = 103842983


def parse(data: bytes) -> dict[str, Any]:
    chunks = parse_raw(data)
    return parse_chunks(chunks)


def find_marker(group, marker):
    for x in group:
        if x["marker"] == marker:
            return x
    return None


def parse_chunks(data: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:

    players = []
    for x in data["group3"]:
        if x["type"] == ChunkType.ChunkArray:
            leader = next(
                (
                    y
                    for y in x["value"]
                    if y["marker"] == GAME_DATA_MARKERS["LEADER_NAME"] and y["value"]
                ),
                None,
            )
            civ = next(
                (
                    y
                    for y in x["value"]
                    if y["marker"] == GAME_DATA_MARKERS["CIV_NAME"] and y["value"]
                ),
                None,
            )
            user_id = next(
                (
                    y
                    for y in x["value"]
                    if y["marker"] == GAME_DATA_MARKERS["USER_ID"] and y["value"]
                ),
                None,
            )
            team_id = next(
                (
                    y
                    for y in x["value"]
                    if y["marker"] == GAME_DATA_MARKERS["TEAM_ID"] and y["value"]
                ),
                None,
            )
            full_civ = any(
                y["marker"] == GAME_DATA_MARKERS["CIV_LEVEL"] and y["value"] == FULL_CIV
                for y in x["value"]
            )
            # Only player arrays carry a user id; keep a human seat even if
            # its leader or civ is missing, so the report still lists it.
            if (leader and civ and full_civ) or user_id:
                players.append(
                    {
                        "leader": leader,
                        "civ": civ,
                        "user_id": user_id,
                        "team_id": team_id,
                    }
                )
    players.sort(key=lambda x: 0 if x["team_id"] is None else x["team_id"]["value"])

    return {
        "turn": find_marker(data["group1"], GAME_DATA_MARKERS["GAME_TURN"]),
        "age": find_marker(data["group1"], GAME_DATA_MARKERS["GAME_AGE"]),
        "map": find_marker(data["group1"], GAME_DATA_MARKERS["MAP_TYPE"]),
        "players": players,
        "rawData": data,
    }


def parse_raw(data: bytes) -> dict[str, list[dict[str, Any]]]:
    if data[0:4] != b"CIV7":
        raise Exception("Not a CIV 7 save file!")

    def last_end_offset(chunks, fallback):
        return chunks[-1]["endOffset"] if chunks else fallback

    log("Group 1:")
    group1_len = struct.unpack_from("<I", data, 8)[0]
    group1 = read_n_chunks(data, 12, group1_len)
    off = last_end_offset(group1, 12)

    log("Group 2:")
    group2_len = struct.unpack_from("<I", data, off + 8)[0]
    group2 = read_n_chunks(data, off + 12, group2_len)
    off = last_end_offset(group2, off)

    log("Group 3")
    group3_len = struct.unpack_from("<I", data, off + 4)[0]
    group3 = read_n_chunks(data, off + 8, group3_len)
    off = last_end_offset(group3, off)

    log("Group 4")
    # The separator counts its own chunks; realtime saves have none, hotseat saves some.
    between_len = struct.unpack_from("<I", data, off + 4)[0]
    between = read_n_chunks(data, off + 8, between_len)
    group4_at = last_end_offset(between, off + 8) + 8
    group4_len = struct.unpack_from("<I", data, group4_at)[0]
    group4 = read_n_chunks(data, group4_at + 4, group4_len)

    log("Group 5")
    # Its header varies and nothing here reads it, so a miss leaves it empty.
    try:
        group5 = _next_group(data, last_end_offset(group4, group4_at + 4))
    except Exception:
        group5 = []

    return {
        "group1": group1,
        "group2": group2,
        "group3": group3,
        "group4": group4,
        "group5": group5,
    }


def _next_group(data: bytes, offset: int) -> list[dict[str, Any]]:
    """The group starting at the first plausible marker after `offset` (a hash
    of at least 256, then a type of 1 to 32); its length is the word before."""
    for at in range(offset, min(offset + 64, len(data) - 7), 4):
        marker, type_ = struct.unpack_from("<II", data, at)
        if marker >= 256 and 0 < type_ <= 32:
            return read_n_chunks(data, at, struct.unpack_from("<I", data, at - 4)[0])
    raise ValueError(f"No group after offset {offset}")


def read_n_chunks(data: bytes, offset: int, num_chunks: int) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    for i in range(num_chunks):
        prev_end = chunks[-1]["endOffset"] if chunks else offset
        result = parse_chunk(data, prev_end)
        log(f"{prev_end}/{hex(prev_end)}: {result} {result['marker'].hex()}")
        chunks.append(result)
    return chunks


def parse_chunk(data: bytes, offset: int, has_marker: bool = True) -> dict[str, Any]:
    # A nested array's items have no marker: just the type, then the body.
    marker = data[offset : offset + 4] if has_marker else b""
    type_ = struct.unpack_from("<I", data, offset + 4 if has_marker else offset)[0]
    data_start_offset = offset + 12 if has_marker else offset + 8

    if type_ == ChunkType.Unknown_1 or type_ == ChunkType.Unknown_12:
        end_offset = data_start_offset + 12
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": data[data_start_offset:end_offset],
        }
    elif type_ == ChunkType.Unknown_9:
        len_ = struct.unpack_from("<H", data, data_start_offset)[0]
        end_offset = data_start_offset + 8 + len_ * 4
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": data[data_start_offset + 8 : end_offset],
        }
    elif type_ in (ChunkType.Unknown_10, ChunkType.Unknown_11, ChunkType.Unknown_17):
        len_ = struct.unpack_from("<H", data, data_start_offset)[0]
        end_offset = data_start_offset + 8 + len_ * 8
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": data[data_start_offset + 4 : end_offset],
        }
    elif type_ == ChunkType.Number32:
        value = struct.unpack_from("<I", data, data_start_offset + 8)[0]
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": data_start_offset + 12,
            "marker": marker,
            "type": type_,
            "value": value,
        }
    elif type_ == ChunkType.Utf8String:
        len_ = struct.unpack_from("<H", data, data_start_offset)[0]
        end_offset = data_start_offset + 8 + len_
        value = data[data_start_offset + 8 : end_offset - 1].decode("utf-8")
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": value,
        }
    elif type_ == ChunkType.Utf16String:
        len_ = struct.unpack_from("<H", data, data_start_offset)[0]
        end_offset = data_start_offset + 8 + len_ * 2
        value = data[data_start_offset + 8 : end_offset - 2].decode("utf-16le")
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": value,
        }
    elif type_ == ChunkType.ChunkArray:
        sub_chunk_count = struct.unpack_from("<I", data, data_start_offset + 8)[0]
        sub_chunks = read_n_chunks(data, data_start_offset + 12, sub_chunk_count)
        end_offset = (
            sub_chunks[-1]["endOffset"] if sub_chunks else data_start_offset + 12
        )
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": sub_chunks,
        }
    elif type_ == ChunkType.NestedArray:
        item_count = struct.unpack_from("<I", data, data_start_offset + 8)[0]
        result = []
        end_offset = data_start_offset + 12
        for _ in range(item_count):
            # Usually a record, kept as its chunks; sometimes a lone value.
            item = parse_chunk(data, end_offset, has_marker=False)
            chunk_array = item["type"] == ChunkType.ChunkArray
            result.append(item["value"] if chunk_array else [item])
            end_offset = item["endOffset"]
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": result,
        }
    elif type_ == ChunkType.Unknown_32:
        len_ = struct.unpack_from("<I", data, data_start_offset + 4)[0]
        end_offset = data_start_offset + 8 + len_
        return {
            "offset": offset,
            "dataStartOffset": data_start_offset,
            "endOffset": end_offset,
            "marker": marker,
            "type": type_,
            "value": data[data_start_offset + 8 : end_offset],
        }
    elif type_ == ChunkType.Unknown_long:
        res = parse_chunk(data, offset + 4)
        res["offset"] = offset
        return res
    else:
        print(f"Unknown chunk type {type_} at offset {offset}!", file=sys.stderr)
        raise Exception(f"Could not parse chunk at offset {offset}!")


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
    for p in root["players"]:
        civ = p["civ"]["value"] if p["civ"] is not None else "CIVILIZATION_NONE"
        leader = p["leader"]["value"] if p["leader"] is not None else "LEADER_NONE"
        team = int(p["team_id"]["value"]) if p["team_id"] is not None else 0
        # normalize team ids to 0..n-1 where n is the number of teams
        if team not in teams_dict:
            teams_dict[team] = len(teams_dict)
        team = teams_dict[team]
        steam_id = (
            p["user_id"]["value"].split("@")[-1] if p["user_id"] is not None else None
        )
        user_name = (
            p["user_id"]["value"].split("@")[0] if p["user_id"] is not None else None
        )
        players.append(
            {
                "steam_id": steam_id,
                "user_name": user_name,
                "civ": civ,
                "leader": leader,
                "team": team,
                "placement": team,
            }
        )
    return players


def extract_turn(root):
    return int(root["turn"]["value"])


def extract_game_age(root):
    return root["age"]["value"]


def extract_map_type(root):
    parsed = json.loads(root["map"]["value"])
    for entry, langs in parsed.items():
        for lang in langs:
            if lang["locale"] == "en_US":
                return lang["text"]
        return entry
    raise ValueError("extract_map_type: no map entry in the localisation blob")


def parse_civ7_save(file_bytes: bytes, version: str = "1.1"):
    root = parse(file_bytes)

    players = extract_player_info(root)
    turn = extract_turn(root)
    age = extract_game_age(root)
    map_type = extract_map_type(root)
    game_mode = determine_game_mode(players)

    return {
        "game": "civ7",
        "age": age,
        "turn": turn,
        "players": players,
        "game_mode": game_mode,
        "map_type": map_type,
        "parser_version": version,
    }


def main():
    parser = argparse.ArgumentParser(description="Parse a CIV 7 save file.")
    parser.add_argument("filename", nargs="?", help="The CIV 7 save file to parse.")
    args = parser.parse_args()

    if not args.filename:
        print("Please pass the filename as the argument to the script.")
        return

    with open(args.filename, "rb") as f:
        buffer = f.read()
        result = parse_civ7_save(buffer, "1.0.0")
        print("Parsed data:", json.dumps(result, indent=4))


if __name__ == "__main__":
    main()
