"""Each player's score, from the save's end-of-game score history. Used only
when it lines up with the save; otherwise there is no score."""

from __future__ import annotations

import struct
import zlib

# The compressed game state: length-prefixed blocks of one zlib stream.
_STATE = bytes([0x00, 0x00, 0x01, 0x00, 0x78, 0x9C])
# A series is named by a length-prefixed string; "Score" is the score's.
_SERIES = b"\x05\x00\x00\x00Score"
# Real states inflate about 11 to 1; an upload is at most 12 MiB.
_MAX_STATE = 256 * 1024 * 1024


def _inflate(data: bytes) -> bytes | None:
    at = data.find(_STATE)
    if at < 0:
        return None
    blocks = []
    size = int.from_bytes(data[at : at + 4], "little")
    pos = at + 4
    while size > 1 and pos + size <= len(data):
        blocks.append(data[pos : pos + size])
        pos += size
        if pos + 4 > len(data):
            break
        size = int.from_bytes(data[pos : pos + 4], "little")
        pos += 4
    inflater = zlib.decompressobj()
    try:
        state = inflater.decompress(b"".join(blocks), _MAX_STATE)
    except zlib.error:
        return None
    return None if inflater.unconsumed_tail else state


def final_scores(
    data: bytes, alive: list[bool], turn: int
) -> list[tuple[int, int]] | None:
    """(last turn, score) for each civ, or None unless the series match the save:
    one per civ, in order, ending last exactly for the civs still alive."""
    blob = _inflate(data)
    if blob is None:
        return None
    found: list[tuple[int, int]] = []
    at = blob.find(_SERIES)
    while at >= 0:
        head = at + len(_SERIES)
        if head + 12 > len(blob):
            return None
        number, zero, count = struct.unpack_from("<III", blob, head)
        end = head + 12 + 8 * count
        if number != len(found) + 1 or zero != 0 or not 0 < count <= turn + 1:
            return None
        if end > len(blob):
            return None
        pairs = list(struct.iter_unpack("<II", blob[head + 12 : end]))
        turns = [t for t, _ in pairs]
        if any(b <= a for a, b in zip(turns, turns[1:])) or turns[-1] > turn:
            return None
        found.append(pairs[-1])
        at = blob.find(_SERIES, end)
    if not found or len(found) != len(alive):
        return None
    last = max(t for t, _ in found)
    if any((t == last) != living for (t, _), living in zip(found, alive)):
        return None
    return found


__all__ = ["final_scores"]
