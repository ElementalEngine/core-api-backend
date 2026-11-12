#!/usr/bin/env python3
"""migrate_placements [--apply]

Counts the older history's placements from 0, as every other match does.

The matches brought in as older history (their _id dates from 20 Jan to
2 Mar 2026) store placements from 1: first place is 1. A match v1 or v2
approves stores them from 0, and the code reads 0 as first place: the first
places in the stats summary, the place on the history page, the first a revert
takes off a record. Read as they are, the older matches have no first place.

Selects the validated matches with no seat at placement 0 and takes 1 off every
seat's placement. Nothing is written unless every selected match has a whole
number on every seat and its lowest is 1. Ratings, deltas and stat documents
are not touched.

Idempotent: a corrected match has a seat at 0 and is not selected again, so a
run that stopped part way is finished by running it again. Dry run unless
--apply. Export first.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient

from app.core.config import settings
from app.core.constants import GAMES_DB
from app.features.matches.repository import COL_VALIDATED_MATCHES

# The older matches: none of their seats is at placement 0.
FROM_ONE: dict[str, Any] = {
    "players.0": {"$exists": True},
    "players": {"$not": {"$elemMatch": {"placement": 0}}},
}


def lowest(match: dict[str, Any]) -> int | None:
    """A match's lowest placement; None when a seat holds no whole number."""
    places = [seat.get("placement") for seat in match.get("players", [])]
    if any(isinstance(p, bool) or not isinstance(p, int) for p in places):
        return None
    return min(places)


def main(argv: list[str]) -> int:
    apply = "--apply" in argv
    rest = [a for a in argv if a != "--apply"]
    if rest:
        print(f"unknown argument(s): {rest}. only --apply is accepted.")
        return 2

    client = MongoClient(settings.mongo_url.get_secret_value())
    try:
        matches = client[GAMES_DB][COL_VALIDATED_MATCHES]
        total = matches.count_documents({})
        # Every selected match is read before anything is written.
        found = Counter(
            lowest(match) for match in matches.find(FROM_ONE, {"players.placement": 1})
        )
        selected = sum(found.values())
        print(f"{selected} of {total} matches have no seat at placement 0")
        if set(found) - {1}:
            for low in sorted(set(found) - {1}, key=str):
                what = "no whole number on a seat" if low is None else f"lowest {low}"
                print(f"  {found[low]} with {what}: not counted from 1")
            print("nothing written.")
            return 1
        if apply:
            corrected = matches.update_many(
                FROM_ONE, {"$inc": {"players.$[].placement": -1}}
            ).modified_count
            left = matches.count_documents(FROM_ONE)
            print(f"corrected {corrected}: their placements count from 0; {left} left")
            if corrected != selected or left:
                return 1
        else:
            print("dry run -- nothing written. pass --apply to write.")
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
