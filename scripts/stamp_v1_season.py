#!/usr/bin/env python3
"""stamp_v1_season [--apply]

Gives the open season's id to the matches v1 approved in it.

v1 stamps no season, and v2 reads a Civ 6 match without one as Season 6's or
older (V1_LABELS). Season 6 was ended on v1 by hand, so when v2 starts v1 has
already approved matches in Season 7: unstamped, a revert would leave their
season stats alone.

For each edition: the validated matches with no season_id whose _id is not
older than the open season's started_at. v1 gives a match a new _id when it
approves it, so the _id holds the approval time. A season seeded at its
tracking start has no such match.

Run after seed_seasons. Idempotent: a stamped match is not selected again. Dry
run unless --apply. Export first.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bson import ObjectId
from pymongo import DESCENDING, MongoClient

from app.core.config import settings
from app.core.constants import COL_SEASONS, EDITIONS, GAMES_DB
from app.features.matches.repository import COL_VALIDATED_MATCHES


def main(argv: list[str]) -> int:
    apply = "--apply" in argv
    rest = [a for a in argv if a != "--apply"]
    if rest:
        print(f"unknown argument(s): {rest}. only --apply is accepted.")
        return 2

    client = MongoClient(settings.mongo_url.get_secret_value())
    try:
        db = client[GAMES_DB]
        matches = db[COL_VALIDATED_MATCHES]
        # Every edition's open season is read before anything is written.
        seasons = {
            edition: db[COL_SEASONS].find_one(
                {"edition": edition}, sort=[("started_at", DESCENDING)]
            )
            for edition in EDITIONS
        }
        missing = [edition for edition, season in seasons.items() if season is None]
        if missing:
            print(f"no season row for {missing}. run seed_seasons first.")
            return 1
        for edition, season in seasons.items():
            unstamped = {
                "game": edition,
                "season_id": None,
                "_id": {"$gte": ObjectId.from_datetime(season["started_at"])},
            }
            found = matches.count_documents(unstamped)
            print(
                f"{edition}: {found} matches approved since {season['label']} "
                f"began ({season['started_at']:%Y-%m-%d %H:%M:%S} UTC) carry no season"
            )
            if apply and found:
                stamped = matches.update_many(
                    unstamped, {"$set": {"season_id": season["_id"]}}
                ).modified_count
                print(f"{edition}: stamped {stamped} with {season['label']}")
        if not apply:
            print("dry run -- nothing written. pass --apply to write.")
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
