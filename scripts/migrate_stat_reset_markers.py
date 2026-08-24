#!/usr/bin/env python3
"""migrate_stat_reset_markers [--apply]

Playbook Entry 6. Moves the stat_reset markers out of validated_matches and
into stat_resets, so every document in validated_matches is a match.

The markers carry no timestamp -- the reset date lives only inside the
ObjectId. The move preserves _id and writes that date out as occurred_at, so
it survives anything that ever re-inserts these documents.

The stat_reset flag is dropped: it existed to tell markers apart from matches
inside one collection, and every document here is a reset.

Copy, verify, then delete. Idempotent: a re-run after a partial one finds the
markers still in place, upserts them again for no change, and finishes the
delete. Dry run unless --apply. Export first, D115.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient  # noqa: E402

from app.core.config import settings  # noqa: E402
from app.core.constants import COL_STAT_RESETS, GAMES_DB  # noqa: E402
from app.features.matches.repository import COL_VALIDATED_MATCHES  # noqa: E402

MARKER = {"stat_reset": {"$exists": True}}


def to_reset(marker: Dict[str, Any]) -> Dict[str, Any]:
    """The marker as a stat_resets document: dated, and without the flag."""
    return {
        "_id": marker["_id"],
        "occurred_at": marker["_id"].generation_time,
        "civ_version": marker.get("civ_version"),
        "is_cloud": bool(marker.get("is_cloud")),
        "discord_id": marker.get("discord_id"),
    }


def main(argv: List[str]) -> int:
    apply = "--apply" in argv
    rest = [a for a in argv if a != "--apply"]
    if rest:
        print(f"unknown argument(s): {rest}. only --apply is accepted.")
        return 2

    client = MongoClient(settings.mongodb_uri.get_secret_value())
    try:
        db = client[GAMES_DB]
        matches = db[COL_VALIDATED_MATCHES]
        resets = db[COL_STAT_RESETS]

        markers = list(matches.find(MARKER).sort("_id", 1))
        print(f"{len(markers)} markers in {COL_VALIDATED_MATCHES}")
        print(f"{resets.count_documents({})} documents already in {COL_STAT_RESETS}\n")
        for marker in markers:
            doc = to_reset(marker)
            print(
                f"  {doc['_id']}  {doc['occurred_at'].isoformat()}  "
                f"{doc['civ_version']}  cloud={doc['is_cloud']}  "
                f"discord_id={doc['discord_id']}"
            )

        if not apply:
            print("\ndry run -- nothing written. pass --apply to write.")
            return 0
        if not markers:
            print("\nnothing to move")
            return 0

        for marker in markers:
            doc = to_reset(marker)
            resets.replace_one({"_id": doc["_id"]}, doc, upsert=True)

        # Verify every marker landed before deleting the only copy.
        ids = [m["_id"] for m in markers]
        copied = resets.count_documents({"_id": {"$in": ids}})
        if copied != len(ids):
            print(f"\nABORT: copied {copied} of {len(ids)}. Nothing deleted.")
            return 1

        deleted = matches.delete_many({"_id": {"$in": ids}}).deleted_count
        print(f"\ncopied {copied}, deleted {deleted}")
        print(f"markers remaining: {matches.count_documents(MARKER)}")
        print(f"{COL_STAT_RESETS} total: {resets.count_documents({})}")
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
