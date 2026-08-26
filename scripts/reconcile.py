#!/usr/bin/env python3
"""reconcile [--epsilon E]

Phase 2 deliverable #7. Sums rating_events deltas per (player_id, scope) and
compares against the stat document that scope names. The arithmetic lives in
app.features.ratings.reconciliation and is fixture-tested; this only fetches.

Exit 1 on any divergence. Run after Entry 8 Release B, after each S5 migration
touching stats, before cutover, and periodically in prod -- the failure it
detects has no symptom, so nobody will think to run it.
"""

from __future__ import annotations

import sys
from pathlib import Path

# The repo is not installed as a package, so `app` is importable only with
# the repo root on sys.path -- running a file in scripts/ puts scripts/ there.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bson.int64 import Int64
from pymongo import MongoClient

from app.core.config import settings
from app.core.constants import GAMES_DB
from app.features.ratings.reconciliation import EPSILON, reconcile


def main(argv: list[str]) -> int:
    epsilon = EPSILON
    if "--epsilon" in argv:
        epsilon = float(argv[argv.index("--epsilon") + 1])

    client = MongoClient(settings.mongodb_uri.get_secret_value(), tz_aware=True)
    try:
        events = list(client[GAMES_DB]["rating_events"].find())
        wanted = {(int(e["player_id"]), str(e["scope"])) for e in events}
        actual: dict[tuple[int, str], float | None] = {}
        for pid, scope in wanted:
            db_name, col_name = scope.split(".")
            doc = client[db_name][col_name].find_one({"_id": Int64(pid)})
            actual[(pid, scope)] = None if doc is None else doc["mu"]

        found = reconcile(events, actual, initial_mu=settings.ts_mu, epsilon=epsilon)
        missing = sum(1 for v in actual.values() if v is None)
        print(
            f"{len(events)} events, {len(wanted)} (player, scope) pairs, "
            f"{missing} with no stat document, epsilon={epsilon}"
        )
        for d in found:
            print(
                f"  DIVERGENT {d.player_id} {d.scope}: expected={d.expected!r} "
                f"actual={d.actual!r} by={d.amount!r} over {d.event_count} events"
            )
        print(f"{len(found)} divergent")
        return 1 if found else 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
