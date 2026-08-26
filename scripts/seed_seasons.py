#!/usr/bin/env python3
"""seed_seasons [--dry-run]

Playbook Entry 11 step 3. Creates the two seed rows in `seasons`, one per
edition. `started_at` is the migration timestamp, recorded as the TRACKING
start, not the true start of either season (D104).

Deliberately NOT idempotent: a second run must fail with E11000 against
unique_label_per_edition (Entry 11 checks 5 and 6). That index is what stops
the future rollover script running twice, so proving it here proves that too.

Not run at startup. The indexes are ensured there; the data is a deliberate
operation with an operator behind it.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

# The repo is not installed as a package, so `app` is importable only with
# the repo root on sys.path -- running a file in scripts/ puts scripts/ there.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import AsyncMongoClient

from app.core.config import settings
from app.features.seasons.repository import (
    SeasonsAlreadySeededError,
    SeasonsRepository,
    seed_documents,
)


async def main(argv: list[str]) -> int:
    unknown = [a for a in argv if a != "--dry-run"]
    if unknown:
        print(f"unknown argument(s): {unknown}. only --dry-run is accepted.")
        return 2
    dry_run = "--dry-run" in argv

    now = datetime.now(UTC)
    documents = seed_documents(now)
    for doc in documents:
        print(f"{doc['edition']}: {doc['label']} started_at={doc['started_at']}")

    if dry_run:
        print("dry run -- nothing written")
        return 0

    client = AsyncMongoClient(settings.mongo_url.get_secret_value(), tz_aware=True)
    try:
        repo = SeasonsRepository(client)
        await repo.ensure_indexes()
        try:
            inserted = await repo.seed(documents)
        except SeasonsAlreadySeededError:
            print("already seeded (E11000) -- nothing written")
            return 1
        print(f"inserted {inserted}")
        for edition in ("civ6", "civ7"):
            row = await repo.get_current_season(edition)
            print(f"{edition}: current -> {row['label']} (_id={row['_id']})")
    finally:
        await client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv[1:])))
