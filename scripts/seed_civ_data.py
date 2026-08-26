#!/usr/bin/env python3
"""seed_civ_data [--dry-run]

Playbook Entry 9 step 3. Upserts the authored seed files into civ_data and
prunes tokens the files no longer carry. Idempotent: a second run reports
zero upserted and zero pruned, which is also how a partial run resumes.

Not run at startup. The indexes are ensured there; the data is a deliberate
operation with an operator behind it.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# The repo is not installed as a package, so `app` is importable only with
# the repo root on sys.path -- running a file in scripts/ puts scripts/ there.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import AsyncMongoClient

from app.core.config import settings
from app.features.civdata.repository import CivDataRepository
from app.features.civdata.seeds import EDITIONS, to_documents


async def main(argv: list[str]) -> int:
    unknown = [a for a in argv if a != "--dry-run"]
    if unknown:
        print(f"unknown argument(s): {unknown}. only --dry-run is accepted.")
        return 2
    dry_run = "--dry-run" in argv

    for edition in EDITIONS:
        docs = to_documents(edition)
        kinds = {"leader": 0, "civ": 0}
        for doc in docs:
            kinds[doc["kind"]] += 1
        print(
            f"{edition}: {len(docs)} documents "
            f"({kinds['leader']} leaders, {kinds['civ']} civs), "
            f"version {docs[0]['leader_data_version']}"
        )

    if dry_run:
        print("dry run -- nothing written")
        return 0

    client = AsyncMongoClient(settings.mongo_url.get_secret_value(), tz_aware=True)
    try:
        repo = CivDataRepository(client)
        await repo.ensure_indexes()
        for edition in EDITIONS:
            result = await repo.seed(edition, to_documents(edition))
            print(
                f"{edition}: upserted {result['upserted']}, "
                f"modified {result['modified']}, pruned {result['pruned']}, "
                f"total {result['total']}"
            )
    finally:
        await client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv[1:])))
