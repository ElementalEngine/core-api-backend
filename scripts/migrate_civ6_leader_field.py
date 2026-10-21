#!/usr/bin/env python3
"""migrate_civ6_leader_field [--apply]

Playbook Entry 10 Half B. Civ6 match records reach this in three shapes, and
every one is corrected to a CIVILIZATION_* token in civ and a LEADER_* token
in leader:

  leader token in civ   civ='LEADER_TRAJAN', no leader
  name key in both      civ='RomeTrajan', leader='RomeTrajan'
  no information        civ='Unknown' or 'None', no leader -- left as written

The mapping is civ_data's: a name key gives the leader token, the leader token
gives the civ token. A leader with no civ token yet keeps its leader token in
civ and no leader -- the first shape -- so a later run, once a save has
supplied the civ, picks it up like any other. Nothing that carries no
information is rewritten.

Idempotent: every update is filtered on the shape it corrects. Dry run unless
--apply is passed; this rewrites the match store in place and there is no
PITR. Export first.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

# The repo is not installed as a package, so `app` is importable only with
# the repo root on sys.path -- running a file in scripts/ puts scripts/ there.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient

from app.core.config import settings
from app.core.constants import COL_CIV_DATA, GAMES_DB

COL = "validated_matches"
CIV6 = {"game": "civ6"}

Row = Mapping[str, Any]
Pair = tuple[str, str | None]


def correction(
    civ: str,
    leader: str | None,
    by_name: Mapping[str, Row],
    by_token: Mapping[str, Row],
) -> Pair | None:
    """The (civ, leader) an entry should hold, or None to leave it as it is."""
    if civ.startswith("LEADER_") and not leader:
        row = by_token.get(civ)
        return (row["civ"], civ) if row and row.get("civ") else None
    if civ and civ == leader and civ in by_name:
        row = by_name[civ]
        if row.get("civ"):
            return row["civ"], row["token"]
        return row["token"], None
    return None


def distinct_pairs(db: Any) -> dict[Pair, int]:
    """Every (civ, leader) a civ6 player entry holds, with its count."""
    rows = db[COL].aggregate(
        [
            {"$match": CIV6},
            {"$unwind": "$players"},
            {
                "$group": {
                    "_id": {
                        "civ": {"$ifNull": ["$players.civ", ""]},
                        "leader": {"$ifNull": ["$players.leader", ""]},
                    },
                    "n": {"$sum": 1},
                }
            },
        ],
        allowDiskUse=True,
    )
    return {(r["_id"]["civ"], r["_id"]["leader"] or None): r["n"] for r in rows}


def report(pairs: Mapping[Pair, int], plan: Mapping[Pair, Pair]) -> None:
    corrected = sum(n for p, n in pairs.items() if p in plan)
    tokens = sum(
        n
        for (civ, leader), n in pairs.items()
        if civ.startswith("CIVILIZATION_") and (leader or "").startswith("LEADER_")
    )
    left = {
        p: n
        for p, n in pairs.items()
        if p not in plan and not p[0].startswith("CIVILIZATION_")
    }
    print(f"{sum(pairs.values())} civ6 player entries: {tokens} already tokens")
    print(f"  {corrected} to correct, in {len(plan)} distinct (civ, leader) pairs")
    for (civ, leader), n in sorted(left.items(), key=lambda kv: -kv[1]):
        print(f"  left alone: civ={civ!r} leader={leader!r} x{n}")


def build_plan(db: Any) -> tuple[dict[Pair, int], dict[Pair, Pair]]:
    """The entries as they are, and the correction for each pair that has one."""
    rows = list(db[COL_CIV_DATA].find({"edition": "civ6", "kind": "leader"}))
    if not rows:
        raise SystemExit("civ_data holds no civ6 leaders -- run seed_civ_data first")
    by_token = {r["token"]: r for r in rows}
    by_name = {r["name"]: r for r in rows if r.get("name")}
    pairs = distinct_pairs(db)
    plan: dict[Pair, Pair] = {}
    for civ, leader in pairs:
        fixed = correction(civ, leader, by_name, by_token)
        if fixed is not None:
            plan[(civ, leader)] = fixed
    return pairs, plan


def apply_plan(db: Any, plan: Mapping[Pair, Pair]) -> int:
    """One update per pair, filtered on the shape it corrects."""
    modified = 0
    for (civ, leader), (new_civ, new_leader) in sorted(
        plan.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")
    ):
        shape = {"civ": civ, "leader": leader}
        result = db[COL].update_many(
            {**CIV6, "players": {"$elemMatch": shape}},
            {"$set": {"players.$[p].civ": new_civ, "players.$[p].leader": new_leader}},
            array_filters=[{"p.civ": civ, "p.leader": leader}],
        )
        modified += result.modified_count
        print(
            f"  {civ!r:34} -> {new_civ!r:30} {new_leader!r:30} {result.modified_count}"
        )
    return modified


def main(argv: list[str]) -> int:
    unknown = [a for a in argv if a != "--apply"]
    if unknown:
        print(f"unknown argument(s): {unknown}. only --apply is accepted.")
        return 2
    client = MongoClient(settings.mongo_url.get_secret_value())
    try:
        db = client[GAMES_DB]
        pairs, plan = build_plan(db)
        report(pairs, plan)
        if "--apply" not in argv:
            print("\ndry run -- nothing written. pass --apply to write.")
            return 0
        print(f"\n{apply_plan(db, plan)} documents modified")
        after = distinct_pairs(db)
        still = sum(n for p, n in after.items() if p in plan)
        print(f"check: entries still in a corrected shape: {still}")
        report(after, {})
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
