#!/usr/bin/env python3
"""migrate_civ6_leader_field [--apply]

Playbook Entry 10 Half B. Civ6 match records hold the LEADER_* token in
players[].civ and leave players[].leader unset. This moves the token to
leader and writes the mapped CIVILIZATION_* token into civ.

The mapping is read from civ_data, so a leader with no civ yet is left
alone -- its records keep matching the filter, which is also how a later
run picks them up once a save supplies the token.

Idempotent: a second run matches nothing, because the filter selects on the
shape being corrected. Dry run unless --apply is passed; this rewrites the
match store in place and there is no PITR (D61). Export first, D115.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

# The repo is not installed as a package, so `app` is importable only with
# the repo root on sys.path -- running a file in scripts/ puts scripts/ there.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient

from app.core.config import settings
from app.core.constants import COL_CIV_DATA, GAMES_DB

COL = "validated_matches"
FILTER = {"game": "civ6", "players.civ": {"$regex": "^LEADER_"}}


def main(argv: list[str]) -> int:
    unknown = [a for a in argv if a != "--apply"]
    if unknown:
        print(f"unknown argument(s): {unknown}. only --apply is accepted.")
        return 2
    apply = "--apply" in argv
    client = MongoClient(settings.mongodb_uri.get_secret_value())
    try:
        db = client[GAMES_DB]
        mapping = {
            row["token"]: row["civ"]
            for row in db[COL_CIV_DATA].find({"edition": "civ6", "kind": "leader"})
            if row.get("civ")
        }
        if not mapping:
            print("civ_data holds no civ6 mapping -- run seed_civ_data first")
            return 1

        tokens: Counter[str] = Counter()
        for match in db[COL].find(FILTER, {"players.civ": 1}):
            for player in match.get("players", []):
                civ = player.get("civ")
                if isinstance(civ, str) and civ.startswith("LEADER_"):
                    tokens[civ] += 1

        mapped = {t: n for t, n in tokens.items() if t in mapping}
        unmapped = {t: n for t, n in tokens.items() if t not in mapping}
        print(f"{db[COL].count_documents(FILTER)} documents match the filter")
        print(
            f"{sum(tokens.values())} player entries: "
            f"{sum(mapped.values())} mappable, {sum(unmapped.values())} unmapped"
        )
        for token, count in sorted(unmapped.items()):
            print(f"  left alone: {token} x{count}")

        if not apply:
            print("\ndry run -- nothing written. pass --apply to write.")
            return 0

        matched = modified = 0
        for token in sorted(mapped):
            result = db[COL].update_many(
                {"game": "civ6", "players.civ": token},
                {
                    "$set": {
                        "players.$[p].civ": mapping[token],
                        "players.$[p].leader": token,
                    }
                },
                array_filters=[{"p.civ": token}],
            )
            matched += result.matched_count
            modified += result.modified_count
            print(
                f"  {token:32} -> {mapping[token]:28} "
                f"matched {result.matched_count}, modified {result.modified_count}"
            )

        print(f"\n{matched} matched, {modified} modified")
        print(f"check 2 -- documents still matching: {db[COL].count_documents(FILTER)}")
        print(
            "check 3 -- documents with an unset player leader: "
            f"{db[COL].count_documents({'game': 'civ6', 'players.leader': None})}"
        )
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
