#!/usr/bin/env python3
"""rebuild_civ_tallies [--apply] [--edition civ6|civ7]

Playbook Entry 5. Rebuilds the civs tally on every stat document from
validated_matches, and builds the leaders tally beside it.

Counting is tallies.bump(), the same function the approve path calls, so a
rebuild and the next approve cannot disagree. Scopes come from
tallies.stat_legs(): lifetime and combined always, season only for realtime.

Every scope is cleared, not only the ones with matches -- a scope whose
matches were all reverted must end up empty, not stale.

A record still holding a LEADER_* token in civ has not been through the
Entry 10 migration, or its leader has no civ token yet. The leader is known
there and the civ is not, so it counts once, in leaders.

Never creates a stat document. A player with matches and no document was
reset; the reset deleted their tally with it and that is correct.

Dry run unless --apply. Export first, D115. mu and sigma are never written.
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient, UpdateOne

from app.core.config import settings
from app.core.constants import GAMES_DB
from app.features.matches.tallies import bump, is_rated, stat_legs
from app.features.ratings.scope import stat_scope

EDITIONS = ("civ6", "civ7")
MODES = ("ffa", "teamer", "duel")
CHUNK = 500


def game_mode(raw: Any) -> str:
    mode = str(raw or "").strip().lower()
    return "teamer" if mode == "team" else mode


def all_scopes(edition: str) -> list[str]:
    """Every scope this edition can write, from the rules rather than the data."""
    scopes = set()
    for is_cloud in (False, True):
        for is_seasonal, is_combined in stat_legs(is_cloud=is_cloud):
            for mode in ("ffa",) if is_combined else MODES:
                scopes.add(
                    stat_scope(
                        civ_version=edition,
                        is_seasonal=is_seasonal,
                        match_type=mode,
                        is_cloud=is_cloud,
                        is_combined=is_combined,
                    )
                )
    return sorted(scopes)


def read_tokens(player: dict[str, Any]) -> tuple[str | None, str | None]:
    """civ and leader for one player entry, or None where a token is unknown."""
    civ = player.get("civ") or None
    leader = player.get("leader") or None
    if civ and civ.startswith("LEADER_"):
        return None, leader or civ
    return civ, leader


def collect(db: Any, edition: str) -> dict[str, dict[int, dict[str, Any]]]:
    """One pass over an edition's matches, bucketed into every scope."""
    scopes: dict[str, dict[int, dict[str, Any]]] = defaultdict(
        lambda: defaultdict(lambda: {"civs": {}, "leaders": {}})
    )
    matches = 0
    unmigrated = 0
    for match in db["validated_matches"].find(
        {"game": edition, "stat_reset": {"$exists": False}}
    ):
        mode = game_mode(match.get("game_mode"))
        if mode not in MODES:
            print(f"  skipped {match.get('_id')}: game_mode {match.get('game_mode')!r}")
            continue
        matches += 1
        is_cloud = bool(match.get("is_cloud"))
        players = [p for p in match.get("players", []) if is_rated(p.get("discord_id"))]
        unmigrated += sum(1 for p in players if read_tokens(p)[0] is None)
        for is_seasonal, is_combined in stat_legs(is_cloud=is_cloud):
            scope = stat_scope(
                civ_version=edition,
                is_seasonal=is_seasonal,
                match_type=mode,
                is_cloud=is_cloud,
                is_combined=is_combined,
            )
            for player in players:
                entry = scopes[scope][int(player["discord_id"])]
                won = float(player.get("delta") or 0) > 0
                civ, leader = read_tokens(player)
                if civ:
                    bump(entry["civs"], civ, won=won, step=1)
                if leader:
                    bump(entry["leaders"], leader, won=won, step=1)
    print(f"{edition}: {matches} matches, {unmigrated} entries with no civ token")
    return scopes


def write(client: MongoClient, scope: str, tallies: dict[int, dict[str, Any]]) -> None:
    db_name, col_name = scope.split(".", 1)
    col = client[db_name][col_name]
    started = time.monotonic()

    cleared = col.update_many({}, {"$set": {"civs": {}, "leaders": {}}}).modified_count

    ops = [
        UpdateOne({"_id": pid}, {"$set": {"civs": t["civs"], "leaders": t["leaders"]}})
        for pid, t in tallies.items()
    ]
    matched = 0
    for i in range(0, len(ops), CHUNK):
        matched += col.bulk_write(ops[i : i + CHUNK], ordered=False).matched_count

    print(
        f"  {scope:36} cleared {cleared:6} · filled {matched:6} · "
        f"no document {len(ops) - matched:4} · {time.monotonic() - started:.1f}s"
    )


def main(argv: list[str]) -> int:
    apply = "--apply" in argv
    rest = [a for a in argv if a != "--apply"]
    edition = None
    if rest[:1] == ["--edition"]:
        edition, rest = rest[1], rest[2:]
    if rest:
        print(f"unknown argument(s): {rest}. only --apply and --edition are accepted.")
        return 2
    editions = (edition,) if edition else EDITIONS
    if any(e not in EDITIONS for e in editions):
        print(f"unknown edition: {edition}")
        return 2

    client = MongoClient(settings.mongodb_uri.get_secret_value())
    try:
        for name in editions:
            collected = collect(client[GAMES_DB], name)
            for scope in all_scopes(name):
                tallies = collected.get(scope, {})
                if apply:
                    write(client, scope, tallies)
                    continue
                db_name, col_name = scope.split(".", 1)
                docs = client[db_name][col_name].count_documents({})
                civ_keys = {k for t in tallies.values() for k in t["civs"]}
                leaders = {k for t in tallies.values() for k in t["leaders"]}
                print(
                    f"  {scope:36} {docs:6} docs · {len(tallies):5} players · "
                    f"{len(civ_keys):3} civs · {len(leaders):3} leaders"
                )
    finally:
        client.close()

    if not apply:
        print("\ndry run -- nothing written. pass --apply to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
