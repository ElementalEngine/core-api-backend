#!/usr/bin/env python3
"""differential <v1_url> [<validated_match_id> ...]

The same approve through v1 (production's code) and through v2, compared on
the numbers that rate a player. Dev only; run before cutover, on the final
code, with v1 serving at <v1_url> against the dev cluster.

Each validated match in games, or each id given, is cloned as a pending match
and approved twice: by v1, which reads match_reporter, and by v2, which reads
games. Everything either approve wrote is put back before the next one,
including v2's rating events and sub events, so the stat documents and the
ledger end exactly where they began.

Compared, per rated player and per stat document v2 writes: mu, sigma, games,
wins, first, subbedIn, subbedOut; per player: delta, season_delta,
combined_delta; and the affected players returned. Not compared, because they
changed on purpose: civ and leader tallies (token keys), the sub counter
(replaced by sub events), and the seasonal documents v1 writes for cloud games.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

# The repo is not installed as a package, so `app` is importable only with
# the repo root on sys.path -- running a file in scripts/ puts scripts/ there.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bson import ObjectId
from bson.int64 import Int64
from pymongo import MongoClient

from app.core.config import settings
from app.core.constants import DB_SERVER_MEMBERS, GAMES_DB
from app.features.matches.tallies import stat_legs
from app.features.ratings.scope import stats_collection_name, stats_db_name

V1_DB = "match_reporter"
APPROVER = "1"
RATED = ("mu", "sigma", "games", "wins", "first", "subbedIn", "subbedOut")
DELTAS = ("delta", "season_delta", "combined_delta")
ALL_LEGS = ((False, False), (True, False), (False, True))
# The byte hash is unique across validated matches, so a clone cannot carry it.
NOT_CLONED = (
    "_id",
    "approved_at",
    "approver_discord_id",
    "approving_at",
    "save_bytes_sha256",
)

Doc = dict[str, Any]


def connect() -> MongoClient[Doc]:
    uri = settings.mongo_url.get_secret_value()
    if settings.api_port != 8001 or "-dev" not in uri:
        sys.exit("refusing: not the dev checkout or not the dev cluster")
    return MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=5000)


def rated(match: Doc) -> list[str]:
    ids = [str(p.get("discord_id") or "") for p in match["players"]]
    return [i for i in ids if i and i not in ("-1", "-2") and not i.startswith("-")]


def address(match: Doc, is_seasonal: bool, is_combined: bool) -> tuple[str, str]:
    db = stats_db_name(civ_version=match["game"], is_seasonal=is_seasonal)
    col = stats_collection_name(
        match_type=match["game_mode"],
        is_cloud=bool(match.get("is_cloud")),
        is_combined=is_combined,
    )
    return db, col


def read_stats(c: MongoClient[Doc], match: Doc) -> dict[str, Doc | None]:
    """Every stat document either version may write. Absence is None."""
    out: dict[str, Doc | None] = {}
    for did in rated(match):
        for is_seasonal, is_combined in ALL_LEGS:
            db, col = address(match, is_seasonal, is_combined)
            out[f"{db}.{col}.{did}"] = c[db][col].find_one({"_id": Int64(did)})
    return out


def read_subs(c: MongoClient[Doc], match: Doc) -> dict[str, Doc | None]:
    subs = c[DB_SERVER_MEMBERS]["subs"]
    return {did: subs.find_one({"_id": Int64(did)}) for did in rated(match)}


def put_back(c: MongoClient[Doc], stats: dict[str, Doc | None], subs: dict) -> None:
    for key, doc in stats.items():
        db, col, did = key.split(".")
        if doc is None:
            c[db][col].delete_one({"_id": Int64(did)})
        else:
            c[db][col].replace_one({"_id": doc["_id"]}, doc, upsert=True)
    for did, doc in subs.items():
        if doc is None:
            c[DB_SERVER_MEMBERS]["subs"].delete_one({"_id": Int64(did)})
        else:
            c[DB_SERVER_MEMBERS]["subs"].replace_one(
                {"_id": doc["_id"]}, doc, upsert=True
            )


def drop_created(c: MongoClient[Doc], before: dict[str, set[str]]) -> None:
    """A collection v1 recreated and the restore emptied (v1 still writes the
    seasonal cloud documents v2 no longer does) is dropped again."""
    for db, names in before.items():
        for name in set(c[db].list_collection_names()) - names:
            if c[db][name].estimated_document_count() == 0:
                c[db].drop_collection(name)


def call(url: str, *, method: str, body: bytes, headers: dict[str, str]) -> Any:
    token = settings.mito_service_token.get_secret_value()
    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Authorization": f"Bearer {token}", **headers},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"{method} {url} -> {e.code}: {e.read().decode()[:300]}"
        ) from e


def approve_v1(c: MongoClient[Doc], base: str, pending: Doc) -> dict[str, Any]:
    known = {d["_id"] for d in c[V1_DB].validated_matches.find({}, {"_id": 1})}
    c[V1_DB].pending_matches.insert_one(pending)
    try:
        form = {"match_id": str(pending["_id"]), "approver_discord_id": APPROVER}
        response = call(
            f"{base}/api/v1/approve-match/",
            method="PUT",
            body=urllib.parse.urlencode(form).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        new = [d for d in c[V1_DB].validated_matches.find() if d["_id"] not in known]
        return {"response": response, "validated": new[0] if len(new) == 1 else None}
    finally:
        c[V1_DB].validated_matches.delete_many({"_id": {"$nin": list(known)}})
        c[V1_DB].pending_matches.delete_one({"_id": pending["_id"]})


def approve_v2(c: MongoClient[Doc], pending: Doc) -> dict[str, Any]:
    oid = pending["_id"]
    c[GAMES_DB].pending_matches.insert_one(pending)
    try:
        response = call(
            f"http://127.0.0.1:{settings.api_port}/api/v2/matches/{oid}/approve",
            method="POST",
            body=b"",
            headers={"X-Actor-Discord-Id": APPROVER, "X-Actor-Is-Staff": "true"},
        )
        validated = c[GAMES_DB].validated_matches.find_one({"_id": oid})
        return {"response": response, "validated": validated}
    finally:
        c[GAMES_DB].validated_matches.delete_one({"_id": oid})
        c[GAMES_DB].pending_matches.delete_one({"_id": oid})
        c[GAMES_DB].rating_events.delete_many({"match_id": oid})
        c[GAMES_DB].sub_events.delete_many({"match_id": oid})


def numbers(match: Doc, capture: dict[str, Any], stats: dict[str, Doc | None]) -> Doc:
    """The values the rating rules freeze, from one approve's documents."""
    out: Doc = {}
    for is_seasonal, is_combined in stat_legs(is_cloud=bool(match.get("is_cloud"))):
        db, col = address(match, is_seasonal, is_combined)
        for did in rated(match):
            doc = stats[f"{db}.{col}.{did}"]
            out[f"{db}.{col}.{did}"] = (
                None if doc is None else {f: doc.get(f) for f in RATED}
            )
    validated = capture["validated"] or {}
    for i, p in enumerate(validated.get("players", [])):
        out[f"players.{i}"] = {f: p.get(f) for f in ("discord_id", *DELTAS)}
    affected = (capture["response"] or {}).get("affected_players")
    if affected is not None:
        out["affected_players"] = sorted(
            (str(a["discord_id"]), float(a["rating_mu"])) for a in affected
        )
    return out


def differences(a: Doc, b: Doc) -> list[str]:
    return [
        f"  {k}\n    v1: {a.get(k)}\n    v2: {b.get(k)}"
        for k in sorted(set(a) | set(b))
        if a.get(k) != b.get(k)
    ]


def one(c: MongoClient[Doc], base: str, source: Doc) -> bool | None:
    label = f"{source['_id']} {source['game']} {source['game_mode']}"
    label += " cloud" if source.get("is_cloud") else ""
    pending = {k: v for k, v in source.items() if k not in NOT_CLONED}
    pending["_id"] = ObjectId()
    if len(rated(pending)) < 2:
        print(f"skip {label}: fewer than two rated players")
        return None

    stats, subs = read_stats(c, pending), read_subs(c, pending)
    dbs = {
        stats_db_name(civ_version=pending["game"], is_seasonal=s) for s in (False, True)
    }
    before = {db: set(c[db].list_collection_names()) for db in dbs}
    try:
        try:
            v1 = approve_v1(c, base, pending)
            v1_stats = read_stats(c, pending)
        finally:
            put_back(c, stats, subs)
            drop_created(c, before)
        try:
            v2 = approve_v2(c, pending)
            v2_stats = read_stats(c, pending)
        finally:
            put_back(c, stats, subs)
            drop_created(c, before)
    except RuntimeError as e:
        print(f"FAILED {label}: {e}")
        return False

    a, b = numbers(pending, v1, v1_stats), numbers(pending, v2, v2_stats)
    found = differences(a, b)
    if found:
        print(f"DIFFERENT {label}: {len(found)}")
        print("\n".join(found))
        return False

    # The comparison must be able to fail: alter one value and expect it seen.
    some = next(k for k, v in b.items() if isinstance(v, dict) and "mu" in v)
    if not differences(a, {**b, some: {**b[some], "mu": b[some]["mu"] + 1e-9}}):
        sys.exit("STOP: the comparison did not see an altered mu")
    slots = sum(1 for v in b.values() if isinstance(v, dict) and "mu" in v)
    print(f"identical {label}: {len(rated(pending))} players, {slots} stat documents")
    return True


def main(argv: list[str]) -> int:
    if not argv:
        sys.exit(__doc__)
    base, ids = argv[0].rstrip("/"), argv[1:]
    c = connect()
    try:
        query = {"_id": {"$in": [ObjectId(i) for i in ids]}} if ids else {}
        sources = list(c[GAMES_DB].validated_matches.find(query).sort("_id", 1))
        results = [one(c, base, s) for s in sources]
    finally:
        c.close()
    ran = [r for r in results if r is not None]
    print(f"{ran.count(True)} of {len(ran)} identical, {results.count(None)} skipped")
    return 0 if ran and all(ran) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
