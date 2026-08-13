#!/usr/bin/env python3
"""snapshot <match_id> | run <label> <url> | restore | diff <a> <b>

Order: snapshot -> run motor -> restore -> run pymongo -> diff.
Restore cannot use revert-match: revert computes sigma + 2 rather than
restoring the prior value (D66), so the snapshot is the only pre-state.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from bson import ObjectId, json_util
from pymongo import MongoClient

OUT = Path(os.environ.get("DIFF_DIR", "/home/cisco/s2-baseline"))
MATCH_DB = "match_reporter"
IGNORED_FIELDS = {"lastModified", "approved_at"}
# generated per insert, so never equal across runs; _id stays compared in stats
SECTION_IGNORED = {"validated": {"_id"}, "response": {"match_id"}}


def connect() -> MongoClient:
    uri = os.environ["MONGO_URL"]
    if "MONGO_URL_ALLOW_ANY" not in os.environ and "-dev" not in uri:
        sys.exit("refusing: MONGO_URL does not look like the dev cluster")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def stat_dbs(game: str) -> list[str]:
    return [f"{game}_lifetime_stats", f"{game}_season_stats"]


def read_stats(c: MongoClient, game: str, ids: list[int]) -> dict:
    """Every stat collection for these players. Absence is recorded as None."""
    out = {}
    for db in stat_dbs(game):
        for col in sorted(c[db].list_collection_names()):
            for pid in ids:
                doc = c[db][col].find_one({"_id": pid})
                out[f"{db}.{col}.{pid}"] = doc
    return out


def write(name: str, payload: dict) -> Path:
    OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
    p = OUT / name
    p.write_text(json_util.dumps(payload, indent=2))
    p.chmod(0o600)
    return p


def read(name: str) -> dict:
    return json_util.loads((OUT / name).read_text())


def cmd_snapshot(match_id: str) -> None:
    c = connect()
    match = c[MATCH_DB].pending_matches.find_one({"_id": ObjectId(match_id)})
    if match is None:
        sys.exit(f"no pending match {match_id}")

    ids = [int(p["discord_id"]) for p in match["players"]
           if p.get("discord_id") and not str(p["discord_id"]).startswith("-")]
    if len(ids) < 2:
        sys.exit(f"only {len(ids)} non-placeholder players; nothing to rate")

    snap = {
        "match_id": match_id,
        "game": match["game"],
        "player_ids": ids,
        "pending": match,
        "validated_ids": [d["_id"] for d in c[MATCH_DB].validated_matches.find({}, {"_id": 1})],
        "stats": read_stats(c, match["game"], ids),
        "subs": {str(i): c["server_members"].subs.find_one({"_id": i}) for i in ids},
    }
    existing = sum(1 for v in snap["stats"].values() if v is not None)
    p = write("snapshot.json", snap)
    print(f"snapshot -> {p}")
    print(f"  {len(ids)} rated players, {len(snap['stats'])} stat slots, "
          f"{existing} exist, {len(snap["stats"]) - existing} absent")


def cmd_run(label: str, base_url: str) -> None:
    snap = read("snapshot.json")
    c = connect()

    body = urllib.parse.urlencode({
        "match_id": snap["match_id"],
        "approver_discord_id": os.environ["APPROVER_DISCORD_ID"],
    }).encode()
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/v1/approve-match/",
        data=body, method="PUT",
        headers={"Authorization": f"Bearer {os.environ['MITO_SERVICE_TOKEN']}",
                 "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, raw = r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read().decode()

    print(f"{label}: approve -> {status}")
    if status != 200:
        print(raw)
        sys.exit(f"{label}: approve failed; run restore before retrying")

    known = set(snap["validated_ids"])
    new_validated = [d for d in c[MATCH_DB].validated_matches.find()
                     if d["_id"] not in known]

    p = write(f"capture-{label}.json", {
        "status": status,
        "response": json.loads(raw),
        "validated": new_validated,
        "stats": read_stats(c, snap["game"], snap["player_ids"]),
        "subs": {str(i): c["server_members"].subs.find_one({"_id": i})
                 for i in snap["player_ids"]},
    })
    print(f"  captured {len(new_validated)} validated doc(s) -> {p}")


def cmd_restore() -> None:
    snap = read("snapshot.json")
    c = connect()
    counts = {"stat_restored": 0, "stat_deleted": 0}

    for key, doc in snap["stats"].items():
        db, col, pid = key.split(".")
        coll = c[db][col]
        if doc is None:
            counts["stat_deleted"] += coll.delete_one({"_id": int(pid)}).deleted_count
        else:
            coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            counts["stat_restored"] += 1

    for pid, doc in snap["subs"].items():
        if doc is None:
            c["server_members"].subs.delete_one({"_id": int(pid)})
        else:
            c["server_members"].subs.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    known = set(snap["validated_ids"])
    removed = c[MATCH_DB].validated_matches.delete_many(
        {"_id": {"$nin": list(known)}}).deleted_count

    pending = snap["pending"]
    c[MATCH_DB].pending_matches.replace_one({"_id": pending["_id"]}, pending, upsert=True)

    print(f"restored: {counts['stat_restored']} stat docs replaced, "
          f"{counts['stat_deleted']} deleted, {removed} validated removed, pending back")


def strip(doc, extra=frozenset()):
    if isinstance(doc, dict):
        return {k: strip(v, extra) for k, v in doc.items()
                if k not in IGNORED_FIELDS and k not in extra}
    if isinstance(doc, list):
        return [strip(v, extra) for v in doc]
    return doc


def cmd_diff(a: str, b: str) -> None:
    ca, cb = read(f"capture-{a}.json"), read(f"capture-{b}.json")
    diffs = []

    for section in ("response", "stats", "subs", "validated"):
        extra = SECTION_IGNORED.get(section, frozenset())
        va, vb = strip(ca[section], extra), strip(cb[section], extra)
        if section in ("stats", "subs"):
            for key in sorted(set(va) | set(vb)):
                if va.get(key) != vb.get(key):
                    diffs.append((f"{section}.{key}", va.get(key), vb.get(key)))
        elif va != vb:
            diffs.append((section, va, vb))

    if not diffs:
        n = len(ca["stats"])
        print(f"IDENTICAL across {n} stat slots, subs, the validated document "
              f"and the response body ({a} vs {b})")
        print(f"  excluded as non-deterministic: {sorted(IGNORED_FIELDS)} "
              f"plus {SECTION_IGNORED}")
        for lbl, cap in ((a, ca), (b, cb)):
            v = cap["validated"][0]
            print(f"    {lbl}: _id={v['_id']} approved_at={v['approved_at']}")
        return

    print(f"{len(diffs)} DIFFERENCE(S)\n")
    for key, va, vb in diffs:
        print(f"  {key}\n    {a}: {json_util.dumps(va)}\n    {b}: {json_util.dumps(vb)}\n")
    sys.exit(1)


COMMANDS = {"snapshot": cmd_snapshot, "run": cmd_run, "restore": cmd_restore, "diff": cmd_diff}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        sys.exit(__doc__)
    COMMANDS[sys.argv[1]](*sys.argv[2:])
