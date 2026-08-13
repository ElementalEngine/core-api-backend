#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from bson import json_util
from pymongo import MongoClient

OUT = Path(os.environ.get("DIFF_DIR", "/home/cisco/s2-baseline"))
UID = os.environ.get("C10_TEST_ID", "999999999999999901")
ROLES = ["111111111111111111", "222222222222222222"]
INFRA = "/api/v1/infractions"
AUTH = "/api/v1/auth"


def connect() -> MongoClient:
    uri = os.environ["MONGO_URL"]
    if "MONGO_URL_ALLOW_ANY" not in os.environ and "-dev" not in uri:
        sys.exit("refusing: MONGO_URL does not look like the dev cluster")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def call(
    base: str, method: str, path: str, token: str, body: dict | None = None
) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if data:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        base.rstrip("/") + path, data=data, method=method, headers=headers
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode()
            status = r.status
    except urllib.error.HTTPError as e:
        raw, status = e.read().decode(), e.code
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        parsed = raw
    return {"method": method, "path": path, "status": status, "body": parsed}


def sequence(c: MongoClient, base: str, lj: str, auth: str) -> tuple[list[dict], dict]:
    """Every infractions route, in lifecycle order, plus the reachable auth reads."""
    steps: list[dict] = []
    add = steps.append
    disk: dict = {}

    add(call(base, "GET", f"{INFRA}/{UID}", lj))  # create on miss
    add(
        call(
            base,
            "POST",
            f"{INFRA}/{UID}/flat/smurf",
            lj,
            {"reason": "c10 differential", "suspended_roles": ROLES},
        )
    )
    add(call(base, "GET", f"{INFRA}/{UID}", lj))  # roles on the wire
    disk.update(on_disk(c))  # read while suspended; unsuspend clears the field later
    add(call(base, "GET", f"{INFRA}/active", lj))
    add(call(base, "GET", f"{INFRA}/overdue", lj))
    add(
        call(
            base,
            "POST",
            f"{INFRA}/{UID}/tier/minor",
            lj,
            {"reason": "c10", "suspended_roles": ROLES},
        )
    )
    add(
        call(
            base,
            "POST",
            f"{INFRA}/{UID}/tier/moderate",
            lj,
            {"reason": "c10", "suspended_roles": ROLES},
        )
    )
    add(call(base, "GET", f"{INFRA}/{UID}", lj))
    add(call(base, "POST", f"{INFRA}/{UID}/add-days", lj, {"days": 3}))
    add(call(base, "POST", f"{INFRA}/{UID}/remove-days", lj, {"days": 1}))
    add(call(base, "POST", f"{INFRA}/{UID}/remove-tier", lj, {"category": "minor"}))
    add(
        call(
            base,
            "POST",
            f"{INFRA}/{UID}/pending",
            lj,
            {"punishment_type": "smurf", "reason": "c10"},
        )
    )
    add(call(base, "GET", f"{INFRA}/{UID}/pending", lj))
    add(call(base, "DELETE", f"{INFRA}/{UID}/pending", lj))
    add(call(base, "POST", f"{INFRA}/{UID}/unsuspend", lj))
    add(call(base, "GET", f"{INFRA}/{UID}", lj))

    # auth: the four reachable read paths, on an id that does not exist
    add(call(base, "GET", f"{AUTH}/admin/accounts/discord/{UID}", auth))
    add(call(base, "GET", f"{AUTH}/admin/accounts/linked-account/{UID}", auth))
    add(call(base, "GET", f"{AUTH}/admin/accounts/steam/{UID}", auth))
    add(call(base, "GET", f"{AUTH}/registration-sessions/nonexistent-session", auth))

    # both gates reject a bad token identically
    add(call(base, "GET", f"{INFRA}/active", "not-a-real-token"))
    add(call(base, "GET", f"{AUTH}/admin/accounts/discord/{UID}", "not-a-real-token"))
    return steps, disk


def on_disk(c: MongoClient) -> dict:
    """Criterion 3's disk side: the stored key must literally be suspendedRoles."""
    doc = c["server_members"].suspensions.find_one({"discord_id": UID})
    return {
        "present": doc is not None,
        "keys": sorted(doc.keys()) if doc else [],
        "camel_key_present": bool(doc) and "suspendedRoles" in doc,
        "snake_key_absent": bool(doc) and "suspended_roles" not in doc,
        "value": doc.get("suspendedRoles") if doc else None,
    }


def cmd_run(label: str, base_url: str) -> None:
    c = connect()
    steps, disk = sequence(
        c, base_url, os.environ["LJ_SERVICE_TOKEN"], os.environ["AUTH_SERVICE_TOKEN"]
    )
    payload = {"steps": steps, "disk_after_flat": disk}
    OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
    p = OUT / f"capture-c10-{label}.json"
    p.write_text(json_util.dumps(payload, indent=2))
    p.chmod(0o600)
    codes = [s["status"] for s in steps]
    print(f"{label}: {len(steps)} calls -> {codes}")
    print(f"  wrote {p}")


def cmd_restore() -> None:
    c = connect()
    a = c["server_members"].suspensions.delete_many({"discord_id": UID}).deleted_count
    b = c["server_members"].suspensions_due.delete_many({"_id": UID}).deleted_count
    print(f"restored: {a} suspension(s), {b} pending removed for {UID}")


TIME_KEYS = {"ends", "new_ends", "decays", "new_decays", "created_at", "updated_at"}


def normalise(v, path=""):
    """Datetime values differ by design; keep presence and nullness, drop the value."""
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            if k in TIME_KEYS:
                out[k] = None if x is None else "<datetime>"
            else:
                out[k] = normalise(x, f"{path}.{k}")
        return out
    if isinstance(v, list):
        return [normalise(x, path) for x in v]
    if isinstance(v, datetime):
        return "<datetime>"
    return v


def scope_lists(step: dict) -> dict:
    """/active and /overdue return the whole collection; only our fixture is stable."""
    if step["path"].endswith(("/active", "/overdue")) and isinstance(
        step["body"], list
    ):
        mine = [r for r in step["body"] if r.get("discord_id") == UID]
        return {**step, "body": mine, "_total_rows": len(step["body"])}
    return step


def cmd_diff(a: str, b: str) -> None:
    ca = json_util.loads((OUT / f"capture-c10-{a}.json").read_text())
    cb = json_util.loads((OUT / f"capture-c10-{b}.json").read_text())
    diffs = []

    for i, (sa, sb) in enumerate(zip(ca["steps"], cb["steps"])):
        na, nb = normalise(scope_lists(sa)), normalise(scope_lists(sb))
        na.pop("_total_rows", None)
        nb.pop("_total_rows", None)
        if na != nb:
            diffs.append((f"step {i} {sa['method']} {sa['path']}", na, nb))

    if ca["disk_after_flat"] != cb["disk_after_flat"]:
        diffs.append(("disk_after_flat", ca["disk_after_flat"], cb["disk_after_flat"]))

    d = ca["disk_after_flat"]
    print("criterion 3 - suspendedRoles round trip")
    print(
        f"  on disk:  camelCase present={d['camel_key_present']} "
        f"snake_case absent={d['snake_key_absent']} value={d['value']}"
    )
    wire = next(
        (
            s["body"]
            for s in ca["steps"]
            if s["method"] == "GET"
            and s["path"] == f"{INFRA}/{UID}"
            and isinstance(s["body"], dict)
            and s["body"].get("suspended_roles")
        ),
        None,
    )
    print(f"  on wire:  suspended_roles={wire}\n")

    if not diffs:
        print(
            f"IDENTICAL across {len(ca['steps'])} calls and the stored document "
            f"({a} vs {b}; datetime values normalised)"
        )
        print(f"  status codes: {[s['status'] for s in ca['steps']]}")
        return

    print(f"{len(diffs)} DIFFERENCE(S)\n")
    for key, va, vb in diffs:
        print(
            f"  {key}\n    {a}: {json_util.dumps(va)}\n    {b}: {json_util.dumps(vb)}\n"
        )
    sys.exit(1)


COMMANDS = {"run": cmd_run, "restore": cmd_restore, "diff": cmd_diff}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        sys.exit(__doc__)
    COMMANDS[sys.argv[1]](*sys.argv[2:])
