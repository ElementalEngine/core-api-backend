"""differential_gate.py <fingerprint | fpdiff A B | rating | sample OUT | v1 TREE PORT>

The pieces scripts/differential_gate.sh runs to compare v1, production's own
code, against v2 on approve:

  fingerprint   every dev collection's indexes, plus the count and content
                hash of everything the differential or v1 can write (stdout)
  fpdiff A B    the collections that differ between two fingerprints
  rating        production's rating settings (stdin) against v2's
  sample OUT    a read-only sample of production's validated matches covering
                what dev has never played; the URL arrives on stdin
  v1 TREE PORT  exec v1 from TREE on 127.0.0.1:PORT against the dev cluster,
                with v2's rating settings, so the comparison is of code alone

Secrets arrive on stdin or from the dev .env and are never printed.

Governed by D246.
"""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bson import json_util
from bson.codec_options import CodecOptions
from bson.json_util import CANONICAL_JSON_OPTIONS
from bson.raw_bson import RawBSONDocument
from pymongo import DESCENDING, MongoClient

from app.core.config import settings

ROOT = Path(__file__).resolve().parent.parent
WRITTEN_DBS = (
    "match_reporter",
    "civ6_lifetime_stats",
    "civ6_season_stats",
    "civ7_lifetime_stats",
    "civ7_season_stats",
)
WRITTEN = {
    "games.pending_matches",
    "games.validated_matches",
    "games.rating_events",
    "games.sub_events",
    "server_members.subs",
}
RATING_KEYS = (
    "TS_MU",
    "TS_SIGMA",
    "TS_BETA",
    "TS_TAU",
    "TS_DRAW_PROB",
    "MIN_POINTS_FOR_SUBS",
)
PER = 3
SAMPLE = {
    "civ7, every mode": ({"game": "civ7"}, 10),
    "civ6 realtime teamer": (
        {"game": "civ6", "is_cloud": False, "game_mode": {"$in": ["teamer", "team"]}},
        PER,
    ),
    "civ6 realtime duel": (
        {"game": "civ6", "is_cloud": False, "game_mode": "duel"},
        PER,
    ),
    "civ6 cloud ffa": ({"game": "civ6", "is_cloud": True, "game_mode": "ffa"}, PER),
    "civ6 cloud duel": ({"game": "civ6", "is_cloud": True, "game_mode": "duel"}, PER),
    "a sub": ({"players.is_sub": True}, PER),
    "a subbed-out player": ({"players.subbed_out": True}, PER),
    "a quit": ({"players.quit": True}, PER),
    "civ6 realtime ffa": ({"game": "civ6", "is_cloud": False, "game_mode": "ffa"}, 2),
}


def stdin_env() -> dict[str, str]:
    pairs = (line.rstrip("\n").split("=", 1) for line in sys.stdin if "=" in line)
    return {k.strip(): v.strip().strip('"') for k, v in pairs}


def rating_settings() -> dict[str, float]:
    return {
        "TS_MU": settings.ts_mu,
        "TS_SIGMA": settings.ts_sigma,
        "TS_BETA": settings.ts_beta,
        "TS_TAU": settings.ts_tau,
        "TS_DRAW_PROB": settings.ts_draw_prob,
        "MIN_POINTS_FOR_SUBS": settings.min_points_for_subs,
    }


def fingerprint() -> int:
    raw = CodecOptions(document_class=RawBSONDocument)
    c: MongoClient = MongoClient(settings.mongo_url.get_secret_value())
    out = {}
    try:
        for db in sorted(set(c.list_database_names()) - {"admin", "local", "config"}):
            for name in sorted(c[db].list_collection_names()):
                col = c[db][name]
                indexes = sorted(
                    (dict(i) for i in col.list_indexes()), key=lambda i: i["name"]
                )
                row: dict = {"indexes": json_util.dumps(indexes)}
                if db in WRITTEN_DBS or f"{db}.{name}" in WRITTEN:
                    row["count"] = col.count_documents({})
                    digest = hashlib.sha256()
                    for doc in col.with_options(codec_options=raw).find().sort("_id"):
                        digest.update(doc.raw)
                    row["sha256"] = digest.hexdigest()
                out[f"{db}.{name}"] = row
    finally:
        c.close()
    print(json_util.dumps(out, sort_keys=True))
    return 0


def fpdiff(before: str, after: str) -> int:
    a = json_util.loads(Path(before).read_text(encoding="utf-8"))
    b = json_util.loads(Path(after).read_text(encoding="utf-8"))
    for key in sorted(set(a) | set(b)):
        if a.get(key) != b.get(key):
            print(" ", key, "before" if key in a else "-", "after" if key in b else "-")
    return 0


def rating() -> int:
    prod = stdin_env()
    v2 = rating_settings()
    bad = [k for k in RATING_KEYS if float(prod.get(k, "nan")) != float(v2[k])]
    for k in RATING_KEYS:
        print(f"  {k:<20} prod {prod.get(k, 'MISSING'):<8} v2 {v2[k]}")
    if bad:
        print(f"STOP: rating settings differ: {bad}")
        return 1
    return 0


def sample(out: str) -> int:
    env = stdin_env()
    url = env.get("MONGO_URL") or env.get("MONGO_URI") or ""
    if not url:
        print("STOP: no MONGO_URL on stdin")
        return 1
    m: MongoClient = MongoClient(
        url, serverSelectionTimeoutMS=15000, readPreference="secondaryPreferred"
    )
    picked: dict = {}
    try:
        col = m["match_reporter"]["validated_matches"]
        for label, (query, n) in SAMPLE.items():
            found = {**query, "stat_reset": {"$exists": False}}
            docs = list(col.find(found).sort("_id", DESCENDING).limit(n))
            for doc in docs:
                picked.setdefault(doc["_id"], doc)
            print(f"  {label:<22} {len(docs)}")
    finally:
        m.close()
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for doc in picked.values():
            f.write(json_util.dumps(doc, json_options=CANONICAL_JSON_OPTIONS) + "\n")
    print(f"{len(picked)} distinct matches")
    return 0


def v1(tree: str, port: str) -> int:
    base = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            base[k.strip()] = v.strip().strip('"')
    env = {
        **base,
        "PATH": os.environ["PATH"],
        "MONGO_URL": settings.mongo_url.get_secret_value(),
        "API_HOST": "127.0.0.1",
        "API_PORT": port,
        "ALLOWED_ORIGINS": "",
        "MITO_SERVICE_TOKEN": settings.mito_service_token.get_secret_value(),
        **{k: str(v) for k, v in rating_settings().items()},
    }
    python = str(Path(tree) / ".venv" / "bin" / "python")
    os.chdir(tree)
    os.execve(python, [python, "-m", "app"], env)


def main(argv: list[str]) -> int:
    match argv:
        case ["fingerprint"]:
            return fingerprint()
        case ["fpdiff", before, after]:
            return fpdiff(before, after)
        case ["rating"]:
            return rating()
        case ["sample", out]:
            return sample(out)
        case ["v1", tree, port]:
            return v1(tree, port)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
