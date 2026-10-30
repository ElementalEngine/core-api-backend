#!/usr/bin/env python3
"""copy_database <source> <target> [--check]

Copies every collection of the source database to the target, listed at run
time, with its options and indexes, then checks both sides hold the same
collections, counts, raw BSON and indexes. The source is only read, and a
target that already holds a collection is refused. --check compares only.

MONGO_URL must name the dev cluster unless MONGO_URL_ALLOW_ANY is set.
"""

from __future__ import annotations

import hashlib
import os
import sys
from typing import Any

from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
from pymongo.database import Database

CHUNK = 1000
RAW = CodecOptions(document_class=RawBSONDocument)


def connect() -> MongoClient:
    uri = os.environ["MONGO_URL"]
    if "MONGO_URL_ALLOW_ANY" not in os.environ and "-dev" not in uri:
        sys.exit("refusing: MONGO_URL does not look like the dev cluster")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def collections(db: Database) -> dict[str, dict[str, Any]]:
    """Each collection's creation options, by name; system collections aside."""
    out: dict[str, dict[str, Any]] = {}
    for info in db.list_collections():
        if info["name"].startswith("system."):
            continue
        if info.get("type", "collection") != "collection":
            sys.exit(f"refusing: {db.name}.{info['name']} is a {info['type']}")
        out[info["name"]] = dict(info.get("options") or {})
    return out


def index_specs(info: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """An index as created: its keys and options, without the server's version."""
    return {
        name: {k: v for k, v in spec.items() if k not in ("v", "ns")}
        for name, spec in info.items()
    }


def digest(db: Database, name: str) -> tuple[int, str]:
    """The document count and a sha256 of every document's bytes in _id order."""
    h, n = hashlib.sha256(), 0
    for doc in db.get_collection(name, codec_options=RAW).find(sort=[("_id", 1)]):
        h.update(doc.raw)
        n += 1
    return n, h.hexdigest()


def copy(src: Database, dst: Database, names: dict[str, dict[str, Any]]) -> None:
    for name, options in sorted(names.items()):
        dst.create_collection(name, **options)
        batch: list[RawBSONDocument] = []
        target = dst.get_collection(name, codec_options=RAW)
        for doc in src.get_collection(name, codec_options=RAW).find(sort=[("_id", 1)]):
            batch.append(doc)
            if len(batch) == CHUNK:
                target.insert_many(batch, ordered=True)
                batch = []
        if batch:
            target.insert_many(batch, ordered=True)
        for index, spec in index_specs(src[name].index_information()).items():
            if index == "_id_":
                continue
            keys = spec.pop("key")
            dst[name].create_index(keys, name=index, **spec)
        print(f"  {name}: copied")


def check(src: Database, dst: Database) -> bool:
    ours, theirs = collections(src), collections(dst)
    ok = set(ours) == set(theirs)
    if not ok:
        print(
            f"  collections differ: only in source {sorted(set(ours) - set(theirs))}, "
            f"only in target {sorted(set(theirs) - set(ours))}"
        )
    for name in sorted(set(ours) & set(theirs)):
        same_docs = digest(src, name) == digest(dst, name)
        same_indexes = index_specs(src[name].index_information()) == index_specs(
            dst[name].index_information()
        )
        same_options = ours[name] == theirs[name]
        count = src[name].count_documents({})
        good = same_docs and same_indexes and same_options
        ok = ok and good
        print(
            f"  {name}: {count} documents, "
            f"{'identical' if good else 'DIFFERENT'} "
            f"(documents {same_docs}, indexes {same_indexes}, options {same_options})"
        )
    return ok


def main(argv: list[str]) -> int:
    args = [a for a in argv if a != "--check"]
    if len(args) != 2 or args[0] == args[1]:
        sys.exit(__doc__)
    client = connect()
    src, dst = client[args[0]], client[args[1]]
    if "--check" not in argv:
        names = collections(src)
        if not names:
            sys.exit(f"refusing: {src.name} holds no collections")
        if collections(dst):
            sys.exit(f"refusing: {dst.name} already holds collections")
        print(f"copying {len(names)} collections, {src.name} -> {dst.name}")
        copy(src, dst, names)
    print(f"checking {src.name} against {dst.name}")
    if not check(src, dst):
        print("CHECK FAILED")
        return 1
    print("CHECK PASSED: identical")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
