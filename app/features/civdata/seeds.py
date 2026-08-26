from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SEED_DIR = Path(__file__).resolve().parent / "seed"
EDITIONS = ("civ6", "civ7")

# Provenance and the unfilled type placeholder are the authoring record. They
# stay in the file and out of the documents: no consumer reads them, and a
# served field nobody uses still lands in Mite's generated types (D49, D68).
AUTHORING_ONLY = frozenset({"civ_source", "age_pool_source", "type"})


def load_seed(edition: str) -> dict[str, Any]:
    """Read one edition's authored civ-data file. No I/O beyond the file."""
    if edition not in EDITIONS:
        raise ValueError(f"unknown edition: {edition}")
    with (SEED_DIR / f"{edition}.json").open(encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)
    return data


def to_documents(edition: str) -> list[dict[str, Any]]:
    """Flatten one edition's seed into civ_data documents.

    Leaders and civs share the collection and the {edition, token} unique
    index; kind is what tells them apart. The version is stamped on every
    document so a served payload carries the version it was seeded from.
    """
    seed = load_seed(edition)
    version = seed["leader_data_version"]
    docs: list[dict[str, Any]] = []
    for kind, key in (("leader", "leaders"), ("civ", "civs")):
        for row in seed.get(key, []):
            doc = {k: v for k, v in row.items() if k not in AUTHORING_ONLY}
            doc["edition"] = edition
            doc["kind"] = kind
            doc["leader_data_version"] = version
            docs.append(doc)
    return docs


__all__ = ["AUTHORING_ONLY", "EDITIONS", "load_seed", "to_documents"]
