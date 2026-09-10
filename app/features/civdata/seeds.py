from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SEED_DIR = Path(__file__).resolve().parent / "seed"
EDITIONS = ("civ6", "civ7")

AUTHORING_ONLY = frozenset({"civ_source", "age_pool_source", "type"})


def load_seed(edition: str) -> dict[str, Any]:
    """Read one edition's authored civ-data file. No I/O beyond the file."""
    if edition not in EDITIONS:
        raise ValueError(f"unknown edition: {edition}")
    with (SEED_DIR / f"{edition}.json").open(encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)
    return data


def to_documents(edition: str) -> list[dict[str, Any]]:
    """Flatten one edition's seed into civ_data documents."""
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
