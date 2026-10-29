"""Starting the next season of an edition: the season stats saved to files on
the box, then cleared, and the next season opened, all in one transaction."""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from bson import json_util
from bson.json_util import CANONICAL_JSON_OPTIONS
from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

from app.core.coerce import as_float
from app.core.config import settings
from app.core.constants import COL_RATING_EVENTS, COL_SEASONS, GAMES_DB
from app.features.matches.repository import COL_PENDING_MATCHES
from app.features.ratings.events import build_reset_event
from app.features.ratings.scope import stats_collection_name, stats_db_name
from app.features.seasons.repository import (
    SeasonNotSeededError,
    SeasonsRepository,
    clear_cache,
)
from app.features.seasons.schemas import SeasonPreview, SeasonRolled

_NAMED = re.compile(r"^Season (\d+)$")


class SeasonRolloverError(Exception):
    """The open season's name has no number to count on from."""


class SeasonMovedError(Exception):
    """The season the caller saw is no longer the open one."""


class SeasonBackupUnsetError(Exception):
    """No folder is configured for the backup, so nothing may be cleared."""


def next_label(label: str) -> str:
    """Season 6 becomes Season 7."""
    found = _NAMED.match(label.strip())
    if not found:
        raise SeasonRolloverError(
            f"{label} is not named Season and a number, so its next name is unknown."
        )
    return f"Season {int(found.group(1)) + 1}"


def _write_file(path: Path, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())


# One document per line in canonical Extended JSON, as mongoimport and Compass
# read it, so a collection is restored from its file as it is.
def _write_lines(path: Path, docs: list[dict[str, Any]]) -> None:
    _write_file(
        path,
        "".join(
            json_util.dumps(doc, json_options=CANONICAL_JSON_OPTIONS) + "\n"
            for doc in docs
        ),
    )


# Every collection an approve can write, so one it creates during the change
# is cleared with the rest.
_KNOWN = frozenset(
    stats_collection_name(match_type=mode, is_cloud=cloud, is_combined=combined)
    for mode in ("ffa", "teamer", "duel")
    for cloud in (False, True)
    for combined in (False, True)
)


def _sync_folder(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _reset_events(
    docs: list[dict[str, Any]], scope: str, at: datetime
) -> list[dict[str, Any]]:
    """Each cleared player's move back to the start, so the ledger still sums
    to their stats."""
    return [
        build_reset_event(
            occurred_at=at,
            discord_id=str(doc["_id"]),
            scope=scope,
            mu_before=as_float(doc.get("mu"), settings.ts_mu),
            mu_after=settings.ts_mu,
            sigma_before=as_float(doc.get("sigma"), settings.ts_sigma),
            sigma_after=settings.ts_sigma,
        )
        for doc in docs
        if isinstance(doc.get("_id"), int) and doc["_id"] > 0
    ]


class SeasonRollover:
    def __init__(self, client: AsyncMongoClient, backup_root: str = "") -> None:
        self._client = client
        self._seasons = SeasonsRepository(client)
        self._season_rows = client[GAMES_DB][COL_SEASONS]
        self._events = client[GAMES_DB][COL_RATING_EVENTS]
        self._pending = client[GAMES_DB][COL_PENDING_MATCHES]
        self._root = Path(backup_root) if backup_root else None

    async def preview(self, edition: str) -> SeasonPreview:
        season = await self._seasons.read_open(edition)
        if season is None:
            raise SeasonNotSeededError(f"No season is recorded for {edition}.")
        db = self._client[stats_db_name(civ_version=edition, is_seasonal=True)]
        names = sorted(await db.list_collection_names())
        return SeasonPreview(
            edition=edition,
            label=season["label"],
            started_at=season["started_at"],
            next_label=next_label(season["label"]),
            records={name: await db[name].count_documents({}) for name in names},
        )

    async def roll(
        self, edition: str, *, expected: str, now: datetime | None = None
    ) -> SeasonRolled:
        """Ends `expected`, which must still be the open season: a repeated
        press, or a second person's, changes nothing twice."""
        if self._root is None:
            raise SeasonBackupUnsetError("SEASON_BACKUP_DIR is not set.")
        db_name = stats_db_name(civ_version=edition, is_seasonal=True)
        db = self._client[db_name]
        listed = set(await db.list_collection_names())
        names = sorted(listed | _KNOWN)
        records: dict[str, int] = {}
        opened = ""
        session = self._client.start_session()
        try:
            async with session:
                async with await session.start_transaction():
                    season = await self._seasons.read_open(edition, session=session)
                    if season is None:
                        raise SeasonNotSeededError(
                            f"No season is recorded for {edition}."
                        )
                    if season["label"] != expected:
                        raise SeasonMovedError(
                            f"{expected} is no longer open: {edition} is in {season['label']}."
                        )
                    opened = next_label(season["label"])
                    # Taken after the transaction's first read, so every approve
                    # this change can see sorts before its reset events.
                    at = now or datetime.now(UTC)
                    slug = season["label"].lower().replace(" ", "-")
                    name = f"{edition}-{slug}-{at:%Y%m%dT%H%M%SZ}-{uuid4().hex[:6]}"
                    partial = self._root / f"{name}.partial"
                    partial.mkdir(parents=True, mode=0o750)
                    events: list[dict[str, Any]] = []
                    for col in names:
                        docs = [d async for d in db[col].find({}, session=session)]
                        if not docs and col not in listed:
                            continue
                        _write_lines(partial / f"{db_name}.{col}.json", docs)
                        records[col] = len(docs)
                        events += _reset_events(docs, f"{db_name}.{col}", at)
                        await db[col].delete_many({}, session=session)
                    _sync_folder(partial)
                    if events:
                        await self._events.insert_many(events, session=session)
                    # An approve writes its pending report in its own transaction,
                    # so touching them makes one that overlaps this change clash.
                    await self._pending.update_many(
                        {"game": edition},
                        {"$set": {"season_checked_at": at}},
                        session=session,
                    )
                    await self._season_rows.insert_one(
                        {"edition": edition, "label": opened, "started_at": at},
                        session=session,
                    )
                    await session.commit_transaction()
        except DuplicateKeyError as exc:
            raise SeasonMovedError(f"{opened} has already started.") from exc
        finally:
            # Cleared whatever happened: a stale open season is never served.
            clear_cache()
        manifest = {
            "edition": edition,
            "closed": season["label"],
            "opened": opened,
            "started_at": at.isoformat(),
            "records": records,
        }
        _write_file(partial / "manifest.json", json.dumps(manifest, indent=2) + "\n")
        # A folder still named .partial is a change that may not have finished.
        folder = partial.with_name(name)
        partial.rename(folder)
        _sync_folder(folder.parent)
        return SeasonRolled(
            edition=edition,
            closed=season["label"],
            opened=opened,
            started_at=at,
            backup=folder.name,
            records=records,
        )


__all__ = [
    "SeasonBackupUnsetError",
    "SeasonMovedError",
    "SeasonRollover",
    "SeasonRolloverError",
    "next_label",
]
