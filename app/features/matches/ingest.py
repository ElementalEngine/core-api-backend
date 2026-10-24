from __future__ import annotations

import asyncio
import hashlib
from typing import TYPE_CHECKING, Any

from pymongo.errors import DuplicateKeyError

from app.features.matches.errors import MatchServiceError
from app.features.matches.models import MatchModel
from app.features.matches.validation import is_placeholder

if TYPE_CHECKING:
    from app.features.matches.service import MatchService


def _played_in(match: MatchModel, approved: dict[str, Any]) -> bool:
    """Every registered player in the save played in the approved game."""
    theirs = {str(p.get("discord_id")) for p in approved.get("players") or []}
    ours = {p.discord_id for p in match.players if not is_placeholder(p.discord_id)}
    return ours <= theirs


class IngestService:
    """The upload path: dedup and persist."""

    def __init__(self, matches: MatchService) -> None:
        self._m = matches

    @staticmethod
    def _repeated(doc: dict[str, Any], repeated_by: str) -> dict[str, Any]:
        """One answer shape for every duplicate. `repeated_by` says whether the file
        or the game's composition matched."""
        out = dict(doc)
        out["match_id"] = str(out.pop("_id"))
        out["repeated"] = True
        out["repeated_by"] = repeated_by
        return out

    async def create_from_save(
        self,
        file_bytes: bytes,
        reporter_discord_id: str,
        is_cloud: bool,
        discord_message_id: str,
    ) -> dict[str, Any]:
        save_bytes_sha256 = hashlib.sha256(file_bytes).hexdigest()

        # Both stores at once; a pending copy answers before an approved one.
        pending, validated = await asyncio.gather(
            self._m.q.find_pending_by_bytes(save_bytes_sha256),
            self._m.q.find_validated_by_bytes(save_bytes_sha256),
        )
        existing = pending or validated
        if existing:
            return self._repeated(existing, "file")

        # Parsing is CPU work: off the event loop, so other requests go on.
        parsed = await asyncio.to_thread(self._m._parse_save, file_bytes)
        # Rating needs two sides; one would fail deep in TrueSkill as a 500.
        if len({p["team"] for p in parsed["players"]}) < 2:
            raise MatchServiceError("A report needs at least two players or teams.")

        m = hashlib.sha256()
        unique_data = ",".join(
            [parsed["game"], parsed["map_type"]]
            + [p["civ"] + (p.get("leader") or "") for p in parsed["players"]]
        )
        m.update(unique_data.encode("utf-8"))
        save_file_hash = m.hexdigest()

        existing = await self._m.q.find_pending_by_hash(save_file_hash)
        if existing:
            return self._repeated(existing, "composition")

        parsed["save_file_hash"] = save_file_hash
        parsed["save_bytes_sha256"] = save_bytes_sha256
        parsed["reporter_discord_id"] = reporter_discord_id
        parsed["is_cloud"] = is_cloud
        parsed["discord_messages_id_list"] = [discord_message_id]
        parsed["contest_report_list"] = []

        match = MatchModel(**parsed)
        match = await self._m.match_id_to_discord(match)
        # Another save of a game already rated would rate it twice.
        for approved in await self._m.q.find_validated_by_hash(save_file_hash):
            if _played_in(match, approved):
                return self._repeated(approved, "approved")
        match = await self._m._recompute_deltas(match)

        doc = match.model_dump()
        if not doc.get("save_bytes_sha256"):
            # Never write the field empty -- see models.py.
            doc.pop("save_bytes_sha256", None)
        try:
            inserted_id = await self._m.q.insert_pending_match(doc)
        except DuplicateKeyError:
            # Another upload of this game got in between the checks above.
            winner = await self._m.q.find_pending_by_bytes(save_bytes_sha256)
            if winner is not None:
                return self._repeated(winner, "file")
            winner = await self._m.q.find_pending_by_hash(save_file_hash)
            if winner is None:
                raise
            return self._repeated(winner, "composition")
        return {"match_id": str(inserted_id), "repeated": False, **match.model_dump()}
