from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from pymongo.errors import DuplicateKeyError

from app.features.matches.models import MatchModel

if TYPE_CHECKING:
    from app.features.matches.service import MatchService


class IngestService:
    """The upload path: dedup and persist (D114, carved in S6).

    Parsing and identity resolution stay on MatchService — test_dedup
    monkeypatches them on the service instance — and are borrowed
    through self._m.
    """

    def __init__(self, matches: MatchService) -> None:
        self._m = matches

    @staticmethod
    def _repeated(doc: dict[str, Any], repeated_by: str) -> dict[str, Any]:
        """One shape for all three duplicate paths. repeated_by discriminates
        file from composition (D101); Mite reads it in S9."""
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
        # Byte hash first: an exact re-upload is answered without reparsing,
        # including one that would fail to parse. D83, Entry 12.
        save_bytes_sha256 = hashlib.sha256(file_bytes).hexdigest()

        for finder in (
            self._m.q.find_pending_by_bytes,
            self._m.q.find_validated_by_bytes,
        ):
            existing = await finder(save_bytes_sha256)
            if existing:
                return self._repeated(existing, "file")

        parsed = self._m._parse_save(file_bytes)

        # Composition hash: same map and lineup, a different file. Kept under
        # its legacy name -- 35,941 documents carry it and it is not renamed
        # in Wave 1 (D133).
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
        match = await self._m._recompute_deltas(match)

        doc = match.dict()
        if not doc.get("save_bytes_sha256"):
            # Never write the field empty -- see models.py.
            doc.pop("save_bytes_sha256", None)
        try:
            inserted_id = await self._m.q.insert_pending_match(doc)
        except DuplicateKeyError:
            # Two concurrent uploads of the same bytes both passed the lookup.
            # The index turns that into E11000; re-query and answer with the
            # winner rather than a 500. Entry 12 check 9.
            winner = await self._m.q.find_pending_by_bytes(save_bytes_sha256)
            if winner is None:
                raise
            return self._repeated(winner, "file")
        # Explicit: MatchModel has no `repeated` field, so parsed["repeated"]
        # is dropped by the model and the success response has never carried
        # it. Mite's `res?.repeated === true` reads undefined as false, which
        # is why it never surfaced. F28.
        return {"match_id": str(inserted_id), "repeated": False, **match.dict()}
