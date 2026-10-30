"""Reports approved without staff: complete, uncontested, with no sub or quit,
48 hours after their upload."""

from __future__ import annotations

import logging
from collections.abc import Collection
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from bson import ObjectId

from app.features.matches.errors import MatchServiceError, NotFoundError
from app.features.matches.validation import is_placeholder
from app.features.seasons.repository import SeasonNotSeededError

if TYPE_CHECKING:
    from app.features.civdata.repository import CivDataRepository
    from app.features.matches.service import MatchService

logger = logging.getLogger(__name__)

AUTO_APPROVE_AFTER = timedelta(hours=48)
# Few per call, so the answer beats the bot's timeout; the rest wait an hour.
BATCH = 5
# An approval whose answer was lost is answered again for a week.
UNPUBLISHED_FOR = timedelta(days=7)


def is_complete(
    match: dict[str, Any], leaders: Collection[str], civs: Collection[str]
) -> bool:
    """Nothing a staff member would need to look at: every seat a registered
    player with a known pick, nobody subbed or quit, no contest."""
    players = match.get("players") or []
    if not players or match.get("contest_report_list"):
        return False
    civ7 = match.get("game") == "civ7"
    for p in players:
        if is_placeholder(p.get("discord_id")):
            return False
        if p.get("quit") or p.get("is_sub") or p.get("subbed_out"):
            return False
        if civ7 and (p.get("leader") not in leaders or p.get("civ") not in civs):
            return False
        if not civ7 and (p.get("leader") or p.get("civ")) not in leaders:
            return False
    return True


async def auto_approve(
    svc: MatchService, civ_data: CivDataRepository, approver: str, now: datetime
) -> list[dict[str, Any]]:
    """Its own approvals still unpublished, answered again, then each report due,
    approved as `approver` only while it is as checked; one that cannot be waits."""
    answers = [
        await _answer(svc, doc, approver)
        for doc in await svc.q.find_unpublished_by(
            approver, since=now - UNPUBLISHED_FOR, limit=BATCH
        )
    ]
    cutoff = ObjectId.from_datetime(now - AUTO_APPROVE_AFTER)
    known: dict[str, tuple[set[str], set[str]]] = {}
    approved = 0
    async for doc in svc.q.pending_uploaded_before(cutoff):
        if approved == BATCH:
            break
        edition = str(doc.get("game"))
        if edition not in known:
            data = await civ_data.fetch(edition)
            known[edition] = (
                {row["token"] for row in data.get("leaders") or []},
                {row["token"] for row in data.get("civs") or []},
            )
        if not is_complete(doc, *known[edition]):
            continue
        expect = {"players": doc["players"], "contest_report_list": []}
        answer = await _answer(svc, doc, approver, expect)
        if answer is not None:
            answers.append(answer)
            approved += 1
    return [a for a in answers if a is not None]


async def _answer(
    svc: MatchService,
    doc: dict[str, Any],
    approver: str,
    expect: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    try:
        return await svc.approve_match(str(doc["_id"]), approver, expect=expect)
    except (MatchServiceError, NotFoundError, SeasonNotSeededError) as exc:
        logger.warning("Match %s not auto-approved: %s", doc["_id"], exc)
    except Exception:
        logger.exception("Match %s not auto-approved", doc["_id"])
    return None


__all__ = ["AUTO_APPROVE_AFTER", "auto_approve", "is_complete"]
