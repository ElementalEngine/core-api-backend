from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from bson import ObjectId

from app.features.matches.errors import MatchServiceError, NotFoundError
from app.features.matches.models import ContestReport, MatchModel, PlayerModel
from app.features.matches.validation import (
    SeatPatch,
    UnsetType,
    validate_players_patch,
)

if TYPE_CHECKING:
    from app.features.matches.service import MatchService

logger = logging.getLogger(__name__)


class EditingService:
    """The pending-document edit loop."""

    def __init__(self, matches: MatchService) -> None:
        self._m = matches

    async def _reload_pending(self, oid: ObjectId) -> dict[str, Any]:
        """Re-fetch a pending match and rename _id -> match_id for the response."""
        updated = await self._m.q.find_pending_by_id(oid)
        if updated is None:
            raise NotFoundError("Match not found")
        updated["match_id"] = str(updated.pop("_id"))
        return updated

    async def delete_pending_match(self, match_id: str) -> dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.take_pending_match(oid, now=datetime.now(UTC))
        if res is None:
            raise await self._m.refusal(oid)
        res["match_id"] = str(res.pop("_id"))
        logger.info("✅ 🔄 Match %s removed", match_id)
        return res

    async def contest_report(
        self,
        match_id: str,
        contestor_discord_id: str,
        reason: str,
    ) -> dict[str, Any]:
        oid = self._m._to_oid(match_id)
        entry = ContestReport(contestor_discord_id=contestor_discord_id, reason=reason)
        pushed = await self._m.q.push_contest(
            oid, entry.model_dump(), now=datetime.now(UTC)
        )
        if not pushed:
            raise await self._m.refusal(oid)
        logger.info("✅ 🔄 Match %s contested by %s", match_id, contestor_discord_id)
        return await self._reload_pending(oid)

    async def patch_players(
        self,
        match_id: str,
        patch: Sequence[SeatPatch],
        *,
        actor_is_staff: bool,
    ) -> dict[str, Any]:
        """One request, one judgement, one recompute."""
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        violations = validate_players_patch(match, patch, actor_is_staff=actor_is_staff)
        if violations:
            raise MatchServiceError(" ".join(v.message for v in violations))

        wanted = sorted(
            {
                value
                for entry in patch
                for value in (entry.discord_id, entry.sub_out)
                if isinstance(value, str)
            }
        )
        steam_ids: dict[str, str | None] = {}
        unregistered: list[str] = []
        # Every player looked up at once; a large assign is one wait, not twelve.
        found = await asyncio.gather(
            *(self._m.seat_steam_id(d) for d in wanted), return_exceptions=True
        )
        for discord_id, got in zip(wanted, found):
            if isinstance(got, MatchServiceError):
                unregistered.append(f"<@{discord_id}>")
            elif isinstance(got, BaseException):
                raise got
            else:
                steam_ids[discord_id] = got
        if unregistered:
            who = ", ".join(unregistered)
            verb = "is" if len(unregistered) == 1 else "are"
            raise MatchServiceError(
                f"{who} {verb} not registered yet, so cannot take a seat. "
                "Pick someone else, or have them register first."
            )

        apply_players_patch(match, patch, steam_ids)
        match = await self._m._recompute_deltas(match)

        players = match.model_dump()["players"]
        if not await self._m.q.set_pending_players(oid, players, now=datetime.now(UTC)):
            raise await self._m.refusal(oid)
        logger.info(
            "PATCH applied to %d seat(s) on match %s",
            len({e.seat for e in patch}),
            match_id,
        )
        return await self._reload_pending(oid)


def apply_players_patch(
    match: MatchModel,
    patch: Sequence[SeatPatch],
    steam_ids: Mapping[str, str | None],
) -> None:
    """Apply a validated patch in place."""
    last: dict[int, SeatPatch] = {}
    for entry in patch:
        last[entry.seat] = entry

    for seat, entry in sorted(last.items()):
        row = match.players[seat]
        if not isinstance(entry.discord_id, UnsetType):
            row.discord_id = entry.discord_id
            row.steam_id = steam_ids.get(entry.discord_id, row.steam_id)
        if not isinstance(entry.quit, UnsetType):
            row.quit = entry.quit
        # The leaver's row is derived from its seat, so it mirrors these.
        leaver = _leaver_index(match, seat)
        for name in ("placement", "leader", "civ"):
            value = getattr(entry, name)
            if not isinstance(value, UnsetType):
                setattr(row, name, value)
                if leaver is not None:
                    setattr(match.players[leaver], name, value)

    # Last seat first: inserting or removing a leaver's row moves only the
    # rows after it, so every seat still to come keeps its index.
    for seat, entry in sorted(last.items(), reverse=True):
        if not isinstance(entry.sub_out, UnsetType):
            _apply_sub(match, seat, entry.sub_out, steam_ids)
        if not isinstance(entry.leaver_quit, UnsetType):
            leaver = _leaver_index(match, seat)
            if leaver is not None:
                match.players[leaver].quit = entry.leaver_quit


def _leaver_index(match: MatchModel, seat: int) -> int | None:
    """The synthetic row paired with a seat, by the adjacency v1 builds."""
    nxt = seat + 1
    if nxt < len(match.players) and match.players[nxt].subbed_out:
        return nxt
    return None


def _apply_sub(
    match: MatchModel,
    seat: int,
    sub_out: str | None,
    steam_ids: Mapping[str, str | None],
) -> None:
    row = match.players[seat]
    leaver = _leaver_index(match, seat)

    # The seat's flag follows the patch, whatever shape the row arrived in.
    row.is_sub = sub_out is not None
    if sub_out is None:
        if leaver is not None:
            match.players.pop(leaver)
        return

    if leaver is not None:
        # A different player left: the quit was the old leaver's, not theirs.
        if match.players[leaver].discord_id != sub_out:
            match.players[leaver].quit = False
        match.players[leaver].discord_id = sub_out
        match.players[leaver].steam_id = steam_ids.get(sub_out)
        return

    match.players.insert(
        seat + 1,
        PlayerModel(
            steam_id=steam_ids.get(sub_out),
            user_name=None,
            civ=row.civ,
            team=row.team,
            leader=row.leader,
            player_alive=row.player_alive,
            discord_id=sub_out,
            placement=row.placement,
            quit=False,
            delta=0.0,
            is_sub=False,
            subbed_out=True,
        ),
    )
