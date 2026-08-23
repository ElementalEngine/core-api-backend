from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Sequence, TYPE_CHECKING

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


def _require_int(value: Any, field_name: str) -> int:
    """Parse a client-supplied numeric field; bad input becomes a 400 instead of a bare 500."""
    try:
        return int(str(value).strip())
    except TypeError, ValueError:
        raise MatchServiceError(f"{field_name} must be a whole number, got {value!r}")


class EditingService:
    """The pending-document edit loop (D114, carved in S6).

    Reads, id resolution and the recompute stay on MatchService and are
    borrowed through self._m; this owns every mutation of a pending match
    short of approval. C1's declarative PATCH lands here.
    """

    def __init__(self, matches: MatchService) -> None:
        self._m = matches

    def _player_delta_changes(self, match: MatchModel) -> Dict[str, Any]:
        """$set keys for every player's recomputed deltas."""
        changes: Dict[str, Any] = {}
        for i, player in enumerate(match.players):
            changes[f"players.{i}.delta"] = player.delta
            changes[f"players.{i}.season_delta"] = player.season_delta
            changes[f"players.{i}.combined_delta"] = player.combined_delta
        return changes

    async def _reload_pending(self, oid: ObjectId) -> Dict[str, Any]:
        """Re-fetch a pending match and rename _id -> match_id for the response."""
        updated = await self._m.q.find_pending_by_id(oid)
        updated["match_id"] = str(updated.pop("_id"))
        return updated

    async def append_discord_message_id_list(
        self, match_id: str, discord_message_id_list: list[str]
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        current_list = res.get("discord_messages_id_list", [])
        updated_list = current_list + discord_message_id_list

        await self._m.q.update_pending_match_set(
            oid, {"discord_messages_id_list": updated_list}
        )
        return await self._reload_pending(oid)

    async def update_match(
        self, match_id: str, update_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not update_data:
            raise MatchServiceError("Empty update payload")
        oid = self._m._to_oid(match_id)

        existing = await self._m.q.find_pending_by_id(oid)
        if not existing:
            raise NotFoundError("Match not found")

        await self._m.q.update_pending_match_set(oid, update_data)
        logger.info("✅ 🔄 Updated match %s", match_id)
        return await self._reload_pending(oid)

    async def set_player_order(
        self, match_id: str, player_order: str, discord_message_id: str
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        if "team" in match.game_mode.lower():
            raise MatchServiceError(
                "Cannot set player order for teamer matches. Use change_order instead."
            )
        player_order_list = player_order.split(" ")
        curr_placement = 0
        placement = {}
        for i in range(len(player_order_list)):
            if player_order_list[i].upper() == "TIE":
                curr_placement = max(curr_placement - 1, 0)
            else:
                placement[player_order_list[i]] = curr_placement
                curr_placement += 1

        for i, player in enumerate(match.players):
            if player.subbed_out == False and player.discord_id not in placement:
                raise MatchServiceError(
                    f"Discord ID {player.discord_id} not found in player order list"
                )
            if player.subbed_out:
                player.placement = match.players[i - 1].placement
            else:
                player.placement = placement[player.discord_id]

        match = await self._m._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [
            discord_message_id
        ]
        changes.update(self._player_delta_changes(match))
        for i, player in enumerate(match.players):
            changes[f"players.{i}.placement"] = player.placement

        await self._m.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Changed player order for match %s", match_id)
        return await self._reload_pending(oid)

    async def change_order(
        self, match_id: str, new_order: str, discord_message_id: str
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        num_teams = len({player.team for player in match.players})
        new_order_list = new_order.split(" ")
        if len(new_order_list) != num_teams:
            raise MatchServiceError(
                f"New order length does not match number of players/teams ({num_teams})"
            )

        for player in match.players:
            if player.team < 0 or player.team >= len(new_order_list):
                raise MatchServiceError(
                    f"Team {player.team} has no entry in the new order list"
                )
            player.placement = (
                _require_int(new_order_list[player.team], "new_order") - 1
            )

        match = await self._m._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [
            discord_message_id
        ]
        changes.update(self._player_delta_changes(match))
        for i, player in enumerate(match.players):
            changes[f"players.{i}.placement"] = player.placement

        await self._m.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Changed player order for match %s", match_id)
        return await self._reload_pending(oid)

    async def delete_pending_match(self, match_id: str) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")
        res["match_id"] = str(res.pop("_id"))
        await self._m.q.delete_pending_match(oid)
        logger.info("✅ 🔄 Match %s removed", match_id)
        return res

    async def trigger_quit(
        self, match_id: str, quitter_discord_id: str, discord_message_id: str
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        changes: Dict[str, Any] = {}
        quitter_found = False
        for i, player in enumerate(res["players"]):
            if player.get("discord_id") == quitter_discord_id:
                changes[f"players.{i}.quit"] = not bool(res["players"][i]["quit"])
                quitter_found = True
                break
        if not quitter_found:
            raise MatchServiceError("Quitter discord_id not found in match players")

        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [
            discord_message_id
        ]
        await self._m.q.update_pending_match_set(oid, changes)
        logger.info(
            "✅ 🔄 Match %s, player %s quit toggled", match_id, quitter_discord_id
        )
        return await self._reload_pending(oid)

    async def assign_discord_id(
        self,
        match_id: str,
        player_id: str,
        player_discord_id: str,
        discord_message_id: str,
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        idx = _require_int(player_id, "player_id") - 1
        if idx < 0 or idx >= len(match.players):
            raise MatchServiceError(
                "Player ID out of range. Must be between 1 and number of players"
            )

        match.players[idx].discord_id = player_discord_id
        match.players[idx].steam_id = await self._m.discord_to_steam_id(
            player_discord_id
        )

        match = await self._m._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [
            discord_message_id
        ]
        changes[f"players.{idx}.discord_id"] = player_discord_id
        changes[f"players.{idx}.steam_id"] = match.players[idx].steam_id
        changes.update(self._player_delta_changes(match))

        await self._m.q.update_pending_match_set(oid, changes)
        logger.info(
            "✅ 🔄 Assigned discord_id for match %s (player %s)", match_id, player_id
        )
        return await self._reload_pending(oid)

    async def assign_discord_id_all(
        self, match_id: str, player_discord_id: list[str], discord_message_id: str
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        if len(player_discord_id) != len(match.players):
            raise MatchServiceError("Must provide one discord_id per player")

        # Build mapping (fixed bug: always use the *same index* when writing steam_id)
        for i, did in enumerate(player_discord_id):
            match.players[i].discord_id = did
            match.players[i].steam_id = await self._m.discord_to_steam_id(did)

        match = await self._m._recompute_deltas(match)

        changes: Dict[str, Any] = {}
        changes["discord_messages_id_list"] = res["discord_messages_id_list"] + [
            discord_message_id
        ]
        changes.update(self._player_delta_changes(match))
        for i, p in enumerate(match.players):
            changes[f"players.{i}.discord_id"] = p.discord_id
            changes[f"players.{i}.steam_id"] = p.steam_id

        await self._m.q.update_pending_match_set(oid, changes)
        logger.info("✅ 🔄 Assigned all discord_ids for match %s", match_id)
        return await self._reload_pending(oid)

    async def assign_sub(
        self,
        match_id: str,
        sub_in_id: str,
        sub_out_discord_id: str,
        discord_message_id: str,
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        sub_in_idx = _require_int(sub_in_id, "sub_in_id")
        if sub_in_idx < 0 or sub_in_idx >= len(match.players):
            raise MatchServiceError("Sub-in slot invalid or already claimed")

        match.players[sub_in_idx].is_sub = True
        sub_out_player_steam_id = await self._m.discord_to_steam_id(sub_out_discord_id)
        match.players.insert(
            sub_in_idx + 1,
            PlayerModel(
                steam_id=sub_out_player_steam_id,
                user_name=None,
                civ=match.players[sub_in_idx].civ,
                team=match.players[sub_in_idx].team,
                leader=match.players[sub_in_idx].leader,
                player_alive=match.players[sub_in_idx].player_alive,
                discord_id=sub_out_discord_id,
                placement=match.players[sub_in_idx].placement,
                quit=False,
                delta=0.0,
                is_sub=False,
                subbed_out=True,
            ),
        )
        match = await self._m._recompute_deltas(match)

        match.discord_messages_id_list = match.discord_messages_id_list + [
            discord_message_id
        ]

        await self._m.q.replace_pending_match(oid, match.dict())
        logger.info("✅ 🔄 Sub assigned for match %s", match_id)
        return await self._reload_pending(oid)

    async def remove_sub(
        self, match_id: str, sub_out_id: str, discord_message_id: str
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        idx = _require_int(sub_out_id, "sub_out_id")
        if idx < 1 or idx >= len(match.players):
            raise MatchServiceError(
                "Player ID out of range. Must be between 2 and number of players"
            )
        if not match.players[idx].subbed_out:
            raise MatchServiceError("That player is not marked as a sub")

        # Unmark the sub-out player
        match.players[idx - 1].is_sub = False

        # Remove the sub slot correctly (fix: pop wrong index)
        match.players.pop(idx)
        match.discord_messages_id_list = match.discord_messages_id_list + [
            discord_message_id
        ]

        await self._m.q.replace_pending_match(oid, match.dict())
        logger.info("✅ 🔄 Sub removed for match %s", match_id)
        return await self._reload_pending(oid)

    async def contest_report(
        self,
        match_id: str,
        contestor_discord_id: str,
        reason: str,
        discord_message_id: str | None = None,
    ) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_pending_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)
        contest_report_entry = ContestReport(
            contestor_discord_id=contestor_discord_id, reason=reason
        )
        match.contest_report_list.append(contest_report_entry)
        # v2 contests are ephemeral (D102), so there is no message to record.
        if discord_message_id is not None:
            match.discord_messages_id_list = match.discord_messages_id_list + [
                discord_message_id
            ]

        await self._m.q.replace_pending_match(oid, match.dict())
        logger.info("✅ 🔄 Match %s contested by %s", match_id, contestor_discord_id)
        return await self._reload_pending(oid)

    async def patch_players(
        self,
        match_id: str,
        patch: Sequence[SeatPatch],
        *,
        actor_is_staff: bool,
    ) -> Dict[str, Any]:
        """C1's declarative PATCH: one request, one judgement, one recompute.

        The whole patch is judged before any write, so a rejection leaves
        the document untouched -- partial application is impossible by
        construction rather than by care (D89, D151).
        """
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
        steam_ids = {
            discord_id: await self._m.discord_to_steam_id(discord_id)
            for discord_id in wanted
        }

        apply_players_patch(match, patch, steam_ids)
        match = await self._m._recompute_deltas(match)

        await self._m.q.replace_pending_match(oid, match.dict())
        logger.info(
            "PATCH applied to %d seat(s) on match %s",
            len({e.seat for e in patch}),
            match_id,
        )
        return await self._reload_pending(oid)


def apply_players_patch(
    match: MatchModel,
    patch: Sequence[SeatPatch],
    steam_ids: Mapping[str, Optional[str]],
) -> None:
    """Apply a validated patch in place (D154).

    A seat is a position in the pre-patch array, so field mutations run
    first and structural substitution operations last, in descending seat
    order. Descending means each insert or pop only disturbs positions
    above itself. Ascending substitutes the wrong player: after an insert
    at a low seat, every higher seat names the row below the one the
    client meant, and the result is well formed -- pairing intact,
    adjacency intact -- so nothing downstream catches it.

    Assumes validate_players_patch returned no violations.
    """
    last: Dict[int, SeatPatch] = {}
    for entry in patch:
        last[entry.seat] = entry

    for seat, entry in sorted(last.items()):
        row = match.players[seat]
        if not isinstance(entry.discord_id, UnsetType):
            row.discord_id = entry.discord_id
            row.steam_id = steam_ids.get(entry.discord_id, row.steam_id)
        if not isinstance(entry.quit, UnsetType):
            row.quit = entry.quit
        if not isinstance(entry.placement, UnsetType):
            row.placement = entry.placement
            leaver = _leaver_index(match, seat)
            if leaver is not None:
                match.players[leaver].placement = entry.placement

    structural = [
        (seat, entry.sub_out)
        for seat, entry in last.items()
        if not isinstance(entry.sub_out, UnsetType)
    ]
    for seat, sub_out in sorted(structural, key=lambda p: p[0], reverse=True):
        _apply_sub(match, seat, sub_out, steam_ids)


def _leaver_index(match: MatchModel, seat: int) -> Optional[int]:
    """The synthetic row paired with a seat, by the adjacency v1 builds.

    assign_sub inserts the leaver at sub_in_idx + 1, remove_sub reads
    idx - 1 and set_player_order copies the placement from the row above.
    Three sites depend on it and one has already been wrong once
    (`fix: pop wrong index`), so it is read here, never assumed.
    """
    nxt = seat + 1
    if nxt < len(match.players) and match.players[nxt].subbed_out:
        return nxt
    return None


def _apply_sub(
    match: MatchModel,
    seat: int,
    sub_out: Optional[str],
    steam_ids: Mapping[str, Optional[str]],
) -> None:
    row = match.players[seat]
    leaver = _leaver_index(match, seat)

    if sub_out is None:
        if leaver is None:
            return
        row.is_sub = False
        match.players.pop(leaver)
        return

    if leaver is not None:
        # Repoint rather than insert a second row. This is item 79's guard:
        # v1's assign_sub checks only the range, so subbing one seat twice
        # strands two leavers on one team and rates a duel as 2v1. The
        # declarative shape makes the guard structural, not a check.
        match.players[leaver].discord_id = sub_out
        match.players[leaver].steam_id = steam_ids.get(sub_out)
        return

    row.is_sub = True
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
