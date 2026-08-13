from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Dict, List

from bson.int64 import Int64

from app.features.matches.errors import MatchServiceError, NotFoundError
from app.features.matches.models import MatchModel, PlayerModel, StatModel
from app.core.coerce import as_float, as_int
from app.features.matches.utils import get_cpl_name
from app.features.ratings.events import build_match_event
from app.shared.persistence.mongo_queries import stat_scope

if TYPE_CHECKING:
    from app.features.matches.service import MatchService

logger = logging.getLogger(__name__)

approve_lock = asyncio.Lock()


class ApprovalService:
    """Approve and revert: the two flows that move ratings.

    Reads and rating maths stay on MatchService and are borrowed through
    self._m; this owns the transactional write side, which is what
    playbook Entry 8 rewrites.
    """

    def __init__(self, matches: MatchService) -> None:
        self._m = matches

    def _shift_civ_stat(
        self,
        match: MatchModel,
        player: PlayerModel,
        existing_civs: Dict[str, Any],
        step: int,
    ) -> Dict[str, Any]:
        # Normalize civ naming
        civ_name = get_cpl_name(match.game, player.civ, getattr(player, "leader", None))

        civs = dict(existing_civs) if isinstance(existing_civs, dict) else {}

        entry = civs.get(civ_name)

        # Backwards compatibility:
        # - legacy shape: {"DutchWilhelmina": 3}
        # - new shape: {"DutchWilhelmina": {"games": 3, "wins": 1}}
        if isinstance(entry, dict):
            games = as_int(entry.get("games", 0), 0)
            wins = as_int(entry.get("wins", 0), 0)
        elif isinstance(entry, int):
            games = entry
            wins = 0
        else:
            games = 0
            wins = 0

        games += step
        if player.delta > 0:
            wins += step
        if step < 0:
            games = max(0, games)
            wins = max(0, wins)

        civs[civ_name] = {"games": games, "wins": wins}
        return civs

    def update_existing_stat(
        self,
        match: MatchModel,
        player: PlayerModel,
        existing_civs: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._shift_civ_stat(match, player, existing_civs, +1)

    def revert_existing_stat(
        self,
        match: MatchModel,
        player: PlayerModel,
        existing_civs: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._shift_civ_stat(match, player, existing_civs, -1)

    def _build_stat_doc(
        self,
        *,
        discord_id: str,
        pre: StatModel,
        player: PlayerModel,
        mu: float,
        sigma: float,
        delta_value: float,
        civs: Dict[str, Any],
        step: int,
    ) -> Dict[str, Any]:
        """One stats document for approve (step=+1) or revert (step=-1).

        Reverted counters clamp at 0 so a revert can never write negative stats.
        """

        def shift(current: int, hit: bool) -> int:
            value = current + (step if hit else 0)
            return max(0, value) if step < 0 else value

        return {
            "_id": Int64(discord_id),
            "mu": float(mu),
            "sigma": float(sigma),
            "games": shift(int(pre.games), True),
            "wins": shift(int(pre.wins), delta_value > 0),
            "first": shift(int(pre.first), player.placement == 0),
            "subbedIn": shift(int(pre.subbedIn), player.is_sub),
            "subbedOut": shift(int(pre.subbedOut), player.subbed_out),
            "civs": civs,
            "lastModified": datetime.now(UTC),
        }

    async def revert_match(self, match_id: str) -> Dict[str, Any]:
        oid = self._m._to_oid(match_id)
        res = await self._m.q.find_validated_by_id(oid)
        if not res:
            raise NotFoundError("Match not found")

        match = MatchModel(**res)

        # Pre-states
        pre_lifetime = await self._m.get_players_ranking(match)
        pre_season = await self._m.get_players_ranking(match, is_seasonal=True)
        pre_combined = await self._m.get_players_ranking(match, is_combined=True)

        session = await self._m.q.start_session()
        async with session:
            async with await session.start_transaction():
                try:
                    occurred_at = datetime.now(UTC)
                    events: List[Dict[str, Any]] = []

                    # Stats writes
                    for i, p in enumerate(match.players):
                        if (
                            not p.discord_id
                            or p.discord_id in ("-1", "-2")
                            or p.discord_id.startswith("-")
                        ):
                            continue

                        did = str(p.discord_id)

                        # Legacy validated docs may lack the season/combined deltas (None);
                        # coerce once so the arithmetic below cannot TypeError.
                        delta = as_float(p.delta, 0.0)
                        season_delta = as_float(p.season_delta, 0.0)
                        combined_delta = as_float(p.combined_delta, 0.0)

                        for pre, delta_value, is_seasonal, is_combined in (
                            (pre_lifetime[i], delta, False, False),
                            (pre_season[i], season_delta, True, False),
                            (pre_combined[i], combined_delta, False, True),
                        ):
                            mu_after = pre.mu - delta_value
                            sigma_after = pre.sigma + 2
                            doc = self._build_stat_doc(
                                discord_id=did,
                                pre=pre,
                                player=p,
                                mu=mu_after,
                                sigma=sigma_after,
                                delta_value=delta_value,
                                civs=self.revert_existing_stat(match, p, pre.civs),
                                step=-1,
                            )
                            await self._m.q.upsert_player_stat_doc(
                                civ_version=match.game,
                                is_seasonal=is_seasonal,
                                match_type=match.game_mode,
                                is_cloud=match.is_cloud,
                                is_combined=is_combined,
                                discord_id=did,
                                doc=doc,
                                session=session,
                            )
                            events.append(
                                build_match_event(
                                    event_type="revert",
                                    match_id=oid,
                                    match_created_at=res.get("created_at"),
                                    occurred_at=occurred_at,
                                    discord_id=did,
                                    scope=stat_scope(
                                        civ_version=match.game,
                                        is_seasonal=is_seasonal,
                                        match_type=match.game_mode,
                                        is_cloud=match.is_cloud,
                                        is_combined=is_combined,
                                    ),
                                    mu_before=pre.mu,
                                    mu_after=mu_after,
                                    sigma_before=pre.sigma,
                                    sigma_after=sigma_after,
                                    applied_delta=-delta_value,
                                )
                            )

                        if p.is_sub:
                            await self._m.q.dec_subs_in(did, session=session)

                    await self._m.ratings.insert_events(events, session=session)

                    await self._m.q.delete_validated_match(oid, session=session)

                    await session.commit_transaction()
                except Exception as e:
                    logger.exception("Transaction failed while writing to DB; aborting")
                    await session.abort_transaction()
                    raise MatchServiceError(
                        f"An error occured during writing to DB: {e}"
                    )
        return {"match_id": str(match_id), **match.dict()}

    async def approve_match(
        self, match_id: str, approver_discord_id: str
    ) -> Dict[str, Any]:
        async with approve_lock:
            oid = self._m._to_oid(match_id)
            res = await self._m.q.find_pending_by_id(oid)
            if not res:
                raise NotFoundError("Match not found")

            match = MatchModel(**res)

            # Ensure all players have placements set
            for p in match.players:
                if p.placement is None:
                    raise MatchServiceError(
                        "All players must have a placement before approving"
                    )

            # Pre-states
            pre_lifetime = await self._m.get_players_ranking(match)
            pre_season = await self._m.get_players_ranking(match, is_seasonal=True)
            pre_combined = await self._m.get_players_ranking(match, is_combined=True)

            # Rating updates (writes deltas into match players + returns post mu/sigma)
            match, post_lifetime = self._m.update_player_stats(
                match, pre_lifetime, "delta"
            )
            match, post_season = self._m.update_player_stats(
                match, pre_season, "season_delta"
            )
            match, post_combined = self._m.update_player_stats(
                match, pre_combined, "combined_delta"
            )

            session = await self._m.q.start_session()
            async with session:
                async with await session.start_transaction():
                    try:
                        occurred_at = datetime.now(UTC)
                        events: List[Dict[str, Any]] = []

                        # Stats writes
                        for i, p in enumerate(match.players):
                            if (
                                not p.discord_id
                                or p.discord_id in ("-1", "-2")
                                or p.discord_id.startswith("-")
                            ):
                                continue

                            did = str(p.discord_id)

                            for pre, post, delta_value, is_seasonal, is_combined in (
                                (
                                    pre_lifetime[i],
                                    post_lifetime[i],
                                    p.delta,
                                    False,
                                    False,
                                ),
                                (
                                    pre_season[i],
                                    post_season[i],
                                    p.season_delta,
                                    True,
                                    False,
                                ),
                                (
                                    pre_combined[i],
                                    post_combined[i],
                                    p.combined_delta,
                                    False,
                                    True,
                                ),
                            ):
                                doc = self._build_stat_doc(
                                    discord_id=did,
                                    pre=pre,
                                    player=p,
                                    mu=post.mu,
                                    sigma=post.sigma,
                                    delta_value=delta_value,
                                    civs=self.update_existing_stat(match, p, pre.civs),
                                    step=1,
                                )
                                await self._m.q.upsert_player_stat_doc(
                                    civ_version=match.game,
                                    is_seasonal=is_seasonal,
                                    match_type=match.game_mode,
                                    is_cloud=match.is_cloud,
                                    is_combined=is_combined,
                                    discord_id=did,
                                    doc=doc,
                                    session=session,
                                )
                                events.append(
                                    build_match_event(
                                        event_type="approve",
                                        match_id=oid,
                                        match_created_at=res.get("created_at"),
                                        occurred_at=occurred_at,
                                        discord_id=did,
                                        scope=stat_scope(
                                            civ_version=match.game,
                                            is_seasonal=is_seasonal,
                                            match_type=match.game_mode,
                                            is_cloud=match.is_cloud,
                                            is_combined=is_combined,
                                        ),
                                        mu_before=pre.mu,
                                        mu_after=post.mu,
                                        sigma_before=pre.sigma,
                                        sigma_after=post.sigma,
                                        applied_delta=delta_value,
                                    )
                                )

                            if p.is_sub:
                                await self._m.q.inc_subs_in(did, session=session)

                        await self._m.ratings.insert_events(events, session=session)

                        # Move pending -> validated. D122: the validated
                        # document keeps the pending _id, so a match holds one
                        # identity for its life and its events link.
                        now = datetime.now(UTC)
                        validated_doc = match.dict()
                        validated_doc["_id"] = oid
                        validated_doc["created_at"] = res.get("created_at", now)
                        validated_doc["approved_at"] = now
                        validated_doc["reporter_discord_id"] = res.get(
                            "reporter_discord_id"
                        )
                        validated_doc["approver_discord_id"] = approver_discord_id
                        validated_doc["discord_messages_id_list"] = res.get(
                            "discord_messages_id_list", []
                        )
                        validated_doc["save_file_hash"] = res.get("save_file_hash", "")
                        validated_doc["contest_report_list"] = []

                        validated_insert_id = await self._m.q.insert_validated_match(
                            validated_doc, session=session
                        )
                        await self._m.q.delete_pending_match(oid, session=session)

                        await session.commit_transaction()
                    except Exception as e:
                        logger.exception(
                            "Transaction failed while writing to DB; aborting"
                        )
                        await session.abort_transaction()
                        raise MatchServiceError(
                            f"An error occured during writing to DB: {e}"
                        )

            logger.info("✅ ✅ Approved match %s", match_id)
            affected_players = []
            if match.game_mode.lower() == "ffa" or match.is_cloud:
                affected_players = [
                    {
                        "discord_id": str(player.discord_id),
                        "rating_mu": float(post_combined[index].mu)
                        if match.is_cloud
                        else post_lifetime[index].mu,
                    }
                    for index, player in enumerate(match.players)
                    if player.discord_id
                    and player.discord_id not in ("-1", "-2")
                    and not str(player.discord_id).startswith("-")
                ]
            return {
                "match_id": str(validated_insert_id),
                **match.dict(),
                "affected_players": affected_players,
            }


__all__ = ["ApprovalService", "approve_lock"]
