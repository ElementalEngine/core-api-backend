from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, TYPE_CHECKING

from bson.int64 import Int64

from app.features.matches.errors import MatchServiceError, NotFoundError
from app.features.matches.models import MatchModel, PlayerModel, StatModel
from app.features.matches.tallies import bump, stat_legs
from app.core.coerce import as_float
from app.features.ratings.events import build_match_event
from app.features.ratings.scope import stat_scope

if TYPE_CHECKING:
    from app.features.matches.service import MatchService

logger = logging.getLogger(__name__)


class ApprovalService:
    """Approve and revert: the two flows that move ratings.

    Reads and rating maths stay on MatchService and are borrowed through
    self._m; this owns the transactional write side, which is what
    playbook Entry 8 rewrites.
    """

    def __init__(self, matches: MatchService) -> None:
        self._m = matches

    def _shift_tally(
        self,
        existing: Any,
        key: str | None,
        *,
        won: bool,
        step: int,
    ) -> dict[str, Any]:
        """The rebuild uses the same bump(), so the two cannot drift."""
        tally = dict(existing) if isinstance(existing, dict) else {}
        if not key:
            return tally
        return bump(tally, key, won=won, step=step)

    def update_existing_stat(
        self,
        player: PlayerModel,
        existing_civs: dict[str, Any],
    ) -> dict[str, Any]:
        # D44: the tally keys on the raw token; display names resolve on read.
        return self._shift_tally(
            existing_civs, player.civ, won=player.delta > 0, step=+1
        )

    def revert_existing_stat(
        self,
        player: PlayerModel,
        existing_civs: dict[str, Any],
    ) -> dict[str, Any]:
        return self._shift_tally(
            existing_civs, player.civ, won=player.delta > 0, step=-1
        )

    def update_existing_leaders(
        self,
        player: PlayerModel,
        existing_leaders: dict[str, Any],
    ) -> dict[str, Any]:
        # Civ6 records written before Entry 10 have no leader; they are left
        # out rather than bucketed under a placeholder key.
        return self._shift_tally(
            existing_leaders, player.leader, won=player.delta > 0, step=+1
        )

    def revert_existing_leaders(
        self,
        player: PlayerModel,
        existing_leaders: dict[str, Any],
    ) -> dict[str, Any]:
        return self._shift_tally(
            existing_leaders, player.leader, won=player.delta > 0, step=-1
        )

    def _build_stat_doc(
        self,
        *,
        discord_id: str,
        pre: StatModel,
        player: PlayerModel,
        mu: float,
        sigma: float,
        delta_value: float,
        civs: dict[str, Any],
        leaders: dict[str, Any],
        step: int,
    ) -> dict[str, Any]:
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
            "leaders": leaders,
            "lastModified": datetime.now(UTC),
        }

    async def revert_match(self, match_id: str) -> dict[str, Any]:
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
                    events: list[dict[str, Any]] = []

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

                        legs = stat_legs(is_cloud=match.is_cloud)
                        for pre, delta_value, is_seasonal, is_combined in (
                            (pre_lifetime[i], delta, False, False),
                            (pre_season[i], season_delta, True, False),
                            (pre_combined[i], combined_delta, False, True),
                        ):
                            if (is_seasonal, is_combined) not in legs:
                                continue
                            mu_after = pre.mu - delta_value
                            sigma_after = pre.sigma + 2
                            doc = self._build_stat_doc(
                                discord_id=did,
                                pre=pre,
                                player=p,
                                mu=mu_after,
                                sigma=sigma_after,
                                delta_value=delta_value,
                                civs=self.revert_existing_stat(p, pre.civs),
                                leaders=self.revert_existing_leaders(p, pre.leaders),
                                step=-1,
                            )
                            await self._m.ratings.upsert_player_stat_doc(
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
                            await self._m.q.remove_sub_in(did, oid, session=session)

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
    ) -> dict[str, Any]:
        oid = self._m._to_oid(match_id)
        # D84: the conditional claim replaces approve_lock. No match means
        # gone or already claimed -- both are the 404 the lock produced.
        res = await self._m.q.claim_pending_match(oid, now=datetime.now(UTC))
        if res is None:
            raise NotFoundError("Match not found")

        try:
            match = MatchModel(**res)

            # Ensure all players have placements set
            for p in match.players:
                if p.placement is None:
                    raise MatchServiceError(
                        "All players must have a placement before approving"
                    )

            session = await self._m.q.start_session()
            async with session:
                async with await session.start_transaction():
                    try:
                        # D84: pre-state reads inside the transaction. The lock
                        # was substituting for this, not merely serialising
                        # approvals, which is why they are one change.
                        pre_lifetime = await self._m.get_players_ranking(
                            match, session=session
                        )
                        pre_season = await self._m.get_players_ranking(
                            match, is_seasonal=True, session=session
                        )
                        pre_combined = await self._m.get_players_ranking(
                            match, is_combined=True, session=session
                        )

                        match, post_lifetime = self._m.update_player_stats(
                            match, pre_lifetime, "delta"
                        )
                        match, post_season = self._m.update_player_stats(
                            match, pre_season, "season_delta"
                        )
                        match, post_combined = self._m.update_player_stats(
                            match, pre_combined, "combined_delta"
                        )

                        occurred_at = datetime.now(UTC)
                        events: list[dict[str, Any]] = []

                        # Stats writes
                        for i, p in enumerate(match.players):
                            if (
                                not p.discord_id
                                or p.discord_id in ("-1", "-2")
                                or p.discord_id.startswith("-")
                            ):
                                continue

                            did = str(p.discord_id)

                            legs = stat_legs(is_cloud=match.is_cloud)
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
                                if (is_seasonal, is_combined) not in legs:
                                    continue
                                doc = self._build_stat_doc(
                                    discord_id=did,
                                    pre=pre,
                                    player=p,
                                    mu=post.mu,
                                    sigma=post.sigma,
                                    delta_value=delta_value,
                                    civs=self.update_existing_stat(p, pre.civs),
                                    leaders=self.update_existing_leaders(
                                        p, pre.leaders
                                    ),
                                    step=1,
                                )
                                await self._m.ratings.upsert_player_stat_doc(
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
                                await self._m.q.record_sub_in(did, oid, session=session)

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
                        # Conditional, not defaulted: "" and None both satisfy
                        # the partial index's {$exists: true}, so a default
                        # would collide the second approval of any match
                        # predating Entry 12. D83 Hardening 1.
                        byte_hash = res.get("save_bytes_sha256")
                        if byte_hash:
                            validated_doc["save_bytes_sha256"] = byte_hash
                        else:
                            validated_doc.pop("save_bytes_sha256", None)
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
        except Exception:
            # A write conflict is an ordinary transaction outcome; leaving the
            # claim set would make the match permanently unapprovable.
            await self._m.q.release_pending_claim(oid)
            raise


__all__ = ["ApprovalService"]
