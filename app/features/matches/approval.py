from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from bson.int64 import Int64
from pymongo.errors import PyMongoError

from app.core.coerce import as_float
from app.features.matches.errors import (
    MatchServiceError,
    NotFoundError,
    RetryLaterError,
)
from app.features.matches.models import MatchModel, PlayerModel, StatModel
from app.features.matches.tallies import bump, stat_legs
from app.features.matches.validation import approval_refusal
from app.features.ratings.events import build_match_event
from app.features.ratings.scope import drives_rank_roles, stat_scope

if TYPE_CHECKING:
    from app.features.matches.service import MatchService

logger = logging.getLogger(__name__)


def _write_failure(what: str, exc: Exception) -> MatchServiceError | NotFoundError:
    """A failed rating transaction, said without its internals: a clash with
    another write can be retried as is; anything else cannot."""
    if isinstance(exc, NotFoundError):
        return exc
    if isinstance(exc, PyMongoError) and exc.has_error_label(
        "TransientTransactionError"
    ):
        return RetryLaterError(f"The {what} clashed with another change. Try again.")
    return MatchServiceError(f"The {what} could not be saved.")


def _affected(match: MatchModel, ratings: list[StatModel]) -> list[dict[str, Any]]:
    """The players whose rank roles follow this match, with their rating now."""
    if not drives_rank_roles(game_mode=match.game_mode, is_cloud=match.is_cloud):
        return []
    return [
        {"discord_id": str(p.discord_id), "rating_mu": float(ratings[i].mu)}
        for i, p in enumerate(match.players)
        if p.discord_id and not str(p.discord_id).startswith("-")
    ]


class ApprovalService:
    """Approve and revert: the two flows that move ratings."""

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
        """One stats document for approve (step=+1) or revert (step=-1)."""

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

        session = await self._m.q.start_session()
        async with session:
            async with await session.start_transaction():
                try:
                    # A match from an earlier season leaves this season's
                    # records alone; read here, a season change is never missed.
                    open_season = await self._m.seasons.read_open(
                        match.game, session=session
                    )
                    past_season = res.get("season_id") not in (
                        None,
                        open_season["_id"] if open_season else None,
                    )
                    # Read inside the transaction, so an approve committed
                    # meanwhile is a write conflict, not a lost update.
                    pre_lifetime = await self._m.get_players_ranking(
                        match, session=session
                    )
                    pre_season = await self._m.get_players_ranking(
                        match, is_seasonal=True, session=session
                    )
                    pre_combined = await self._m.get_players_ranking(
                        match, is_combined=True, session=session
                    )
                    occurred_at = datetime.now(UTC)
                    events: list[dict[str, Any]] = []
                    # The rating each player's rank role follows, after the revert.
                    rank_after: dict[str, float] = {}

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
                            if is_seasonal and past_season:
                                continue
                            mu_after = pre.mu - delta_value
                            sigma_after = pre.sigma + 2
                            if not is_seasonal and is_combined == match.is_cloud:
                                rank_after[did] = mu_after
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
                                    match_created_at=match.created_at,
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

                    if not await self._m.q.delete_validated_match(oid, session=session):
                        raise NotFoundError("Match not found")

                    await session.commit_transaction()
                except Exception as e:
                    logger.exception("Transaction failed while writing to DB; aborting")
                    if session.in_transaction:
                        await session.abort_transaction()
                    raise _write_failure("revert", e) from e
        affected = (
            [{"discord_id": d, "rating_mu": mu} for d, mu in rank_after.items()]
            if drives_rank_roles(game_mode=match.game_mode, is_cloud=match.is_cloud)
            else []
        )
        return {
            "match_id": str(match_id),
            **match.model_dump(),
            "affected_players": affected,
        }

    async def approve_match(
        self, match_id: str, approver_discord_id: str
    ) -> dict[str, Any]:
        oid = self._m._to_oid(match_id)
        claimed_at = datetime.now(UTC)
        res = await self._m.q.claim_pending_match(oid, now=claimed_at)
        if res is None:
            # Approved already, perhaps by a request whose answer was lost: the
            # same answer again lets the bot finish posting the result.
            done = await self._m.q.find_validated_by_id(oid)
            if done is not None:
                approved = MatchModel(**done)
                ratings = await self._m.get_players_ranking(
                    approved, is_combined=approved.is_cloud
                )
                return {
                    "match_id": str(oid),
                    **approved.model_dump(),
                    "approved_at": done.get("approved_at"),
                    "approver_discord_id": done.get("approver_discord_id"),
                    "affected_players": _affected(approved, ratings),
                }
            raise await self._m.refusal(oid)

        try:
            match = MatchModel(**res)

            # Ensure all players have placements set
            for p in match.players:
                if p.placement is None:
                    raise MatchServiceError(
                        "All players must have a placement before approving"
                    )
            if refusal := approval_refusal(match):
                raise MatchServiceError(refusal)

            # Read before the transaction opens: a season is cached reference
            # data, and a missing row must fail the approve before any write.
            season = await self._m.seasons.get_current_season(match.game)

            session = await self._m.q.start_session()
            async with session:
                async with await session.start_transaction():
                    try:
                        # Read again here: a season change since the cached read
                        # is seen, so the stamp names the season the stats land in.
                        season = (
                            await self._m.seasons.read_open(match.game, session=session)
                            or season
                        )
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

                        now = datetime.now(UTC)
                        validated_doc = match.model_dump()
                        validated_doc["_id"] = oid
                        validated_doc["created_at"] = res.get("created_at", now)
                        validated_doc["approved_at"] = now
                        validated_doc["season_id"] = season["_id"]
                        validated_doc["reporter_discord_id"] = res.get(
                            "reporter_discord_id"
                        )
                        validated_doc["approver_discord_id"] = approver_discord_id
                        validated_doc["discord_messages_id_list"] = res.get(
                            "discord_messages_id_list", []
                        )
                        validated_doc["save_file_hash"] = res.get("save_file_hash", "")
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
                        # A failed commit has ended the transaction already.
                        if session.in_transaction:
                            await session.abort_transaction()
                        raise _write_failure("approval", e) from e

            logger.info("✅ ✅ Approved match %s", match_id)
            affected_players = _affected(
                match, post_combined if match.is_cloud else post_lifetime
            )
            # The answer is the approved match, so it says who approved it and when.
            return {
                "match_id": str(validated_insert_id),
                **match.model_dump(),
                "approved_at": validated_doc["approved_at"],
                "approver_discord_id": approver_discord_id,
                "affected_players": affected_players,
            }
        except Exception:
            # A write conflict is an ordinary transaction outcome; leaving the
            # claim set would make the match permanently unapprovable.
            await self._m.q.release_pending_claim(oid, claimed_at=claimed_at)
            raise


__all__ = ["ApprovalService"]
