from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

from bson import ObjectId
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from trueskill import Rating

from app.core.coerce import as_float, as_int
from app.core.config import settings
from app.features.matches.approval import ApprovalService
from app.features.matches.editing import EditingService
from app.features.matches.editing import _require_int as _require_int
from app.features.matches.errors import (
    InvalidIDError,
    MatchServiceError,
    NotFoundError,
    ParseError,
)
from app.features.matches.ingest import IngestService
from app.features.matches.models import (
    MatchModel,
    PlayerModel,
    StatModel,
)
from app.features.matches.parsers import parse_civ6_save, parse_civ7_save
from app.features.matches.repository import MatchRepository
from app.features.ratings.repository import RatingsRepository
from app.features.ratings.skill import make_ts_env

logger = logging.getLogger(__name__)

RANKING_CONCURRENCY_LIMIT = 8


class MatchService:
    def __init__(self, client: AsyncMongoClient):
        self.q = MatchRepository(client)
        self.ratings = RatingsRepository(client)

    def _to_oid(self, match_id: str) -> ObjectId:
        if not ObjectId.is_valid(match_id):
            raise InvalidIDError("Invalid match id")
        return ObjectId(match_id)

    def _parse_save(self, file_bytes: bytes) -> dict[str, Any]:
        if file_bytes.startswith(b"CIV6"):
            parser = parse_civ6_save
        elif file_bytes.startswith(b"CIV7"):
            parser = parse_civ7_save
        else:
            raise ParseError(
                f"Unrecognized save file format. starts with {file_bytes[:4]!r}"
            )
        try:
            data = parser(file_bytes, settings.civ_save_parser_version)
            logger.info(f"✅ 🔍 Parsed as {data.get('game')}")
            return data
        except Exception as e:
            raise ParseError(f"⚠️ Parse attempt failed: {e}")

    async def discord_to_steam_id(self, discord_id: str) -> str:
        player = await self.q.get_user_by_discord_id(discord_id)
        if not player:
            raise MatchServiceError(f"User {discord_id} not found")
        steam_id = player.get("steam_id")
        if steam_id:
            return str(steam_id)
        if player.get("linked_platform") == "steam" and player.get("linked_account_id"):
            return str(player["linked_account_id"])
        raise MatchServiceError(
            f"User {discord_id} does not have a linked Steam account"
        )

    async def steam_to_discord_id(self, steam_id: str) -> str:
        player = await self.q.get_user_by_steam_id(steam_id)
        if not player:
            return f"-{steam_id}"
        return str(player["discord_id"])

    async def match_id_to_discord(self, match: MatchModel) -> MatchModel:
        for player in match.players:
            if (
                player.steam_id
                and player.steam_id != "-1"
                and player.steam_id.startswith("-") == False
            ):
                player.discord_id = await self.steam_to_discord_id(player.steam_id)
        return match

    async def get_player_ranking(
        self,
        match: MatchModel,
        discord_id: str | None,
        player_index: int,
        is_seasonal: bool = False,
        is_combined: bool = False,
        session: AsyncClientSession | None = None,
    ) -> StatModel:
        # Missing / placeholder IDs
        if not discord_id or discord_id in ("-1", "-2") or discord_id.startswith("-"):
            return StatModel(
                discord_id=discord_id,
                index=player_index,
                id=0,
                mu=settings.ts_mu,
                sigma=settings.ts_sigma,
                games=0,
                wins=0,
                first=0,
                subbedIn=0,
                subbedOut=0,
                civs={},
                leaders={},
            )

        doc = await self.ratings.get_player_stat_doc(
            civ_version=match.game,
            is_seasonal=is_seasonal,
            match_type=match.game_mode,
            is_cloud=match.is_cloud,
            is_combined=is_combined,
            discord_id=discord_id,
            session=session,
        )

        if not doc:
            return StatModel(
                discord_id=discord_id,
                index=player_index,
                id=discord_id,
                mu=settings.ts_mu,
                sigma=settings.ts_sigma,
                games=0,
                wins=0,
                first=0,
                subbedIn=0,
                subbedOut=0,
                civs={},
                leaders={},
            )

        return StatModel(
            discord_id=discord_id,
            index=player_index,
            mu=as_float(doc.get("mu"), settings.ts_mu),
            sigma=as_float(doc.get("sigma"), settings.ts_sigma),
            games=as_int(doc.get("games"), 0),
            wins=as_int(doc.get("wins"), 0),
            first=as_int(doc.get("first"), 0),
            subbedIn=as_int(doc.get("subbedIn"), 0),
            subbedOut=as_int(doc.get("subbedOut"), 0),
            civs=dict(doc.get("civs", {}))
            if isinstance(doc.get("civs", {}), dict)
            else {},
            leaders=dict(doc.get("leaders", {}))
            if isinstance(doc.get("leaders", {}), dict)
            else {},
        )

    async def get_players_ranking(
        self,
        match: MatchModel,
        is_seasonal: bool = False,
        is_combined: bool = False,
        session: AsyncClientSession | None = None,
    ) -> list[StatModel]:
        if not match.players:
            return []

        # A session cannot carry concurrent operations, and approve reads its
        # pre-state inside the transaction (D84).
        if session is not None:
            return [
                await self.get_player_ranking(
                    match, p.discord_id, i, is_seasonal, is_combined, session
                )
                for i, p in enumerate(match.players)
            ]

        # Bounded concurrency, preserve order
        sem = asyncio.Semaphore(min(RANKING_CONCURRENCY_LIMIT, len(match.players)))

        async def _one(i: int, p: PlayerModel) -> StatModel:
            async with sem:
                return await self.get_player_ranking(
                    match, p.discord_id, i, is_seasonal, is_combined
                )

        tasks = [asyncio.create_task(_one(i, p)) for i, p in enumerate(match.players)]
        return await asyncio.gather(*tasks)

    def update_player_stats(
        self, match: MatchModel, players_ranking: list[StatModel], delta_value_name: str
    ) -> tuple[MatchModel, list[StatModel]]:
        teams_wo_subs = defaultdict(list)
        teams_with_sub_ins = defaultdict(list)
        for i, p in enumerate(match.players):
            if p.is_sub:
                teams_with_sub_ins[p.team].append((i, p))
            elif p.subbed_out:
                teams_wo_subs[p.team].append((i, p))
            else:
                teams_wo_subs[p.team].append((i, p))
                teams_with_sub_ins[p.team].append((i, p))
        team_wo_subs_states: list[list[StatModel]] = [
            [players_ranking[p_index_tuple[0]] for p_index_tuple in teams_wo_subs[team]]
            for team in teams_wo_subs
        ]
        team_with_sub_ins_states: list[list[StatModel]] = [
            [
                players_ranking[p_index_tuple[0]]
                for p_index_tuple in teams_with_sub_ins[team]
            ]
            for team in teams_with_sub_ins
        ]

        ts_teams_wo_subs = [
            [Rating(p.mu, p.sigma) for p in team] for team in team_wo_subs_states
        ]
        ts_teams_with_sub_ins = [
            [Rating(p.mu, p.sigma) for p in team] for team in team_with_sub_ins_states
        ]

        placements_wo_subs = [
            teams_wo_subs[team][0][1].placement for team in teams_wo_subs
        ]
        placements_with_sub_ins = [
            teams_with_sub_ins[team][0][1].placement for team in teams_with_sub_ins
        ]

        ts_wo_subs_env = make_ts_env()
        ts_with_sub_ins_env = make_ts_env()

        new_ts_wo_subs = ts_wo_subs_env.rate(ts_teams_wo_subs, ranks=placements_wo_subs)
        new_ts_with_sub_ins = ts_with_sub_ins_env.rate(
            ts_teams_with_sub_ins, ranks=placements_with_sub_ins
        )

        post: list[StatModel] = list(range(len(match.players)))
        for team_idx, team in enumerate(team_wo_subs_states):
            for player_index, player in enumerate(team):
                if match.players[player.index].is_sub:
                    raise ValueError(
                        "This should not happen: player is a sub but being processed in wo_subs team."
                    )
                r = new_ts_wo_subs[team_idx][player_index]
                post[player.index] = StatModel(
                    discord_id=player.id,
                    index=player.index,
                    id=player.id,
                    mu=float(r.mu),
                    sigma=float(r.sigma),
                    games=player.games,
                    wins=player.wins,
                    first=player.first,
                    subbedIn=player.subbedIn,
                    subbedOut=player.subbedOut,
                    civs=player.civs,
                    leaders=player.leaders,
                )
        for team_idx, team in enumerate(team_with_sub_ins_states):
            for player_index, player in enumerate(team):
                if match.players[player.index].is_sub:
                    r = new_ts_with_sub_ins[team_idx][player_index]
                    post[player.index] = StatModel(
                        discord_id=player.id,
                        index=player.index,
                        id=player.id,
                        mu=float(r.mu),
                        sigma=float(r.sigma),
                        games=player.games,
                        wins=player.wins,
                        first=player.first,
                        subbedIn=player.subbedIn,
                        subbedOut=player.subbedOut,
                        civs=player.civs,
                        leaders=player.leaders,
                    )
        for i, p in enumerate(match.players):
            p_current_ranking = players_ranking[i]
            delta = (
                round(post[i].mu - p_current_ranking.mu) if p.discord_id != None else 0
            )
            if p.is_sub:
                # Subbed in player
                p.__setattr__(
                    delta_value_name, max(settings.min_points_for_subs, delta)
                )
            elif p.subbed_out:
                # Subbed out Player
                p.__setattr__(delta_value_name, delta if delta < 0 else 0)
            else:
                # Regular player
                p.__setattr__(delta_value_name, delta)
            post[i].mu = p_current_ranking.mu + getattr(p, delta_value_name)
        return match, post

    async def _recompute_deltas(self, match: MatchModel) -> MatchModel:
        """Recompute the delta/season_delta/combined_delta triple on match.players.

        This spine (three ranking loads + three rating passes) used to be copy-pasted
        in six methods.
        """
        players_ranking = await self.get_players_ranking(match)
        players_season_ranking = await self.get_players_ranking(match, is_seasonal=True)
        players_combined_ranking = await self.get_players_ranking(
            match, is_combined=True
        )

        match, _ = self.update_player_stats(match, players_ranking, "delta")
        match, _ = self.update_player_stats(
            match, players_season_ranking, "season_delta"
        )
        match, _ = self.update_player_stats(
            match, players_combined_ranking, "combined_delta"
        )
        return match

    async def get_match(self, match_id: str) -> dict[str, Any]:
        oid = self._to_oid(match_id)
        doc = await self.q.find_pending_by_id(oid)
        if not doc:
            raise NotFoundError("Match not found")
        doc["match_id"] = str(doc.pop("_id"))
        return doc

    async def create_from_save(
        self,
        file_bytes: bytes,
        reporter_discord_id: str,
        is_cloud: bool,
        discord_message_id: str,
    ) -> dict[str, Any]:
        return await IngestService(self).create_from_save(
            file_bytes, reporter_discord_id, is_cloud, discord_message_id
        )

    async def append_discord_message_id_list(
        self, match_id: str, discord_message_id_list: list[str]
    ) -> dict[str, Any]:
        return await EditingService(self).append_discord_message_id_list(
            match_id, discord_message_id_list
        )

    async def update_match(
        self, match_id: str, update_data: dict[str, Any]
    ) -> dict[str, Any]:
        return await EditingService(self).update_match(match_id, update_data)

    async def set_player_order(
        self, match_id: str, player_order: str, discord_message_id: str
    ) -> dict[str, Any]:
        return await EditingService(self).set_player_order(
            match_id, player_order, discord_message_id
        )

    async def change_order(
        self, match_id: str, new_order: str, discord_message_id: str
    ) -> dict[str, Any]:
        return await EditingService(self).change_order(
            match_id, new_order, discord_message_id
        )

    async def delete_pending_match(self, match_id: str) -> dict[str, Any]:
        return await EditingService(self).delete_pending_match(match_id)

    async def trigger_quit(
        self, match_id: str, quitter_discord_id: str, discord_message_id: str
    ) -> dict[str, Any]:
        return await EditingService(self).trigger_quit(
            match_id, quitter_discord_id, discord_message_id
        )

    async def assign_discord_id(
        self,
        match_id: str,
        player_id: str,
        player_discord_id: str,
        discord_message_id: str,
    ) -> dict[str, Any]:
        return await EditingService(self).assign_discord_id(
            match_id, player_id, player_discord_id, discord_message_id
        )

    async def assign_discord_id_all(
        self, match_id: str, player_discord_id: list[str], discord_message_id: str
    ) -> dict[str, Any]:
        return await EditingService(self).assign_discord_id_all(
            match_id, player_discord_id, discord_message_id
        )

    async def assign_sub(
        self,
        match_id: str,
        sub_in_id: str,
        sub_out_discord_id: str,
        discord_message_id: str,
    ) -> dict[str, Any]:
        return await EditingService(self).assign_sub(
            match_id, sub_in_id, sub_out_discord_id, discord_message_id
        )

    async def remove_sub(
        self, match_id: str, sub_out_id: str, discord_message_id: str
    ) -> dict[str, Any]:
        return await EditingService(self).remove_sub(
            match_id, sub_out_id, discord_message_id
        )

    async def contest_report(
        self,
        match_id: str,
        contestor_discord_id: str,
        reason: str,
        discord_message_id: str,
    ) -> dict[str, Any]:
        return await EditingService(self).contest_report(
            match_id, contestor_discord_id, reason, discord_message_id
        )

    async def revert_match(self, match_id: str) -> dict[str, Any]:
        return await ApprovalService(self).revert_match(match_id)

    async def approve_match(
        self, match_id: str, approver_discord_id: str
    ) -> dict[str, Any]:
        return await ApprovalService(self).approve_match(match_id, approver_discord_id)


__all__ = [
    "InvalidIDError",
    "MatchService",
    "MatchServiceError",
    "NotFoundError",
    "ParseError",
]
