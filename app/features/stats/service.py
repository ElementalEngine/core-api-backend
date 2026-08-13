from __future__ import annotations

import asyncio
import random
from typing import Dict, List, Tuple

from pymongo import AsyncMongoClient

from app.core.config import settings
from app.features.stats.constants import (
    ALLOWED_CIV_VERSIONS,
    ALLOWED_GAME_TYPES,
    ALLOWED_MATCH_TYPES,
)
from app.features.stats.errors import InvalidStatsRequestError, StatsNotFoundError
from app.features.stats.repository import StatsRepository
from app.features.ratings.skill import make_ts_env
from app.features.stats.schemas import (
    StatRow,
    StatSet,
    TeamGenResponse,
    UserStatsResponse,
)


class StatsService:
    def __init__(self, client: AsyncMongoClient) -> None:
        self.repository = StatsRepository(client)

    def _validate(self, civ_version: str, game_type: str) -> Tuple[str, bool]:
        version = (civ_version or "").strip().lower()
        normalized_game_type = (game_type or "").strip().lower()

        if version not in ALLOWED_CIV_VERSIONS:
            raise InvalidStatsRequestError("Invalid civ_version")
        if normalized_game_type not in ALLOWED_GAME_TYPES:
            raise InvalidStatsRequestError("Invalid game_type")

        return version, normalized_game_type == "cloud"

    def _doc_to_row(self, doc: Dict[str, object]) -> StatRow:
        mu_raw = float(doc.get("mu", settings.ts_mu))
        sigma_raw = float(doc.get("sigma", settings.ts_sigma))

        return StatRow(
            mu=int(round(mu_raw)),
            sigma=sigma_raw,
            games=int(doc.get("games", 0)),
            wins=int(doc.get("wins", 0)),
            first=int(doc.get("first", 0)),
            subbedIn=int(doc.get("subbedIn", doc.get("subbed_in", 0))),
            subbedOut=int(doc.get("subbedOut", doc.get("subbed_out", 0))),
            lastModified=doc.get("lastModified"),
        )

    @staticmethod
    def _has_any_stats(response: UserStatsResponse) -> bool:
        for stat_set in (response.lifetime, response.season):
            if stat_set.ffa or stat_set.teamer or stat_set.duel:
                return True
        return False

    async def get_user_stats(
        self, *, civ_version: str, game_type: str, discord_id: str
    ) -> UserStatsResponse:
        version, is_cloud = self._validate(civ_version, game_type)
        normalized_discord_id = str(discord_id).strip()
        if not normalized_discord_id:
            raise InvalidStatsRequestError("Missing discord_id")
        if not normalized_discord_id.isdigit():
            raise InvalidStatsRequestError("Invalid discord_id")

        lifetime_map = await self._load_stat_set(
            civ_version=version,
            is_seasonal=False,
            is_cloud=is_cloud,
            discord_ids=[normalized_discord_id],
        )

        if is_cloud:
            season_map: Dict[str, StatSet] = {normalized_discord_id: StatSet()}
        else:
            season_map = await self._load_stat_set(
                civ_version=version,
                is_seasonal=True,
                is_cloud=is_cloud,
                discord_ids=[normalized_discord_id],
            )

        response = UserStatsResponse(
            discord_id=normalized_discord_id,
            civ_version=version,
            game_type="cloud" if is_cloud else "realtime",
            lifetime=lifetime_map.get(normalized_discord_id, StatSet()),
            season=season_map.get(normalized_discord_id, StatSet()),
        )

        if not self._has_any_stats(response):
            raise StatsNotFoundError("No stats found")

        return response

    async def get_users_stats_batch(
        self, *, civ_version: str, game_type: str, discord_ids: List[str]
    ) -> List[UserStatsResponse]:
        version, is_cloud = self._validate(civ_version, game_type)

        ids = [str(value).strip() for value in discord_ids if str(value).strip()]
        if not ids:
            return []
        if any(not discord_id.isdigit() for discord_id in ids):
            raise InvalidStatsRequestError("Invalid discord_ids")

        lifetime_map = await self._load_stat_set(
            civ_version=version,
            is_seasonal=False,
            is_cloud=is_cloud,
            discord_ids=ids,
        )

        if is_cloud:
            season_map: Dict[str, StatSet] = {
                discord_id: StatSet() for discord_id in ids
            }
        else:
            season_map = await self._load_stat_set(
                civ_version=version,
                is_seasonal=True,
                is_cloud=is_cloud,
                discord_ids=ids,
            )

        return [
            UserStatsResponse(
                discord_id=discord_id,
                civ_version=version,
                game_type="cloud" if is_cloud else "realtime",
                lifetime=lifetime_map.get(discord_id, StatSet()),
                season=season_map.get(discord_id, StatSet()),
            )
            for discord_id in ids
        ]

    async def reset_user_stats(
        self, *, civ_version: str, game_type: str, discord_id: str
    ) -> UserStatsResponse:
        version, is_cloud = self._validate(civ_version, game_type)
        normalized_discord_id = str(discord_id).strip()
        if not normalized_discord_id:
            raise InvalidStatsRequestError("Missing discord_id")
        if not normalized_discord_id.isdigit():
            raise InvalidStatsRequestError("Invalid discord_id")

        lifetime_map = await self._load_stat_set(
            civ_version=version,
            is_seasonal=False,
            is_cloud=is_cloud,
            discord_ids=[normalized_discord_id],
        )

        if is_cloud:
            season_map: Dict[str, StatSet] = {normalized_discord_id: StatSet()}
        else:
            season_map = await self._load_stat_set(
                civ_version=version,
                is_seasonal=True,
                is_cloud=is_cloud,
                discord_ids=[normalized_discord_id],
            )

        response = UserStatsResponse(
            discord_id=normalized_discord_id,
            civ_version=version,
            game_type="cloud" if is_cloud else "realtime",
            lifetime=lifetime_map.get(normalized_discord_id, StatSet()),
            season=season_map.get(normalized_discord_id, StatSet()),
        )

        # A user with no stats must 404 without side effects — previously the reset
        # transaction ran first and inserted a stat_reset marker even when there was
        # nothing to reset.
        if not self._has_any_stats(response):
            raise StatsNotFoundError("No stats found")

        await self._reset_stat_set(
            civ_version=version,
            is_cloud=is_cloud,
            discord_id=normalized_discord_id,
        )

        return response

    async def get_team_gen(
        self, *, civ_version: str, game_type: str, discord_ids: List[str]
    ) -> TeamGenResponse:
        version, is_cloud = self._validate(civ_version, game_type)

        ids = [str(value).strip() for value in discord_ids if str(value).strip()]
        if not ids:
            return TeamGenResponse(
                civ_version=version,
                game_type="cloud" if is_cloud else "realtime",
                game_quality=0.0,
                teams=[[], []],
            )
        if any(not discord_id.isdigit() for discord_id in ids):
            raise InvalidStatsRequestError("Invalid discord_ids")

        players_ranking = await self._load_stat_set(
            civ_version=version,
            is_seasonal=False,
            is_cloud=is_cloud,
            discord_ids=ids,
        )

        ts_env = make_ts_env()
        ranked_teams: List[List[StatRow]] = [[], []]
        best_quality = 0.0
        best_ids = ids.copy()

        for _ in range(settings.team_gen_tries):
            ranked_teams[0].clear()
            ranked_teams[1].clear()
            random.shuffle(ids)
            for index, discord_id in enumerate(ids):
                target_team = int(index * 2 / len(ids))
                stat_set = players_ranking.get(discord_id)
                if stat_set is None or stat_set.teamer is None:
                    ranked_teams[target_team].append(
                        StatRow(
                            mu=int(settings.ts_mu),
                            sigma=settings.ts_sigma,
                            games=0,
                            wins=0,
                            first=0,
                        )
                    )
                    continue
                ranked_teams[target_team].append(stat_set.teamer)

            game_quality = ts_env.quality(ranked_teams)
            # Deliberate (confirmed 2026-07-07): a new shuffle only replaces the current
            # pick when it beats it by at least team_gen_randomness, so the returned
            # split is within that margin of the best seen while favoring earlier random
            # shuffles — variety without giving up balance.
            if game_quality < best_quality + settings.team_gen_randomness:
                continue
            best_quality = game_quality
            best_ids = ids.copy()

        result: List[List[str]] = [[], []]
        for index, discord_id in enumerate(best_ids):
            result[int(index * 2 / len(best_ids))].append(discord_id)

        return TeamGenResponse(
            civ_version=version,
            game_type="cloud" if is_cloud else "realtime",
            game_quality=best_quality,
            teams=result,
        )

    async def _load_stat_set(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        is_cloud: bool,
        discord_ids: List[str],
    ) -> Dict[str, StatSet]:
        result: Dict[str, StatSet] = {
            discord_id: StatSet() for discord_id in discord_ids
        }

        tasks = [
            self.repository.get_player_stat_docs_batch(
                civ_version=civ_version,
                is_seasonal=is_seasonal,
                match_type=match_type,
                is_cloud=is_cloud,
                is_combined=False,
                discord_ids=discord_ids,
            )
            for match_type in ALLOWED_MATCH_TYPES
        ]

        docs_by_match_type = await asyncio.gather(*tasks)
        for match_type, docs in zip(
            ALLOWED_MATCH_TYPES, docs_by_match_type, strict=True
        ):
            for discord_id, doc in docs.items():
                if discord_id not in result:
                    continue

                row = self._doc_to_row(doc)
                current = result[discord_id]
                if match_type == "ffa":
                    current.ffa = row
                elif match_type == "teamer":
                    current.teamer = row
                else:
                    current.duel = row
        return result

    async def _reset_stat_set(
        self,
        *,
        civ_version: str,
        is_cloud: bool,
        discord_id: str,
    ):

        await self.repository.reset_player_stat_doc(
            civ_version=civ_version,
            is_cloud=is_cloud,
            discord_id=discord_id,
        )

        return
