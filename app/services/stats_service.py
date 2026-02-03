from __future__ import annotations

from typing import Dict, List, Tuple

from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings
from app.models.mongo_queries import MongoQueries
from app.models.schemas import StatRow, StatSet, UserStatsResponse


_ALLOWED_CIV_VERSIONS = {"civ6", "civ7"}
_ALLOWED_GAME_TYPES = {"realtime", "cloud"}
_ALLOWED_MATCH_TYPES = ("ffa", "teamer", "duel")


class StatsServiceError(Exception):
    pass


class InvalidStatsRequestError(StatsServiceError):
    pass


class StatsNotFoundError(StatsServiceError):
    pass


class StatsService:
    def __init__(self, client: AsyncIOMotorClient) -> None:
        self.q = MongoQueries(client)

    def _validate(self, civ_version: str, game_type: str) -> Tuple[str, bool]:
        v = (civ_version or "").strip().lower()
        gt = (game_type or "").strip().lower()

        if v not in _ALLOWED_CIV_VERSIONS:
            raise InvalidStatsRequestError("Invalid civ_version")
        if gt not in _ALLOWED_GAME_TYPES:
            raise InvalidStatsRequestError("Invalid game_type")

        return v, gt == "cloud"

    def _doc_to_row(self, doc: Dict) -> StatRow:
        mu_raw = float(doc.get("mu", settings.ts_mu))
        sigma_raw = float(doc.get("sigma", settings.ts_sigma))

        return StatRow(
            mu=int(round(mu_raw)),
            sigma=sigma_raw,
            games=int(doc.get("games", 0)),
            wins=int(doc.get("wins", 0)),
            first=int(doc.get("first", 0)),
            subbedIn=int(doc.get("subbedIn", 0)),
            subbedOut=int(doc.get("subbedOut", 0)),
            lastModified=doc.get("lastModified"),
        )

    @staticmethod
    def _has_any_stats(resp: UserStatsResponse) -> bool:
        for s in (resp.lifetime, resp.season):
            if s.ffa or s.teamer or s.duel:
                return True
        return False

    async def get_user_stats(self, *, civ_version: str, game_type: str, discord_id: str) -> UserStatsResponse:
        v, is_cloud = self._validate(civ_version, game_type)
        did = str(discord_id).strip()
        if not did:
            raise InvalidStatsRequestError("Missing discord_id")
        if not did.isdigit():
            raise InvalidStatsRequestError("Invalid discord_id")

        lifetime_map = await self._load_stat_set(
            civ_version=v,
            is_seasonal=False,
            is_cloud=is_cloud,
            discord_ids=[did],
        )

        # Cloud (pbc) does not have a season collection.
        if is_cloud:
            season_map: Dict[str, StatSet] = {did: StatSet()}
        else:
            season_map = await self._load_stat_set(
                civ_version=v,
                is_seasonal=True,
                is_cloud=is_cloud,
                discord_ids=[did],
            )

        resp = UserStatsResponse(
            discord_id=did,
            civ_version=v,
            game_type="cloud" if is_cloud else "realtime",
            lifetime=lifetime_map.get(did, StatSet()),
            season=season_map.get(did, StatSet()),
        )

        if not self._has_any_stats(resp):
            raise StatsNotFoundError("No stats found")

        return resp

    async def get_users_stats_batch(
        self, *, civ_version: str, game_type: str, discord_ids: List[str]
    ) -> List[UserStatsResponse]:
        v, is_cloud = self._validate(civ_version, game_type)

        # Normalize ids as strings; keep caller order.
        ids = [str(x).strip() for x in discord_ids if str(x).strip()]
        if not ids:
            return []
        if any((not did.isdigit()) for did in ids):
            raise InvalidStatsRequestError("Invalid discord_ids")

        lifetime_map = await self._load_stat_set(
            civ_version=v,
            is_seasonal=False,
            is_cloud=is_cloud,
            discord_ids=ids,
        )

        if is_cloud:
            season_map: Dict[str, StatSet] = {did: StatSet() for did in ids}
        else:
            season_map = await self._load_stat_set(
                civ_version=v,
                is_seasonal=True,
                is_cloud=is_cloud,
                discord_ids=ids,
            )

        out: List[UserStatsResponse] = []
        for did in ids:
            out.append(
                UserStatsResponse(
                    discord_id=did,
                    civ_version=v,
                    game_type="cloud" if is_cloud else "realtime",
                    lifetime=lifetime_map.get(did, StatSet()),
                    season=season_map.get(did, StatSet()),
                )
            )
        return out

    async def _load_stat_set(
        self,
        *,
        civ_version: str,
        is_seasonal: bool,
        is_cloud: bool,
        discord_ids: List[str],
    ) -> Dict[str, StatSet]:
        result: Dict[str, StatSet] = {did: StatSet() for did in discord_ids}

        for mt in _ALLOWED_MATCH_TYPES:
            docs = await self.q.get_player_stat_docs_batch(
                civ_version=civ_version,
                is_seasonal=is_seasonal,
                match_type=mt,
                is_cloud=is_cloud,
                is_combined=False,
                discord_ids=discord_ids,
            )

            for did, doc in docs.items():
                key = str(did)
                if key not in result:
                    continue

                row = self._doc_to_row(doc)
                current = result[key]
                if mt == "ffa":
                    current.ffa = row
                elif mt == "teamer":
                    current.teamer = row
                else:
                    current.duel = row
        return result
