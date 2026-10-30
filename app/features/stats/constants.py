from app.core.constants import EDITIONS
from app.features.stats.errors import InvalidStatsRequestError

GAME_TYPES = ("realtime", "cloud")
MATCH_TYPES = ("ffa", "teamer", "duel")


def scope_of(civ_version: str, game_type: str) -> tuple[str, bool]:
    """The edition and whether it is cloud, as a stats request names them."""
    version = (civ_version or "").strip().lower()
    kind = (game_type or "").strip().lower()
    if version not in EDITIONS:
        raise InvalidStatsRequestError("Invalid civ_version")
    if kind not in GAME_TYPES:
        raise InvalidStatsRequestError("Invalid game_type")
    return version, kind == "cloud"


__all__ = ["GAME_TYPES", "MATCH_TYPES", "scope_of"]
