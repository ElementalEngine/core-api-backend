"""Where a stat document lives: database and collection. One resolver serves the
stat write and its ledger event, so the two cannot disagree."""

from __future__ import annotations

DB_CIV6_LIFETIME = "civ6_lifetime_stats"
DB_CIV7_LIFETIME = "civ7_lifetime_stats"
DB_CIV6_SEASON = "civ6_season_stats"
DB_CIV7_SEASON = "civ7_season_stats"


def stats_db_name(*, civ_version: str, is_seasonal: bool) -> str:
    if civ_version == "civ6":
        return DB_CIV6_SEASON if is_seasonal else DB_CIV6_LIFETIME
    return DB_CIV7_SEASON if is_seasonal else DB_CIV7_LIFETIME


def stats_collection_name(*, match_type: str, is_cloud: bool, is_combined: bool) -> str:
    prefix = "pbc_" if is_cloud else "rt_"

    if is_combined:
        return f"{prefix}combined"

    mt = match_type.strip().lower()
    # Accept legacy alias but keep internal naming as 'teamer'.
    if mt == "team":
        mt = "teamer"

    if mt not in {"ffa", "teamer", "duel"}:
        raise ValueError(
            f"Unexpected match_type: {match_type!r} (expected ffa|teamer|duel)"
        )

    return f"{prefix}{mt}"


def drives_rank_roles(*, game_mode: str, is_cloud: bool) -> bool:
    """FFA and cloud games set rank roles; a realtime teamer or duel does not,
    though it is rated."""
    return is_cloud or game_mode.strip().lower() == "ffa"


def stat_scope(
    *,
    civ_version: str,
    is_seasonal: bool,
    match_type: str,
    is_cloud: bool,
    is_combined: bool,
) -> str:
    """The stat document's address, and the ledger's scope field."""
    db = stats_db_name(civ_version=civ_version, is_seasonal=is_seasonal)
    col = stats_collection_name(
        match_type=match_type, is_cloud=is_cloud, is_combined=is_combined
    )
    return f"{db}.{col}"


__all__ = [
    "DB_CIV6_LIFETIME",
    "DB_CIV6_SEASON",
    "DB_CIV7_LIFETIME",
    "DB_CIV7_SEASON",
    "drives_rank_roles",
    "stat_scope",
    "stats_collection_name",
    "stats_db_name",
]
