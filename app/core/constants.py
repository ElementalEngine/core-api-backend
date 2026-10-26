from __future__ import annotations

GAMES_DB = "games"

COL_CIV_DATA = "civ_data"
COL_MATCH_SETUPS = "match_setups"
COL_RATING_EVENTS = "rating_events"
COL_SEASONS = "seasons"
COL_STAT_RESETS = "stat_resets"
COL_SUB_EVENTS = "sub_events"

# The shared members database. Auth owns its indexes; infractions and
# matches read the same collection.
DB_SERVER_MEMBERS = "server_members"
COL_USERS = "users"

__all__ = [
    "COL_CIV_DATA",
    "COL_MATCH_SETUPS",
    "COL_RATING_EVENTS",
    "COL_SEASONS",
    "COL_STAT_RESETS",
    "COL_SUB_EVENTS",
    "COL_USERS",
    "DB_SERVER_MEMBERS",
    "GAMES_DB",
]
