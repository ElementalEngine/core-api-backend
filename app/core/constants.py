from __future__ import annotations

# Not settings.mongo_db_name: that names civ_match_reporter on dev, a database
# the cluster does not have, and which no repository reads. Entry 3's rename
# edits this line.
GAMES_DB = "match_reporter"

COL_CIV_DATA = "civ_data"
COL_RATING_EVENTS = "rating_events"
COL_STAT_RESETS = "stat_resets"
COL_SUB_EVENTS = "sub_events"

__all__ = [
    "COL_CIV_DATA",
    "COL_RATING_EVENTS",
    "COL_STAT_RESETS",
    "COL_SUB_EVENTS",
    "GAMES_DB",
]
