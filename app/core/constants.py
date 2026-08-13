from __future__ import annotations

# Not settings.mongo_db_name: that names civ_match_reporter on dev, a database
# the cluster does not have, and which no repository reads. Entry 3's rename
# edits this line.
GAMES_DB = "match_reporter"

COL_RATING_EVENTS = "rating_events"

__all__ = ["COL_RATING_EVENTS", "GAMES_DB"]
