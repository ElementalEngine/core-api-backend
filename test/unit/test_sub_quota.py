from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from app.features.matches.sub_quota import SUB_LIMIT, SUB_WINDOW, sub_quota

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)


class FakeEvents:
    def __init__(self, count: int) -> None:
        self.count = count
        self.asked: list[tuple[str, datetime]] = []

    async def count_sub_ins_since(self, discord_id: str, since: datetime) -> int:
        self.asked.append((discord_id, since))
        return self.count


def test_the_league_allows_three_sub_ins_in_thirty_days():
    assert SUB_LIMIT == 3
    assert SUB_WINDOW == timedelta(days=30)


def test_the_count_is_bounded_by_the_rolling_window():
    events = FakeEvents(2)
    out = asyncio.run(sub_quota(events, "123456789012345678", now=NOW))
    assert out == {
        "discord_id": "123456789012345678",
        "count": 2,
        "limit": 3,
        "window_days": 30,
    }
    assert events.asked == [("123456789012345678", NOW - timedelta(days=30))]
