"""How often a player has subbed in over the last 30 days. Going over is
reported to staff; nothing here refuses."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

SUB_LIMIT = 3
SUB_WINDOW = timedelta(days=30)


class SubEvents(Protocol):
    async def count_sub_ins_since(self, discord_id: str, since: datetime) -> int: ...


async def sub_quota(
    events: SubEvents, discord_id: str, now: datetime | None = None
) -> dict[str, Any]:
    """Sub-ins in the rolling window, with the limit they are judged against."""
    since = (now or datetime.now(UTC)) - SUB_WINDOW
    return {
        "discord_id": discord_id,
        "count": await events.count_sub_ins_since(discord_id, since),
        "limit": SUB_LIMIT,
        "window_days": SUB_WINDOW.days,
    }


__all__ = ["SUB_LIMIT", "SUB_WINDOW", "SubEvents", "sub_quota"]
