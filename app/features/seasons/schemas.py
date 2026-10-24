"""What the season routes answer."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class SeasonPreview(BaseModel):
    """The open season, what comes next, and the season records per collection
    that a change would save and clear."""

    edition: str
    label: str
    started_at: datetime
    next_label: str
    records: dict[str, int]


class StartNextSeason(BaseModel):
    """The season the caller saw open; the change happens only if it still is."""

    expected: str


class SeasonRolled(BaseModel):
    """A season change: what closed, what opened, and the backup folder's name."""

    edition: str
    closed: str
    opened: str
    started_at: datetime
    backup: str
    records: dict[str, int]
