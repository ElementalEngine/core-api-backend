from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from bson import ObjectId
from bson.int64 import Int64

EventType = Literal["approve", "revert", "reset"]


def build_match_event(
    *,
    event_type: EventType,
    match_id: ObjectId,
    match_created_at: datetime,
    occurred_at: datetime,
    discord_id: str,
    scope: str,
    mu_before: float,
    mu_after: float,
    sigma_before: float,
    sigma_after: float,
    applied_delta: float,
) -> dict[str, Any]:
    """One ledger event, for one player, in one scope."""
    return {
        "event_type": event_type,
        "match_id": match_id,
        "match_created_at": match_created_at,
        "occurred_at": occurred_at,
        "player_id": Int64(discord_id),
        "scope": scope,
        "mu_before": float(mu_before),
        "mu_after": float(mu_after),
        "sigma_before": float(sigma_before),
        "sigma_after": float(sigma_after),
        "applied_delta": float(applied_delta),
    }


def build_reset_event(
    *,
    occurred_at: datetime,
    discord_id: str,
    scope: str,
    mu_before: float,
    mu_after: float,
    sigma_before: float,
    sigma_after: float,
) -> dict[str, Any]:
    """A stat reset, for one player, in one scope."""
    return {
        "event_type": "reset",
        "occurred_at": occurred_at,
        "player_id": Int64(discord_id),
        "scope": scope,
        "mu_before": float(mu_before),
        "mu_after": float(mu_after),
        "sigma_before": float(sigma_before),
        "sigma_after": float(sigma_after),
        "applied_delta": float(mu_after) - float(mu_before),
    }


__all__ = ["EventType", "build_match_event", "build_reset_event"]
