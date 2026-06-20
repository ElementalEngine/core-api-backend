from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Final

from motor.motor_asyncio import AsyncIOMotorClient

from app.features.infractions.errors import NotSuspendedError
from app.features.infractions.models import (
    ActiveSuspension,
    FlatSuspensionResponse,
    FlatType,
    ModifyDaysResponse,
    PendingSuspensionResponse,
    RemoveTierResponse,
    SuspensionDocument,
    SuspensionRecordResponse,
    TierCategory,
    TierInfractionResponse,
)
from app.features.infractions.repository import (
    create_pending_suspension as _repo_create_pending,
    delete_pending_suspension as _repo_delete_pending,
    find_or_create_suspension,
    find_pending_suspension as _repo_find_pending,
    find_suspension,
    get_active_suspensions as _repo_get_active_suspensions,
    get_overdue_suspensions as _repo_get_overdue_suspensions,
    upsert_suspension,
)

# ─── Domain constants ─────────────────────────────────────────────────────────
# Must match src/config/constants.ts in the LJ bot exactly.

TIER_CAPS: Final[dict[str, int]] = {
    "quit":     6,
    "minor":    7,
    "moderate": 6,
    "major":    4,
    "extreme":  2,
}

# Days per tier, 1-indexed. Length = cap - 1 (ban tier has no duration).
TIER_DURATIONS: Final[dict[str, list[int]]] = {
    "quit":     [1, 3, 7, 14, 30],         # T6 = ban threshold
    "minor":    [0, 1, 2, 4, 7, 14],       # T1 = warning (0 days); T7 = ban threshold
    "moderate": [1, 4, 7, 14, 30],         # T6 = ban threshold
    "major":    [7, 14, 30],               # T4 = ban threshold
    "extreme":  [30],                      # T2 = ban threshold
}

DECAY_DAYS: Final[dict[str, int]] = {
    "quit":     90,
    "minor":    90,
    "moderate": 90,
    "major":    90,
    "extreme":  1460,   # 4 years
}

FLAT_DAYS: Final[dict[str, int]] = {
    "smurf":   30,
    "oversub": 3,
    "comp":    7,
}


# ─── Private helpers ──────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _get_infraction(record: SuspensionDocument, category: TierCategory) -> Any:
    return getattr(record, category.value)


def _current_end_or_now(record: SuspensionDocument, now: datetime) -> datetime:
    """Base date for stacking suspension days — active end or now, whichever is later."""
    return record.ends if (record.ends and record.ends > now) else now


# ─── Tier infractions ─────────────────────────────────────────────────────────

async def record_tier_infraction(
    db: AsyncIOMotorClient,
    discord_id: str,
    category: TierCategory,
    reason: str | None,
    suspended_roles: list[str],
) -> TierInfractionResponse:
    record = await find_or_create_suspension(db, discord_id)
    was_already_suspended = record.suspended

    current_tier: int = _get_infraction(record, category).tier
    new_tier: int = min(current_tier + 1, TIER_CAPS[category.value])
    is_ban_threshold: bool = new_tier >= TIER_CAPS[category.value]
    is_warning_only: bool = (category == TierCategory.minor and new_tier == 1)

    # At ban threshold new_tier exceeds the durations list — 0 days added (old bot behaviour).
    durations = TIER_DURATIONS[category.value]
    days_added: int = durations[new_tier - 1] if new_tier <= len(durations) else 0

    now = _utcnow()
    current_end = _current_end_or_now(record, now)
    new_ends: datetime | None = (
        None if is_warning_only else current_end + timedelta(days=days_added)
    )
    new_decays = now + timedelta(days=DECAY_DAYS[category.value])
    suspended: bool = not is_warning_only

    update: dict[str, Any] = {
        f"{category.value}.tier":   new_tier,
        f"{category.value}.decays": new_decays,
        "suspended": suspended,
        "ends":      new_ends,
    }
    if suspended:
        update["active_category"] = category.value
        if not was_already_suspended:
            update["suspendedRoles"] = suspended_roles

    await upsert_suspension(db, discord_id, update)

    return TierInfractionResponse(
        discord_id=discord_id,
        category=category,
        tier=new_tier,
        days_added=days_added,
        ends=new_ends,
        suspended=suspended,
        is_ban_threshold=is_ban_threshold,
        is_warning_only=is_warning_only,
        active_category=category if suspended else None,
    )


# ─── Flat suspensions ─────────────────────────────────────────────────────────

async def record_flat_suspension(
    db: AsyncIOMotorClient,
    discord_id: str,
    flat_type: FlatType,
    reason: str | None,
    suspended_roles: list[str],
) -> FlatSuspensionResponse:
    record = await find_or_create_suspension(db, discord_id)
    was_already_suspended = record.suspended

    days_added = FLAT_DAYS[flat_type.value]
    now = _utcnow()
    current_end = _current_end_or_now(record, now)
    new_ends = current_end + timedelta(days=days_added)

    update: dict[str, Any] = {
        "suspended":       True,
        "ends":            new_ends,
        "active_category": "flat",
    }
    if not was_already_suspended:
        update["suspendedRoles"] = suspended_roles

    await upsert_suspension(db, discord_id, update)

    return FlatSuspensionResponse(
        discord_id=discord_id,
        type=flat_type,
        days_added=days_added,
        ends=new_ends,
        suspended=True,
    )


# ─── Day manipulation ─────────────────────────────────────────────────────────

async def add_days(db: AsyncIOMotorClient, discord_id: str, days: int) -> ModifyDaysResponse:
    record = await find_or_create_suspension(db, discord_id)

    now = _utcnow()
    current_end = _current_end_or_now(record, now)
    new_ends = current_end + timedelta(days=days)

    await upsert_suspension(db, discord_id, {
        "suspended": True,
        "ends":      new_ends,
    })

    return ModifyDaysResponse(discord_id=discord_id, days_delta=days, new_ends=new_ends)


async def remove_days(db: AsyncIOMotorClient, discord_id: str, days: int) -> ModifyDaysResponse:
    record = await find_suspension(db, discord_id)
    if record is None or record.ends is None:
        raise NotSuspendedError(discord_id)

    new_ends = record.ends - timedelta(days=days)

    await upsert_suspension(db, discord_id, {"ends": new_ends})

    return ModifyDaysResponse(discord_id=discord_id, days_delta=-days, new_ends=new_ends)


# ─── Tier management ─────────────────────────────────────────────────────────

async def remove_tier(
    db: AsyncIOMotorClient,
    discord_id: str,
    category: TierCategory,
) -> RemoveTierResponse:
    record = await find_or_create_suspension(db, discord_id)
    current_tier: int = _get_infraction(record, category).tier

    if current_tier <= 0:
        return RemoveTierResponse(
            discord_id=discord_id,
            category=category,
            new_tier=0,
            new_decays=None,
            was_changed=False,
        )

    new_tier = current_tier - 1
    now = _utcnow()
    new_decays: datetime | None = (
        now + timedelta(days=DECAY_DAYS[category.value]) if new_tier > 0 else None
    )

    await upsert_suspension(db, discord_id, {
        f"{category.value}.tier":   new_tier,
        f"{category.value}.decays": new_decays,
    })

    return RemoveTierResponse(
        discord_id=discord_id,
        category=category,
        new_tier=new_tier,
        new_decays=new_decays,
        was_changed=True,
    )


# ─── Unsuspend ────────────────────────────────────────────────────────────────

async def unsuspend(db: AsyncIOMotorClient, discord_id: str) -> None:
    await upsert_suspension(db, discord_id, {
        "suspended":       False,
        "ends":            None,
        "suspendedRoles":  [],
        "active_category": None,
    })


# ─── Record retrieval (triggers lazy decay) ───────────────────────────────────

async def get_record(db: AsyncIOMotorClient, discord_id: str) -> SuspensionRecordResponse:
    record = await find_or_create_suspension(db, discord_id)
    now = _utcnow()

    decay_updates: dict[str, Any] = {}
    for category in TierCategory:
        if category == record.active_category:
            continue
        infraction = _get_infraction(record, category)
        if infraction.tier > 0 and infraction.decays and now > infraction.decays:
            new_tier = max(infraction.tier - 1, 0)
            new_decays: datetime | None = (
                now + timedelta(days=DECAY_DAYS[category.value]) if new_tier > 0 else None
            )
            decay_updates[f"{category.value}.tier"]   = new_tier
            decay_updates[f"{category.value}.decays"] = new_decays

    if decay_updates:
        await upsert_suspension(db, discord_id, decay_updates)
        record = await find_suspension(db, discord_id) or record

    return SuspensionRecordResponse(
        discord_id=record.discord_id,
        suspended=record.suspended,
        ends=record.ends,
        suspended_roles=record.suspendedRoles,
        active_category=record.active_category,
    )


# ─── Scheduler recovery ───────────────────────────────────────────────────────

async def get_active_suspensions(db: AsyncIOMotorClient) -> list[ActiveSuspension]:
    return await _repo_get_active_suspensions(db)


async def get_overdue_suspensions(db: AsyncIOMotorClient) -> list[ActiveSuspension]:
    return await _repo_get_overdue_suspensions(db)


# ─── Pending suspensions ─────────────────────────────────────────────────────

async def get_pending_suspension(
    db: AsyncIOMotorClient, discord_id: str
) -> PendingSuspensionResponse | None:
    doc = await _repo_find_pending(db, discord_id)
    if doc is None:
        return None
    return PendingSuspensionResponse(
        discord_id=doc.id,
        punishment_type=doc.punishment_type,
        reason=doc.reason,
    )


async def create_pending_suspension(
    db: AsyncIOMotorClient,
    discord_id: str,
    punishment_type: str,
    reason: str | None,
) -> None:
    await _repo_create_pending(db, discord_id, punishment_type, reason)


async def delete_pending_suspension(db: AsyncIOMotorClient, discord_id: str) -> None:
    await _repo_delete_pending(db, discord_id)