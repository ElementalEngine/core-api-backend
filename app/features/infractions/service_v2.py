"""Lady Justice's v2 rules. A change is worked out from the record as read and
written only if its `rev` has not moved since, so two changes cannot both apply."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

from app.features.infractions.errors import (
    ChangeConflict,
    NoSuspension,
    StillSuspended,
    SuspensionHeld,
)
from app.features.infractions.models import FlatType, SuspensionDocument, TierCategory
from app.features.infractions.repository import (
    create_pending_suspension,
    suspensions_col,
    suspensions_due_col,
)
from app.features.infractions.schemas_v2 import (
    DaysResult,
    DueSuspensions,
    FlatResult,
    InfractionBody,
    PendingApplied,
    SuspensionCleared,
    TierRemoved,
    TierResult,
)
from app.features.infractions.service import (
    DECAY_DAYS,
    FLAT_DAYS,
    TIER_CAPS,
    TIER_DURATIONS,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
Work = Callable[[SuspensionDocument, datetime], tuple[dict[str, Any], T]]

ATTEMPTS = 3
DUE_LIMIT = 100


def now_utc() -> datetime:
    return datetime.now(UTC)


def decay(doc: SuspensionDocument, now: datetime) -> dict[str, Any]:
    """A tier off for each full period since it was due, keeping the clock; the
    category being served does not decay."""
    updates: dict[str, Any] = {}
    for category in TierCategory:
        if doc.suspended and category == doc.active_category:
            continue
        record = getattr(doc, category.value)
        if record.tier <= 0 or record.decays is None or now < record.decays:
            continue
        period = timedelta(days=DECAY_DAYS[category.value])
        steps = 1 + (now - record.decays) // period
        tier = max(record.tier - steps, 0)
        updates[f"{category.value}.tier"] = tier
        updates[f"{category.value}.decays"] = (
            record.decays + steps * period if tier else None
        )
    return updates


def _decayed(doc: SuspensionDocument, updates: dict[str, Any]) -> SuspensionDocument:
    data = doc.model_dump()
    for key, value in updates.items():
        category, field = key.split(".")
        data[category][field] = value
    return SuspensionDocument.model_validate(data)


def _serving(doc: SuspensionDocument, now: datetime) -> bool:
    """Suspended and not yet past its end; an overdue one is over, as Auth reads it."""
    return doc.suspended and (doc.ends is None or doc.ends > now)


def _held(doc: SuspensionDocument) -> bool:
    return doc.suspended and doc.ends is None


def _start(doc: SuspensionDocument, now: datetime) -> datetime:
    """Days stack on a running suspension's end, else on now."""
    return doc.ends if doc.ends and doc.ends > now else now


def _restart(doc: SuspensionDocument, now: datetime) -> dict[str, Any]:
    """A category stops being served: its decay clock starts again from now."""
    served = doc.active_category
    if not doc.suspended or served not in TIER_CAPS:
        return {}
    if getattr(doc, str(served)).tier <= 0:
        return {}
    return {f"{served}.decays": now + timedelta(days=DECAY_DAYS[str(served)])}


def _suspend(
    doc: SuspensionDocument,
    ends: datetime | None,
    category: str,
    roles: list[str],
    now: datetime,
) -> dict[str, Any]:
    changes: dict[str, Any] = {
        "suspended": True,
        "ends": ends,
        "active_category": category,
    }
    if doc.active_category != category:
        changes |= _restart(doc, now)
    if not doc.suspended:
        changes["suspendedRoles"] = roles
    return changes


class Infractions:
    def __init__(self, client: AsyncMongoClient) -> None:  # type: ignore[type-arg]
        self._client = client
        self._col = suspensions_col(client)
        self._due = suspensions_due_col(client)

    async def _change(self, discord_id: str, work: Work[T]) -> T:
        for _ in range(ATTEMPTS):
            raw: dict[str, Any] | None = await self._col.find_one(
                {"discord_id": discord_id}
            )
            doc = SuspensionDocument.model_validate(raw or {"discord_id": discord_id})
            rev = int((raw or {}).get("rev") or 0)
            now = now_utc()
            fresh = decay(doc, now)
            changes, result = work(_decayed(doc, fresh), now)
            update = {**fresh, **changes}
            if not update:
                return result
            match: dict[str, Any] = {
                "discord_id": discord_id,
                "rev": rev or {"$in": [None, 0]},
            }
            try:
                written = await self._col.update_one(
                    match, {"$set": update, "$inc": {"rev": 1}}, upsert=raw is None
                )
            except DuplicateKeyError:
                continue
            if written.matched_count or written.upserted_id is not None:
                return result
        raise ChangeConflict()

    async def tier(
        self,
        discord_id: str,
        category: TierCategory,
        body: InfractionBody,
        actor: str,
    ) -> TierResult:
        roles = body.suspended_roles
        cap = TIER_CAPS[category.value]
        durations = TIER_DURATIONS[category.value]

        def work(
            doc: SuspensionDocument, now: datetime
        ) -> tuple[dict[str, Any], TierResult]:
            tier = min(getattr(doc, category.value).tier + 1, cap)
            threshold = tier >= cap
            warning = category == TierCategory.minor and tier == 1
            changes: dict[str, Any] = {
                f"{category.value}.tier": tier,
                f"{category.value}.decays": now
                + timedelta(days=DECAY_DAYS[category.value]),
            }
            days = 0
            if warning or _held(doc):
                ends = doc.ends
            elif threshold:
                ends = None
                changes |= _suspend(doc, None, category.value, roles, now)
            else:
                days = durations[tier - 1]
                ends = _start(doc, now) + timedelta(days=days)
                changes |= _suspend(doc, ends, category.value, roles, now)
            suspended = "suspended" in changes or _serving(doc, now)
            return changes, TierResult(
                discord_id=discord_id,
                category=category,
                tier=tier,
                days_added=days,
                ends=ends,
                suspended=suspended,
                held=suspended and ends is None,
                is_ban_threshold=threshold,
                is_warning_only=warning,
            )

        result = await self._change(discord_id, work)
        logger.info(
            "Infraction tier discord_id=%s actor=%s category=%s tier=%s ends=%s reason=%r",
            discord_id,
            actor,
            category.value,
            result.tier,
            result.ends,
            body.reason,
        )
        return result

    async def flat(
        self, discord_id: str, flat_type: FlatType, body: InfractionBody, actor: str
    ) -> FlatResult:
        roles = body.suspended_roles

        def work(
            doc: SuspensionDocument, now: datetime
        ) -> tuple[dict[str, Any], FlatResult]:
            if _held(doc):
                raise SuspensionHeld()
            days = FLAT_DAYS[flat_type.value]
            ends = _start(doc, now) + timedelta(days=days)
            return _suspend(doc, ends, "flat", roles, now), FlatResult(
                discord_id=discord_id,
                type=flat_type,
                days_added=days,
                ends=ends,
            )

        result = await self._change(discord_id, work)
        logger.info(
            "Infraction flat discord_id=%s actor=%s type=%s ends=%s reason=%r",
            discord_id,
            actor,
            flat_type.value,
            result.ends,
            body.reason,
        )
        return result

    async def days(self, discord_id: str, days: int, actor: str) -> DaysResult:
        def work(
            doc: SuspensionDocument, now: datetime
        ) -> tuple[dict[str, Any], DaysResult]:
            if not _serving(doc, now):
                raise NoSuspension()
            if doc.ends is None:
                raise SuspensionHeld()
            ends = doc.ends + timedelta(days=days)
            return {"ends": ends}, DaysResult(
                discord_id=discord_id, days=days, ends=ends, suspended=ends > now
            )

        result = await self._change(discord_id, work)
        logger.info(
            "Infraction days discord_id=%s actor=%s days=%s ends=%s",
            discord_id,
            actor,
            days,
            result.ends,
        )
        return result

    async def remove_tier(
        self, discord_id: str, category: TierCategory, actor: str
    ) -> TierRemoved:
        def work(
            doc: SuspensionDocument, now: datetime
        ) -> tuple[dict[str, Any], TierRemoved]:
            if _serving(doc, now):
                raise StillSuspended()
            current = getattr(doc, category.value).tier
            if current <= 0:
                return {}, TierRemoved(
                    discord_id=discord_id, category=category, tier=0, was_changed=False
                )
            tier = current - 1
            decays = now + timedelta(days=DECAY_DAYS[category.value]) if tier else None
            return {
                f"{category.value}.tier": tier,
                f"{category.value}.decays": decays,
            }, TierRemoved(
                discord_id=discord_id, category=category, tier=tier, was_changed=True
            )

        result = await self._change(discord_id, work)
        logger.info(
            "Infraction remove-tier discord_id=%s actor=%s category=%s tier=%s",
            discord_id,
            actor,
            category.value,
            result.tier,
        )
        return result

    async def _clear(self, discord_id: str, due_only: bool) -> SuspensionCleared:
        """Ends a suspension and restarts the served category's decay clock. The
        roles stay on the record, so a repeated call still returns them."""

        def work(
            doc: SuspensionDocument, now: datetime
        ) -> tuple[dict[str, Any], SuspensionCleared]:
            roles = list(doc.suspendedRoles)
            due = doc.ends is not None and doc.ends <= now
            if not doc.suspended or (due_only and not due):
                return {}, SuspensionCleared(
                    discord_id=discord_id, cleared=False, suspended_roles=roles
                )
            changes: dict[str, Any] = {
                "suspended": False,
                "ends": None,
                "active_category": None,
                **_restart(doc, now),
            }
            return changes, SuspensionCleared(
                discord_id=discord_id, cleared=True, suspended_roles=roles
            )

        return await self._change(discord_id, work)

    async def unsuspend(
        self, discord_id: str, reason: str | None, actor: str
    ) -> SuspensionCleared:
        result = await self._clear(discord_id, due_only=False)
        logger.info(
            "Infraction unsuspend discord_id=%s actor=%s cleared=%s reason=%r",
            discord_id,
            actor,
            result.cleared,
            reason,
        )
        return result

    async def expire(self, discord_id: str) -> SuspensionCleared:
        """Ends a suspension only once its end has passed, so a fresh extension
        is never undone and a second call changes nothing."""
        result = await self._clear(discord_id, due_only=True)
        if result.cleared:
            logger.info("Infraction expired discord_id=%s", discord_id)
        return result

    async def due(self) -> DueSuspensions:
        now = now_utc()
        cursor = (
            self._col.find(
                {"suspended": True, "ends": {"$lte": now}},
                {"discord_id": 1, "_id": 0},
            )
            .sort("ends", 1)
            .limit(DUE_LIMIT)
        )
        ids = [str(doc["discord_id"]) for doc in await cursor.to_list()]
        upcoming: dict[str, Any] | None = await self._col.find_one(
            {"suspended": True, "ends": {"$gt": now}},
            {"ends": 1, "_id": 0},
            sort=[("ends", 1)],
        )
        return DueSuspensions(
            discord_ids=ids, next_ends=upcoming["ends"] if upcoming else None
        )

    async def set_pending(
        self,
        discord_id: str,
        punishment: TierCategory | FlatType,
        reason: str | None,
        actor: str,
    ) -> None:
        await create_pending_suspension(
            self._client, discord_id, punishment.value, reason
        )
        logger.info(
            "Infraction pending discord_id=%s actor=%s type=%s",
            discord_id,
            actor,
            punishment.value,
        )

    async def apply_pending(self, discord_id: str, roles: list[str]) -> PendingApplied:
        """On joining: keep the roles the bot removes, then close the entry."""
        entry: dict[str, Any] | None = await self._due.find_one({"_id": discord_id})
        if entry is None:
            return PendingApplied(
                discord_id=discord_id,
                applied=False,
                punishment_type=None,
                reason=None,
                ends=None,
                held=False,
            )

        def work(
            doc: SuspensionDocument, now: datetime
        ) -> tuple[dict[str, Any], tuple[bool, datetime | None]]:
            if not _serving(doc, now):
                return {}, (False, None)
            kept = list(dict.fromkeys([*doc.suspendedRoles, *roles]))
            return {"suspendedRoles": kept}, (True, doc.ends)

        applied, ends = await self._change(discord_id, work)
        # A newer entry written meanwhile is left for the next join; null matches
        # an older entry stored without a time.
        await self._due.delete_one(
            {"_id": discord_id, "created_at": entry.get("created_at")}
        )
        logger.info(
            "Infraction pending applied discord_id=%s applied=%s", discord_id, applied
        )
        return PendingApplied(
            discord_id=discord_id,
            applied=applied,
            punishment_type=str(entry.get("punishment_type")),
            reason=entry.get("reason"),
            ends=ends,
            held=applied and ends is None,
        )
