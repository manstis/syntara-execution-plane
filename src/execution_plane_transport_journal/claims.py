"""Independent journal leases with fenced post-request state transitions."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Literal

from sqlalchemy import and_, case, func, or_, select, update

from execution_plane_transport_journal.notifications import notify_journal
from execution_plane_transport_journal.records import JournalEntry

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_transport_journal.store import ExchangeJournalStore


@dataclass(frozen=True)
class JournalClaim:
    """An external attempt authorized only for one lease generation."""

    entry_id: UUID
    owner_id: UUID
    generation: int
    message: MessageEnvelope
    attempt: int


@dataclass(frozen=True)
class DeliveryOutcome:
    """Safe delivery classification, excluding arbitrary HTTP bodies/errors."""

    kind: Literal["receipt", "retry", "rejected"]
    error_code: str | None = None


@dataclass(frozen=True)
class HandlingClaim(JournalClaim):
    """An independent local claim, preserving uncertain prepared-effect identity."""

    direction: Literal["inbound", "outbound"]
    reconcile: bool
    prepared_effect_ciphertext: str | None
    error_code: str | None
    observe: bool = False


class JournalClaims:
    """Claim and transition rows within explicit short participant transactions."""

    def __init__(
        self,
        store: ExchangeJournalStore,
        *,
        lease_seconds: float = 30,
        retry_base_seconds: float = 1,
        retry_max_seconds: float = 60,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        """Keep transport deadlines distinct from lease and execution deadlines."""
        self.store = store
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self._jitter = jitter

    def retry_delay(self, attempt: int) -> float:
        """Return bounded exponential retry delay with injected jitter."""
        delay = self.retry_base_seconds * float(1 << min(max(attempt - 1, 0), 20))
        return min(self.retry_max_seconds, delay) * (0.5 + 0.5 * self._jitter())

    async def next_delivery_at(self, session: AsyncSession) -> datetime | None:
        """Return the earliest queued or expired-lease delivery deadline."""
        table = self.store.schema.table
        due = case(
            (table.c.delivery_state.in_(["queued", "retrying"]), table.c.delivery_due_at),
            (table.c.delivery_state == "sending", table.c.delivery_lease_until),
        )
        result: datetime | None = await session.scalar(
            select(func.min(due)).where(
                table.c.direction == "outbound", table.c.delivery_state.in_(["queued", "retrying", "sending"])
            )
        )
        return result

    async def next_handling_at(self, session: AsyncSession) -> datetime | None:
        """Return the earliest durable handler, observation or reconciliation deadline."""
        table = self.store.schema.table
        due = case(
            (table.c.handling_state.in_(["pending", "deferred"]), table.c.handling_due_at),
            (table.c.handling_state.in_(["processing", "prepared"]), table.c.handling_lease_until),
            (
                table.c.handling_state == "reconciling",
                case(
                    (table.c.handling_owner.is_(None), table.c.reconciliation_due_at),
                    else_=func.greatest(table.c.reconciliation_due_at, table.c.handling_lease_until),
                ),
            ),
        )
        result: datetime | None = await session.scalar(
            select(func.min(due)).where(
                table.c.handling_state.in_(["pending", "deferred", "processing", "prepared", "reconciling"])
            )
        )
        return result

    async def claim_due_delivery(
        self,
        session: AsyncSession,
        owner_id: UUID,
        now: datetime,
        limit: int,
    ) -> list[JournalClaim]:
        """Lock due sends and expired in-flight sends."""
        table = self.store.schema.table
        due = or_(
            and_(table.c.delivery_state.in_(["queued", "retrying"]), table.c.delivery_due_at <= now),
            and_(table.c.delivery_state == "sending", table.c.delivery_lease_until <= now),
        )
        rows = (
            (
                await session.execute(
                    select(table)
                    .where(table.c.direction == "outbound", due)
                    .order_by(table.c.created_at, table.c.entry_id)
                    .with_for_update(skip_locked=True)
                    .limit(limit),
                )
            )
            .mappings()
            .all()
        )
        claims = []
        for row in rows:
            entry = JournalEntry.model_validate(dict(row))
            generation = entry.delivery_generation + 1
            attempt = entry.delivery_attempts + 1
            await session.execute(
                update(table)
                .where(table.c.entry_id == entry.entry_id)
                .values(
                    delivery_state="sending",
                    delivery_owner=owner_id,
                    delivery_generation=generation,
                    delivery_attempts=attempt,
                    delivery_lease_until=now + timedelta(seconds=self.lease_seconds),
                    updated_at=now,
                )
            )
            message = await self.store.load_message(session, entry.entry_id)
            claims.append(JournalClaim(entry.entry_id, owner_id, generation, message, attempt))
        return claims

    async def record_delivery(
        self,
        session: AsyncSession,
        claim: JournalClaim,
        outcome: DeliveryOutcome,
        now: datetime,
    ) -> bool:
        """Apply a response only while this generation still owns the send."""
        table = self.store.schema.table
        values: dict[str, object] = {
            "delivery_owner": None,
            "delivery_lease_until": None,
            "updated_at": now,
            "delivery_error_code": outcome.error_code,
        }
        if outcome.kind == "receipt":
            values.update(delivery_state="sent", sent_at=now, delivery_due_at=None)
        elif outcome.kind == "retry":
            values.update(
                delivery_state="retrying", delivery_due_at=now + timedelta(seconds=self.retry_delay(claim.attempt))
            )
        else:
            values.update(
                delivery_state="rejected", delivery_due_at=None, handling_state="pending", handling_due_at=now
            )
        changed = await session.execute(
            update(table)
            .where(
                table.c.entry_id == claim.entry_id,
                table.c.delivery_owner == claim.owner_id,
                table.c.delivery_generation == claim.generation,
            )
            .values(**values)
            .returning(table.c.entry_id)
        )
        applied = changed.scalar_one_or_none() is not None
        if applied:
            await notify_journal(session, self.store.schema, "handling" if outcome.kind == "rejected" else "delivery")
        return applied

    async def claim_due_handling(
        self,
        session: AsyncSession,
        owner_id: UUID,
        now: datetime,
        limit: int,
    ) -> list[HandlingClaim]:
        """Claim local work independently of delivery, retaining external uncertainty."""
        table = self.store.schema.table
        due = or_(
            and_(table.c.handling_state.in_(["pending", "deferred"]), table.c.handling_due_at <= now),
            and_(table.c.handling_state.in_(["processing", "prepared"]), table.c.handling_lease_until <= now),
            and_(
                table.c.handling_state == "reconciling",
                table.c.reconciliation_due_at <= now,
                or_(table.c.handling_owner.is_(None), table.c.handling_lease_until <= now),
            ),
        )
        due_at = case(
            (table.c.handling_state.in_(["pending", "deferred"]), table.c.handling_due_at),
            (table.c.handling_state.in_(["processing", "prepared"]), table.c.handling_lease_until),
            else_=table.c.reconciliation_due_at,
        )
        rows = (
            (
                await session.execute(
                    select(table)
                    .where(due)
                    .order_by(due_at, table.c.updated_at, table.c.created_at, table.c.entry_id)
                    .with_for_update(skip_locked=True)
                    .limit(limit),
                )
            )
            .mappings()
            .all()
        )
        claims = []
        for row in rows:
            entry = JournalEntry.model_validate(dict(row))
            generation = entry.handling_generation + 1
            attempt = entry.handling_attempts + 1
            reconcile = not entry.observation_pending and entry.handling_state in {"prepared", "reconciling"}
            await session.execute(
                update(table)
                .where(table.c.entry_id == entry.entry_id)
                .values(
                    handling_state="reconciling" if reconcile else "processing",
                    handling_owner=owner_id,
                    handling_generation=generation,
                    handling_attempts=attempt,
                    handling_lease_until=now + timedelta(seconds=self.lease_seconds),
                    updated_at=now,
                )
            )
            message = await self.store.load_message(session, entry.entry_id)
            claims.append(
                HandlingClaim(
                    entry.entry_id,
                    owner_id,
                    generation,
                    message,
                    attempt,
                    entry.direction,
                    reconcile,
                    entry.prepared_effect_ciphertext,
                    entry.delivery_error_code,
                    entry.observation_pending,
                )
            )
        return claims
