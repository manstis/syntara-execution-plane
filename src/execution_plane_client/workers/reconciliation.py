"""Status-first observation and recovery of original peer completion envelopes."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, or_, select, update

from execution_plane_client.errors import (
    CommandNotFound,
    PermanentRejection,
    ProtocolConflict,
    RetryableDelivery,
    UnauthorizedPeer,
    UnsafeDestination,
)
from execution_plane_client.journal.records import JournalEntry
from execution_plane_client.security import AuthenticatedIdentity

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.ingress import CommandReceiptHandler
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.observers import ExchangeObserver
    from execution_plane_client.protocol import ChildCommandRead, CommandScope, MessageEnvelope
    from execution_plane_client.security import PeerConfig
    from execution_plane_client.transport import HttpCommandTransport

_TERMINAL = ("succeeded", "failed", "cancelled", "superseded")


def _now() -> datetime:
    return datetime.now(UTC)


def _recovered_message(entry: JournalEntry, child: ChildCommandRead) -> MessageEnvelope:
    message = child.recovery_message
    if (
        message is None
        or message.command_id != child.command_id
        or message.command.parent_command_id != entry.command_id
        or message.command.parent_sequence != child.parent_sequence
        or message.scope != entry.scope
        or message.source_id != entry.destination_id
        or message.destination_id != entry.source_id
        or message.command.command_type != f"{entry.command_type}.completed"
    ):
        raise ProtocolConflict
    return message


class ReconciliationWorker:
    """Recover callbacks using authenticated reads; never manufacture message identity."""

    def __init__(
        self,
        store: ExchangeJournalStore,
        sessions: Callable[[], AsyncSession],
        transport: HttpCommandTransport,
        ingress: CommandReceiptHandler,
        peers: Mapping[str, PeerConfig],
        *,
        clock: Callable[[], datetime] = _now,
        poll_seconds: float = 5,
        batch_size: int = 10,
        observer: ExchangeObserver | None = None,
    ) -> None:
        """Configure read-only network observation with normal durable receipt on recovery."""
        self._store = store
        self._sessions = sessions
        self._transport = transport
        self._ingress = ingress
        self._peers = dict(peers)
        self._clock = clock
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size
        self._observer = observer

    async def reconcile_command(self, scope: CommandScope, command_id: UUID) -> None:
        """Recover one scoped caller-selected command through the normal ingress."""
        async with self._sessions() as session:
            entry = await self._store.get(session, scope, command_id, "outbound")
        if entry is None:
            raise CommandNotFound
        await self._observe(entry)

    async def _observe(self, entry: JournalEntry) -> None:
        try:
            status = await self._transport.get_command(entry.destination_id, entry.scope, entry.command_id)
            table = self._store.schema.table
            async with self._sessions() as session:
                latest = (
                    await session.scalar(
                        select(func.max(table.c.parent_sequence)).where(
                            table.c.parent_command_id == entry.command_id,
                            table.c.client_id == entry.client_id,
                            table.c.direction == "inbound",
                        )
                    )
                    or 0
                )
            if status.command_id != entry.command_id or status.latest_child_sequence <= latest:
                return
            page = await self._transport.get_children(entry.destination_id, entry.scope, entry.command_id, latest)
            for child in page.items:
                message = _recovered_message(entry, child)
                identity = AuthenticatedIdentity(
                    entry.destination_id,
                    entry.scope,
                    frozenset({message.command.command_type}),
                    self._peers[entry.destination_id].role,
                )
                # The peer query is authenticated by the transport's registered
                # HTTPS/mTLS destination. Normal receipt still validates parent,
                # routes, scope, catalog and immutable identity before accepting.
                async with self._sessions() as session, session.begin():
                    await self._ingress.receive(message, identity, session)
        except (RetryableDelivery, CommandNotFound):
            return
        except (ProtocolConflict, UnauthorizedPeer, UnsafeDestination, PermanentRejection):
            async with self._sessions() as session, session.begin():
                table = self._store.schema.table
                await session.execute(
                    update(table)
                    .where(
                        table.c.entry_id == entry.entry_id,
                        table.c.command_status.not_in(_TERMINAL),
                    )
                    .values(reconciliation_required=True, reconciliation_error_code="peer_recovery_rejected")
                )

    async def run_once(self) -> int:
        """Schedule bounded observations under locks and perform HTTP after commit."""
        table = self._store.schema.table
        now = self._clock()
        async with self._sessions() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        select(table)
                        .where(
                            table.c.direction == "outbound",
                            table.c.command_type != "command.accepted",
                            ~table.c.command_type.endswith(".completed"),
                            or_(table.c.command_status.not_in(_TERMINAL), table.c.reconciliation_required),
                            or_(table.c.delivery_state != "rejected", table.c.reconciliation_required),
                            or_(table.c.reconciliation_due_at.is_(None), table.c.reconciliation_due_at <= now),
                            # Local rejection handlers own reconciling external actor effects.
                            ~and_(
                                table.c.handling_state == "reconciling", table.c.prepared_effect_ciphertext.is_not(None)
                            ),
                        )
                        .order_by(
                            table.c.reconciliation_due_at.asc().nulls_first(), table.c.created_at, table.c.entry_id
                        )
                        .with_for_update(skip_locked=True)
                        .limit(self._batch_size)
                    )
                )
                .mappings()
                .all()
            )
            entries = [JournalEntry.model_validate(dict(row)) for row in rows]
            for entry in entries:
                await session.execute(
                    update(table)
                    .where(table.c.entry_id == entry.entry_id)
                    .values(
                        reconciliation_due_at=now + timedelta(seconds=self._poll_seconds),
                    )
                )
        await asyncio.gather(*(self._observe(entry) for entry in entries))
        if self._observer is not None:
            async with self._sessions() as session:
                backlog = await session.scalar(
                    select(func.count())
                    .select_from(table)
                    .where(or_(table.c.reconciliation_required, table.c.handling_state == "reconciling"))
                )
                pending = await session.scalar(
                    select(func.count())
                    .select_from(table)
                    .where(table.c.direction == "outbound", table.c.delivery_state.in_(["queued", "retrying", "sent"]))
                )
            self._observer.record(
                "reconciliation.backlog",
                {"unresolved": backlog or 0, "outbound_pending": pending or 0, "observations": len(entries)},
            )
        return len(entries)

    async def run(self) -> None:
        """Poll missing outcomes until participant shutdown."""
        while True:
            await self.run_once()
            await asyncio.sleep(self._poll_seconds)
