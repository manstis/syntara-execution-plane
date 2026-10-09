"""Journal-atomic allocation and persistence of completion children."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func, select

from execution_plane_client.completion import completion_message
from execution_plane_client.errors import CommandNotFound, ProtocolConflict

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import HandlingResult
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_transport_journal.store import ExchangeJournalStore


async def append_completion(
    store: ExchangeJournalStore,
    session: AsyncSession,
    original: MessageEnvelope,
    result: HandlingResult,
    *,
    occurred_at: datetime | None = None,
) -> MessageEnvelope:
    """Allocate an outcome sequence under the parent lock and append before commit."""
    table = store.schema.table
    parent = await session.scalar(
        select(table.c.entry_id)
        .where(
            table.c.command_id == original.command_id,
            table.c.client_id == original.scope.client_id,
            table.c.direction == "inbound",
        )
        .with_for_update()
    )
    if parent is None:
        raise CommandNotFound
    if result.status is None or original.command.command_type.endswith(".completed"):
        raise ProtocolConflict
    latest = await session.scalar(
        select(func.max(table.c.parent_sequence)).where(
            table.c.parent_command_id == original.command_id,
            table.c.client_id == original.scope.client_id,
            table.c.direction == "outbound",
        )
    )
    sequence = (latest or 0) + 1
    message = completion_message(original, result, sequence=sequence, occurred_at=occurred_at)
    await store.append_outbound(session, message)
    return message
