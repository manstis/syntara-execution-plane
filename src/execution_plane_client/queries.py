"""Persistence-neutral command query contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import HandlingResult
    from execution_plane_client.protocol import (
        ChildPage,
        CommandRead,
        CommandScope,
        CommandStatus,
        MessageEnvelope,
        ResourceReference,
    )


class CommandQueryProvider(Protocol):
    """Read command state without prescribing the participant's persistence layer."""

    async def get(
        self,
        scope: CommandScope,
        command_id: UUID,
        *,
        peer_id: str,
    ) -> CommandRead:
        """Read safe command status within the authenticated peer and client scope."""
        ...

    async def children(
        self,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
        limit: int = 50,
        *,
        peer_id: str,
        include_recovery: bool = False,
    ) -> ChildPage:
        """Read a bounded, scoped page of child outcomes."""
        ...


class SessionCommandQueryProvider(Protocol):
    """Read Journal-backed projections inside a caller-owned transaction."""

    async def get(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        *,
        peer_id: str | None = None,
    ) -> CommandRead:
        """Read scoped command state without opening or committing a session."""
        ...

    async def children(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
        limit: int = 50,
        *,
        peer_id: str | None = None,
        include_recovery: bool = False,
    ) -> ChildPage:
        """Read a bounded page of child outcomes within the caller transaction."""
        ...


class CommandCancellationProvider(Protocol):
    """Expose an accepted cancellation intent to the command receiver."""

    async def is_cancelled(
        self,
        scope: CommandScope,
        command_id: UUID,
        *,
        session: AsyncSession | None = None,
    ) -> bool:
        """Return whether this exact client and scope cancelled the command."""
        ...


class CommandStateProvider(CommandQueryProvider, CommandCancellationProvider, Protocol):
    """Query accepted commands and maintain the participant's local projection."""

    def record_received(self, message: MessageEnvelope, *, local_id: str) -> bool:
        """Record an inbound identity once and report whether it is new."""
        ...

    def has_received(self, message: MessageEnvelope) -> bool:
        """Return whether this exact message identity already completed receipt."""
        ...

    def mark_status(
        self,
        scope: CommandScope,
        command_id: UUID,
        status: CommandStatus,
        *,
        resource: ResourceReference | None = None,
        error_code: str | None = None,
    ) -> None:
        """Update the participant-owned command projection."""
        ...

    def append_child(self, original: MessageEnvelope, child: MessageEnvelope, result: HandlingResult) -> None:
        """Record a completion child and update its parent projection."""
        ...
