"""Transport-neutral dispatch and query contracts for participant runtimes."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable
from uuid import UUID  # noqa: TC003 - Pydantic resolves these model fields at runtime.

from execution_plane_client.protocol import WireModel

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from execution_plane_client.protocol import (
        CapabilitiesRead,
        ChildPage,
        CommandRead,
        CommandScope,
        MessageEnvelope,
    )


class DispatchState(StrEnum):
    """Transport progress independent of the command's execution outcome."""

    QUEUED = "queued"
    DELIVERED = "delivered"


class DispatchReceipt(WireModel):
    """A transport's durable-queue or HTTP-delivery result for one message."""

    message_id: UUID
    state: DispatchState


class TransportReceipt(WireModel):
    """Inbound HTTP receipt returned after a participant records a message."""

    received: Literal[True]
    message_id: UUID
    duplicate: bool = False


class TransactionalDispatcher(Protocol):
    """Stage an outbound command alongside a participant-owned transaction."""

    async def stage(self, session: object, message: MessageEnvelope) -> Callable[[], Awaitable[None]]:
        """Return work to run after commit; staging itself must not commit the session."""
        ...


@runtime_checkable
class Transport(Protocol):
    """Outbound command dispatch and authenticated query operations."""

    async def send(self, message: MessageEnvelope) -> DispatchReceipt:
        """Dispatch an immutable command envelope."""
        ...

    async def get_command(self, peer_id: str, scope: CommandScope, command_id: UUID) -> CommandRead:
        """Read one scoped command status."""
        ...

    async def get_children(self, peer_id: str, scope: CommandScope, command_id: UUID, after: int = 0) -> ChildPage:
        """Read a bounded page of command children."""
        ...

    async def get_capabilities(self, peer_id: str, scope: CommandScope | None = None) -> CapabilitiesRead:
        """Read peer command capabilities."""
        ...

    async def get_resource(
        self,
        peer_id: str,
        scope: CommandScope,
        collection: str,
        *,
        resource_id: UUID | None = None,
        limit: int = 50,
    ) -> object:
        """Read a scoped participant resource."""
        ...

    async def aclose(self) -> None:
        """Release transport resources."""
        ...
