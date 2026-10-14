"""Participant-owned command handling, without database or HTTP dependencies."""

from dataclasses import dataclass
from typing import Protocol

from execution_plane_client.protocol import MessageEnvelope, ResourceReference


@dataclass(frozen=True)
class HandlingResult:
    """Optional resource created or affected by a command."""

    resource: ResourceReference | None = None


class CommandHandler[ContextT](Protocol):
    """A participant supplies its own authenticated execution context."""

    async def handle(self, message: MessageEnvelope, context: ContextT) -> HandlingResult:
        """Apply domain effects; the protocol makes no persistence guarantees."""
        ...
