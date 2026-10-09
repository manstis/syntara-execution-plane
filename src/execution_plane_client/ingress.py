"""Persistence-neutral validation and handler resolution for received commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from execution_plane_client.errors import UnauthorizedPeer

if TYPE_CHECKING:
    from execution_plane_client.catalog import CommandCatalog, CommandDefinition
    from execution_plane_client.handlers import CommandRegistry, ExternalHandler, TransactionalHandler
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_client.security import AuthenticatedIdentity


@dataclass(frozen=True)
class ResolvedCommand:
    """Validated definition and optional local handler for a received envelope."""

    definition: CommandDefinition
    handler: TransactionalHandler | ExternalHandler | None


class CommandIngress:
    """Validate peer, scope, role and handler registration without persistence."""

    def __init__(
        self,
        catalog: CommandCatalog,
        registry: CommandRegistry,
        *,
        local_id: str,
        local_role: str,
    ) -> None:
        """Retain only the shared catalog and participant's handler registry."""
        self._catalog = catalog
        self._registry = registry
        self._local_id = local_id
        self._local_role = local_role

    def resolve(self, message: MessageEnvelope, identity: AuthenticatedIdentity) -> ResolvedCommand:
        """Validate a received command and resolve its registered handler."""
        self._catalog.validate(message)
        command = message.command
        definition = self._catalog.definition(command.command_type, command.command_revision)
        if (
            identity.peer_id != message.source_id
            or identity.scope != message.scope
            or command.command_type not in identity.scopes
            or message.destination_id != self._local_id
            or identity.role not in definition.source_roles
            or self._local_role not in definition.destination_roles
        ):
            raise UnauthorizedPeer
        handler = self._registry.get(command.command_type, command.command_revision)
        return ResolvedCommand(definition, handler)
