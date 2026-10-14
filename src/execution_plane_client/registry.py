"""In-memory registration of participant-owned handlers."""

from execution_plane_client.catalog import CommandDefinition
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.handlers import CommandHandler
from execution_plane_client.protocol import CapabilitiesRead


class CommandRegistry[ContextT]:
    """Map installed command revisions to exactly one handler."""

    def __init__(self) -> None:
        """Start without handlers or advertised capabilities."""
        self._handlers: dict[tuple[str, int], CommandHandler[ContextT]] = {}

    def register(self, definition: CommandDefinition, handler: CommandHandler[ContextT]) -> None:
        """Reject duplicate registrations."""
        key = (definition.command_type, definition.revision)
        if key in self._handlers:
            raise ProtocolConflict
        self._handlers[key] = handler

    def get(self, command_type: str, revision: int) -> CommandHandler[ContextT]:
        """Reject commands the participant cannot handle."""
        try:
            return self._handlers[command_type, revision]
        except KeyError:
            raise UnsupportedCommand from None

    def capabilities(self) -> CapabilitiesRead:
        """Advertise only installed revisions."""
        commands: dict[str, list[int]] = {}
        for name, revision in sorted(self._handlers):
            commands.setdefault(name, []).append(revision)
        return CapabilitiesRead(commands=commands)
