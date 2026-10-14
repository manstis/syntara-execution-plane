"""Supported command revisions and their wire payload contracts."""

from dataclasses import dataclass

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.clusters import ClusterDelete, ClusterProvision, ClusterRegister, ClusterUpdate
from execution_plane_client.commands.control import CancelCommand, Completion
from execution_plane_client.commands.targets import TargetDelete, TargetRegister, TargetUpdate
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.errors import UnsupportedCommand
from execution_plane_client.protocol import MessageEnvelope


@dataclass(frozen=True)
class CommandDefinition:
    """One command type, revision and payload schema."""

    command_type: str
    revision: int
    payload_schema: type[CommandPayload]


class CommandCatalog:
    """Validate known commands independently of installed handlers."""

    def __init__(self) -> None:
        """Install version-one work, resource and completion contracts."""
        payloads = (
            WorkExecute,
            Completion,
            ClusterRegister,
            ClusterProvision,
            ClusterUpdate,
            ClusterDelete,
            TargetRegister,
            TargetUpdate,
            TargetDelete,
            CancelCommand,
        )
        self._definitions = {
            (schema.command_type, schema.command_revision): CommandDefinition(
                schema.command_type, schema.command_revision, schema
            )
            for schema in payloads
        }

    def definition(self, command_type: str, revision: int) -> CommandDefinition:
        """Return a supported revision or an explicit rejection."""
        try:
            return self._definitions[command_type, revision]
        except KeyError:
            raise UnsupportedCommand from None

    def validate(self, message: MessageEnvelope) -> None:
        """Validate the payload and completion parent before handling."""
        definition = self.definition(message.command.command_type, message.command.command_revision)
        definition.payload_schema.model_validate(message.command.payload)
        definition.payload_schema.validate_parent(message.command.parent_command_id)
