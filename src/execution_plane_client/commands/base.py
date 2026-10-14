"""Construct transport-neutral messages from typed command payloads."""

from datetime import UTC, datetime
from typing import ClassVar
from uuid import UUID, uuid4

from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope, WireModel


class CommandPayload(WireModel):
    """A typed command knows its wire identity and builds its delivery envelope."""

    command_type: ClassVar[str]
    command_revision: ClassVar[int] = 1
    requires_parent: ClassVar[bool] = False

    @classmethod
    def validate_parent(cls, parent_command_id: UUID | None) -> None:
        """Require correlation for commands that report another command's outcome."""
        if cls.requires_parent and parent_command_id is None:
            raise ProtocolConflict

    def _message_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json")

    def to_message(
        self,
        *,
        source_id: str,
        destination_id: str,
        client_id: str,
        command_id: UUID | None = None,
        parent_command_id: UUID | None = None,
        occurred_at: datetime | None = None,
        message_id: UUID | None = None,
        sequence: int = 0,
    ) -> MessageEnvelope:
        """Build a validated message, generating a command UUID and UTC time if omitted.

        The message ID defaults to the command ID. Retain the returned message for
        retries, or supply the original IDs and timestamp when reconstructing it.
        Routing, authentication, HTTP delivery and persistence belong to the caller.
        """
        self.validate_parent(parent_command_id)
        payload = self._message_payload()
        type(self).model_validate(payload)
        identity = uuid4() if command_id is None else command_id
        return MessageEnvelope(
            message_id=identity if message_id is None else message_id,
            command_id=identity,
            source_id=source_id,
            destination_id=destination_id,
            scope=CommandScope(client_id=client_id),
            sequence=sequence,
            command=Command(
                command_id=identity,
                parent_command_id=parent_command_id,
                command_type=self.command_type,
                command_revision=self.command_revision,
                payload=payload,
                occurred_at=datetime.now(UTC) if occurred_at is None else occurred_at,
            ),
        )
