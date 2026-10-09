"""Journal command preparation coordinates lazy intent and private context creation."""

from datetime import UTC, datetime
from typing import Never
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope
from execution_plane_transport_journal.records import JournalEntry, LocalContext
from execution_plane_transport_journal.store import ExchangeJournalStore


class InMemoryJournal(ExchangeJournalStore):
    """Replace database primitives while exercising the real command orchestration."""

    def __init__(self, entry: JournalEntry, message: MessageEnvelope) -> None:
        """Keep one frozen command and its context in memory for this behavior test."""
        self.entry = None
        self.expected_entry = entry
        self.message = message
        self.context = None
        self.events = []

    async def lock_outbound_command_identity(
        self, session: AsyncSession, scope: CommandScope, command_id: UUID
    ) -> None:
        self.events.append("identity-lock")

    async def lock_command_family(self, session: AsyncSession, scope: CommandScope, command_id: UUID) -> None:
        self.events.append("family-lock")

    async def get(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        direction: str,
    ) -> JournalEntry | None:
        self.events.append("lookup")
        return self.entry

    async def get_local_context(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        direction: str = "outbound",
        *,
        for_update: bool = False,
    ) -> LocalContext | None:
        return self.context

    async def set_local_context(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        *,
        data: dict[str, object],
        correlation_key: str | None,
        expected_generation: int,
        direction: str = "outbound",
    ) -> LocalContext:
        self.context = LocalContext(
            entry_id=self.expected_entry.entry_id,
            generation=expected_generation + 1,
            correlation_key=correlation_key,
            data=dict(data),
        )
        return self.context

    async def load_message(self, session: AsyncSession, entry_id: UUID) -> MessageEnvelope:
        self.events.append("load-message")
        return self.message


async def test_ensure_outbound_builds_once_and_reuses_frozen_message():
    command_id = UUID(int=42)
    scope = CommandScope(client_id="ao", project_id=UUID(int=7))
    message = MessageEnvelope(
        message_id=uuid4(),
        command_id=command_id,
        source_id="ao",
        destination_id="ep",
        scope=scope,
        command=Command(
            command_id=command_id,
            command_type="work.execute",
            command_revision=1,
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
            occurred_at=datetime(2026, 10, 12, tzinfo=UTC),
            payload={"script": "print('hello')"},
        ),
    )
    entry = JournalEntry.model_construct(
        entry_id=uuid4(),
        message_id=message.message_id,
        command_id=command_id,
        command_type="work.execute",
        direction="outbound",
        client_id=scope.client_id,
        project_id=scope.project_id,
        all_projects=False,
    )
    journal = InMemoryJournal(entry, message)
    factory_calls = {"command": 0, "context": 0}

    async def create_command() -> JournalEntry:
        factory_calls["command"] += 1
        journal.entry = entry
        return entry

    def create_context() -> dict[str, object]:
        factory_calls["context"] += 1
        return {"task_token": "private-token"}

    first = await journal.ensure_outbound(
        object(),
        scope,
        command_id,
        command_factory=create_command,
        context_factory=create_context,
        correlation_key="workflow-key",
    )

    assert first.created is True
    assert first.message == message
    assert first.local_context.data == {"task_token": "private-token"}
    assert factory_calls == {"command": 1, "context": 1}
    assert journal.events[:3] == ["identity-lock", "family-lock", "lookup"]

    async def unexpected_factory() -> Never:
        raise AssertionError

    def unexpected_context() -> dict[str, object]:
        factory_calls["context"] += 1
        return {"unexpected": True}

    second = await journal.ensure_outbound(
        object(),
        scope,
        command_id,
        command_factory=unexpected_factory,
        context_factory=unexpected_context,
        correlation_key="workflow-key",
    )

    assert second.created is False
    assert second.entry == entry
    assert second.message == message
    assert second.local_context == first.local_context
    assert factory_calls == {"command": 1, "context": 1}
