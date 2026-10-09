"""Real PostgreSQL atomicity, deduplication races and encrypted snapshots."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from execution_plane_transport_journal.records import JournalEntry

from .exchange_helpers import journal_database


@pytest.fixture
async def journal_db():
    async with journal_database() as database:
        yield database


@pytest.fixture
def message():
    from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope

    return MessageEnvelope(
        message_id=uuid4(),
        command_id=UUID(int=2),
        source_id="ao",
        destination_id="ep",
        scope=CommandScope(client_id="ao", project_id=UUID(int=3)),
        command=Command(
            command_id=UUID(int=2),
            command_type="work.execute",
            command_revision=1,
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
            occurred_at=datetime(2026, 10, 9, tzinfo=UTC),
            payload={"credential": "private-test-value", "nested": {"value": 1}},
        ),
    )


async def test_append_joins_caller_transaction_and_snapshots(journal_db, message):
    async with journal_db.sessions() as session:
        row = await journal_db.store.append_outbound(session, message)
        message.command.payload["nested"]["value"] = 2
        saved = await journal_db.store.load_message(session, row.entry_id)
        assert saved.command.payload["nested"]["value"] == 1
        await session.rollback()
    assert await journal_db.read_rows() == []


async def test_envelope_is_encrypted_at_rest(journal_db, message):
    async with journal_db.sessions.begin() as session:
        row = await journal_db.store.append_outbound(session, message)
    assert "private-test-value" not in str(await journal_db.read_rows())
    async with journal_db.sessions() as session:
        restored = await journal_db.store.load_message(session, row.entry_id)
    assert restored.command.payload["credential"] == "private-test-value"


async def test_concurrent_duplicates_have_one_identity(journal_db, message):
    async def receive() -> tuple[JournalEntry, bool]:
        async with journal_db.sessions.begin() as session:
            return await journal_db.store.append_or_confirm_inbound(session, message)

    received = await asyncio.gather(receive(), receive())
    assert len({row.entry_id for row, _duplicate in received}) == 1
    assert sorted(duplicate for _row, duplicate in received) == [False, True]
    assert len(await journal_db.read_rows()) == 1


@pytest.mark.parametrize("change", ["message_id", "route", "payload", "scope", "sequence"])
async def test_conflicting_identity_does_not_rollback_caller(journal_db, message, change):
    from execution_plane_client.errors import ProtocolConflict

    async with journal_db.sessions.begin() as session:
        await journal_db.store.append_outbound(session, message)
    changed = message.model_copy(deep=True)
    if change == "message_id":
        changed = changed.model_copy(update={"message_id": uuid4()})
    elif change == "route":
        changed = changed.model_copy(
            update={"command": changed.command.model_copy(update={"callback_url": "https://other.test/v1/commands"})}
        )
    elif change == "scope":
        changed = changed.model_copy(update={"scope": changed.scope.model_copy(update={"project_id": UUID(int=99)})})
    elif change == "sequence":
        changed = changed.model_copy(update={"sequence": 1})
    else:
        changed.command.payload["nested"]["value"] = 2
    async with journal_db.sessions.begin() as session:
        await session.execute(text("CREATE TEMP TABLE caller_data (value INTEGER) ON COMMIT DROP"))
        await session.execute(text("INSERT INTO caller_data VALUES (42)"))
        with pytest.raises(ProtocolConflict):
            await journal_db.store.append_outbound(session, changed)
        assert await session.scalar(text("SELECT value FROM caller_data")) == 42


async def test_scoped_lookup_cannot_read_other_project(journal_db, message):
    async with journal_db.sessions.begin() as session:
        await journal_db.store.append_outbound(session, message)
    wrong_scope = message.scope.model_copy(update={"project_id": UUID(int=99)})
    async with journal_db.sessions() as session:
        assert await journal_db.store.get(session, wrong_scope, message.command_id, "outbound") is None


@pytest.mark.parametrize(
    ("direction", "rejected", "expected"),
    [("inbound", False, True), ("outbound", False, False), ("outbound", True, True)],
)
async def test_rearm_handling_is_scoped_fences_old_ownership_and_preserves_envelope(
    journal_db, message, direction, rejected, expected
):
    from sqlalchemy import update

    from execution_plane_client.protocol import CommandScope

    table = journal_db.store.schema.table
    async with journal_db.sessions.begin() as session:
        if direction == "inbound":
            entry, _ = await journal_db.store.append_or_confirm_inbound(session, message)
        else:
            entry = await journal_db.store.append_outbound(session, message)
        await session.execute(
            update(table)
            .where(table.c.entry_id == entry.entry_id)
            .values(
                handling_state="handled",
                handling_generation=4,
                handling_owner=uuid4(),
                handling_lease_until=datetime.now(UTC),
                handled_at=datetime.now(UTC),
                prepared_effect_ciphertext="old-effect",
                reconciliation_required=True,
                delivery_state="rejected" if rejected else entry.delivery_state,
            )
        )
    async with journal_db.sessions.begin() as session:
        other_scope = CommandScope(client_id=message.scope.client_id, project_id=uuid4())
        assert not await journal_db.store.rearm_handling(session, other_scope, message.command_id, direction)
        unchanged = await journal_db.store.get(session, message.scope, message.command_id, direction)
        assert unchanged.handling_generation == 4
        await journal_db.store.lock_command_family(session, message.scope, message.command_id)
        assert await journal_db.store.rearm_handling(session, message.scope, message.command_id, direction) is expected
    async with journal_db.sessions() as session:
        refreshed = await journal_db.store.get(session, message.scope, message.command_id, direction)
        assert refreshed.handling_generation == (5 if expected else 4)
        assert refreshed.handling_state == ("pending" if expected else "handled")
        if expected:
            assert refreshed.handling_owner is None
            assert refreshed.handling_lease_until is None
            assert refreshed.prepared_effect_ciphertext is None
            assert refreshed.handled_at is None
            assert not refreshed.reconciliation_required
        assert (await journal_db.store.load_message(session, entry.entry_id)) == message


async def test_private_context_is_encrypted_scoped_fenced_and_joins_transaction(journal_db, message):
    from execution_plane_client.errors import CommandNotFound, StaleClaim
    from execution_plane_client.protocol import CommandScope

    async with journal_db.sessions.begin() as session:
        await journal_db.store.append_outbound(session, message)
        context = await journal_db.store.set_local_context(
            session,
            message.scope,
            message.command_id,
            data={"token": "secret-token", "attempt": 1},
            correlation_key="workflow-key",
            expected_generation=0,
        )
        assert context.generation == 1
        assert "secret-token" not in repr(context)
    assert "secret-token" not in str(await journal_db.read_rows())
    async with journal_db.sessions() as session:
        assert await journal_db.store.load_message(session, context.entry_id) == message
        other = CommandScope(client_id=message.scope.client_id, project_id=uuid4())
        assert await journal_db.store.get_local_context(session, other, message.command_id) is None
        with pytest.raises(CommandNotFound):
            await journal_db.store.set_local_context(
                session, other, message.command_id, data={}, correlation_key=None, expected_generation=0
            )
        with pytest.raises(StaleClaim):
            await journal_db.store.set_local_context(
                session, message.scope, message.command_id, data={}, correlation_key=None, expected_generation=0
            )
        refreshed = await journal_db.store.set_local_context(
            session,
            message.scope,
            message.command_id,
            data={"token": "new-token", "attempt": 2},
            correlation_key="workflow-key",
            expected_generation=context.generation,
        )
        assert refreshed.generation == 2
        await session.rollback()
    async with journal_db.sessions() as session:
        restored = await journal_db.store.get_local_context(session, message.scope, message.command_id)
        assert restored.data == {"token": "secret-token", "attempt": 1}
        assert restored.generation == 1
        assert (
            len(
                await journal_db.store.list_local_context_entries(
                    session,
                    client_id=message.scope.client_id,
                    correlation_key="workflow-key",
                    command_type="work.execute",
                )
            )
            == 1
        )
        assert (
            await journal_db.store.list_local_context_entries(
                session, client_id="other", correlation_key="workflow-key", command_type="work.execute"
            )
            == []
        )


@pytest.mark.parametrize("tamper", ["generation", "correlation", "row"])
async def test_private_context_authentication_prevents_rebinding(journal_db, message, tamper):
    from sqlalchemy import update

    from execution_plane_client.errors import InvalidCiphertext

    async with journal_db.sessions.begin() as session:
        first = await journal_db.store.append_outbound(session, message)
        await journal_db.store.set_local_context(
            session,
            message.scope,
            message.command_id,
            data={"task_token": "private"},
            correlation_key="one",
            expected_generation=0,
        )
        table = journal_db.store.schema.table
        if tamper == "generation":
            values = {"local_context_generation": 9}
        elif tamper == "correlation":
            values = {"correlation_key": "another"}
        else:
            other_id = uuid4()
            other = message.model_copy(
                update={
                    "message_id": uuid4(),
                    "command_id": other_id,
                    "command": message.command.model_copy(update={"command_id": other_id}),
                }
            )
            await journal_db.store.append_outbound(session, other)
            await journal_db.store.set_local_context(
                session,
                other.scope,
                other_id,
                data={"task_token": "other"},
                correlation_key="one",
                expected_generation=0,
            )
            other_entry = await journal_db.store.get(session, other.scope, other_id, "outbound")
            values = {"local_context_ciphertext": other_entry.local_context_ciphertext}
        await session.execute(update(table).where(table.c.entry_id == first.entry_id).values(**values))
    async with journal_db.sessions() as session:
        with pytest.raises(InvalidCiphertext):
            await journal_db.store.get_local_context(session, message.scope, message.command_id)
