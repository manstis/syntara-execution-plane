"""Resource reservations and cancellation live on real journal command rows."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text, update

from execution_plane_client.commands import CommandIdentity, CommandRoute, ResourceRevision
from execution_plane_client.commands.control import CancelCommand, cancel_command
from execution_plane_client.commands.targets import target_delete
from execution_plane_client.errors import ProtocolConflict, UnauthorizedPeer
from execution_plane_client.handlers import CommandRegistry, HandlingResult
from execution_plane_client.protocol import CommandScope, CommandStatus
from execution_plane_transport_journal.claims import JournalClaims
from execution_plane_transport_journal.completion import append_completion
from execution_plane_transport_journal.workers.handling import JournalHandlingWorker

from .exchange_helpers import journal_database


def resource_message(key=None, revision=1, *, scope=None, command_type="execution_target.delete"):
    """Build immutable resource commands with independently selected identities."""
    message = target_delete(
        identity=CommandIdentity(command_id=uuid4(), message_id=uuid4(), occurred_at=datetime.now(UTC)),
        route=CommandRoute(
            source_id="ao",
            destination_id="ep",
            scope=scope or CommandScope(client_id="ao", project_id=UUID(int=3)),
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
        ),
        payload=ResourceRevision(resource_key=key or uuid4(), desired_revision=revision),
    )
    return message.model_copy(update={"command": message.command.model_copy(update={"command_type": command_type})})


def cancellation_message(message, occurred_at=None):
    """Build a cancellation with the exact original scope and transport routes."""
    return cancel_command(
        identity=CommandIdentity(
            command_id=uuid4(),
            message_id=uuid4(),
            occurred_at=occurred_at or datetime.now(UTC),
        ),
        route=CommandRoute(
            source_id="ao",
            destination_id="ep",
            scope=message.scope,
            target_url=message.command.target_url,
            callback_url=message.command.callback_url,
        ),
        payload=CancelCommand(target_command_id=message.command_id),
    )


async def test_new_resource_head_inherits_observation_and_duplicate_preserves_it():
    async with journal_database() as database:
        store = database.store
        first = resource_message()
        second = resource_message(first.command.payload["resource_key"], 2)
        physical = uuid4()
        async with database.sessions.begin() as session:
            first_entry, _ = await store.append_or_confirm_inbound(session, first)
            await store.resource_intents.observe_resource(
                session, first, resource_id=physical, observed_revision=1, deleted=True
            )
        async with database.sessions.begin() as session:
            second_entry, duplicate = await store.append_or_confirm_inbound(session, second)
            assert not duplicate
            assert second_entry.resource_head
            assert (second_entry.resource_id, second_entry.observed_revision, second_entry.resource_deleted) == (
                physical,
                1,
                True,
            )
            duplicate_entry, duplicate = await store.append_or_confirm_inbound(session, first)
            assert duplicate
            assert not duplicate_entry.resource_head
            assert (await store.resource_intents.resource(session, first)).entry_id == second_entry.entry_id
            assert (
                await store.resource_intents.owner(session, "execution_target", physical)
            ).entry_id == second_entry.entry_id
        rows = await database.read_rows()
        assert len(rows) == 2
        assert sum(row["resource_head"] for row in rows) == 1
        assert next(row for row in rows if row["entry_id"] == first_entry.entry_id)["observed_revision"] == 1


async def test_revision_conflict_scope_and_stale_receipt():
    async with journal_database() as database:
        store = database.store
        first = resource_message(revision=3)
        key = first.command.payload["resource_key"]
        async with database.sessions.begin() as session:
            head, _ = await store.append_or_confirm_inbound(session, first)
        for message, error in (
            (resource_message(key, 3, command_type="execution_target.update"), ProtocolConflict),
            (resource_message(key, 4, scope=CommandScope(client_id="ao", project_id=UUID(int=9))), UnauthorizedPeer),
        ):
            with pytest.raises(error):
                async with database.sessions.begin() as session:
                    await store.append_or_confirm_inbound(session, message)
        async with database.sessions.begin() as session:
            same, _ = await store.append_or_confirm_inbound(session, resource_message(key, 3))
            stale, _ = await store.append_or_confirm_inbound(session, resource_message(key, 2))
            assert not same.resource_head
            assert not stale.resource_head
            assert (await store.resource_intents.resource(session, first)).entry_id == head.entry_id
        assert len(await database.read_rows()) == 3


async def test_cancellation_before_original_is_indexed_and_exact_scope(work_message):
    async with journal_database() as database:
        store = database.store
        cancellation = cancellation_message(work_message)
        async with database.sessions.begin() as session:
            entry, _ = await store.append_or_confirm_inbound(session, cancellation)
            assert entry.target_command_id == work_message.command_id
        async with database.sessions.begin() as session:
            await store.append_or_confirm_inbound(session, work_message)
            assert await store.intents.is_cancelled(work_message.scope, work_message.command_id, session=session)
            other = work_message.model_copy(update={"scope": CommandScope(client_id="ao", project_id=UUID(int=9))})
            assert not await store.intents.is_cancelled(other.scope, other.command_id, session=session)
        async with database.sessions() as session:
            assert (
                await session.scalar(
                    select(store.schema.table.c.target_command_id).where(
                        store.schema.table.c.entry_id == entry.entry_id
                    )
                )
                == work_message.command_id
            )


async def test_receipt_waits_for_handler_resource_lock_without_family_cycle():
    async with journal_database() as database:
        store = database.store
        original = resource_message()
        newer = resource_message(original.command.payload["resource_key"], 2)
        async with database.sessions.begin() as session:
            await store.append_or_confirm_inbound(session, original)
        held = asyncio.Event()
        release = asyncio.Event()
        pid = asyncio.get_running_loop().create_future()
        physical = uuid4()

        async def handling() -> None:
            async with database.sessions.begin() as session:
                await store.intents.lock(session, original)
                await store.lock_command_family(session, original.scope, original.command_id)
                held.set()
                await release.wait()
                await store.resource_intents.observe_resource(
                    session, original, resource_id=physical, observed_revision=1
                )

        async def receiving() -> None:
            async with database.sessions.begin() as session:
                pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                await store.append_or_confirm_inbound(session, newer)

        handler = asyncio.create_task(handling())
        receiver = None
        try:
            await asyncio.wait_for(held.wait(), 5)
            receiver = asyncio.create_task(receiving())
            backend = await pid
            async with asyncio.timeout(5), database.sessions() as session:
                while not await session.scalar(  # noqa: ASYNC110 - Observe real database blocking before release.
                    text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": backend}
                ):
                    await asyncio.sleep(0.01)
            release.set()
            await asyncio.wait_for(asyncio.gather(handler, receiver), 5)
            async with database.sessions() as session:
                head = await store.resource_intents.resource(session, newer)
                assert head.resource_id == physical
                assert head.observed_revision == 1
        finally:
            release.set()
            for task in (handler, receiver):
                if task is not None:
                    task.cancel()
            await asyncio.gather(handler, *([receiver] if receiver is not None else []), return_exceptions=True)


async def test_stale_observation_and_physical_owner_conflict_leave_head_intact():
    async with journal_database() as database:
        store = database.store
        original = resource_message()
        newer = resource_message(original.command.payload["resource_key"], 2)
        another = resource_message()
        physical = uuid4()
        async with database.sessions.begin() as session:
            await store.append_or_confirm_inbound(session, original)
            await store.resource_intents.observe_resource(session, original, resource_id=physical, observed_revision=1)
            await store.append_or_confirm_inbound(session, newer)
            await store.append_or_confirm_inbound(session, another)
        for message in (original, another):
            with pytest.raises(ProtocolConflict):
                async with database.sessions.begin() as session:
                    await store.resource_intents.observe_resource(
                        session, message, resource_id=physical, observed_revision=1
                    )
        async with database.sessions() as session:
            head = await store.resource_intents.resource(session, newer)
            assert head.resource_id == physical
            assert head.observed_revision == 1


async def test_completion_parent_row_lock_never_waits_for_work_intent(work_message):
    """Provider completion may already own domain/parent rows when cancellation starts."""
    async with journal_database() as database:
        store = database.store
        async with database.sessions.begin() as session:
            entry, _ = await store.append_or_confirm_inbound(session, work_message)
        table = store.schema.table
        parent_locked = asyncio.Event()
        intent_locked = asyncio.Event()
        finish = asyncio.Event()
        pid = asyncio.get_running_loop().create_future()

        async def complete() -> None:
            async with database.sessions.begin() as session:
                await session.execute(select(table).where(table.c.entry_id == entry.entry_id).with_for_update())
                parent_locked.set()
                await finish.wait()
                await append_completion(
                    store,
                    session,
                    work_message,
                    HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True),
                )

        async def cancel() -> None:
            await parent_locked.wait()
            async with database.sessions.begin() as session:
                pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                await store.intents.lock(session, work_message)
                intent_locked.set()
                await store.lock_command_family(session, work_message.scope, work_message.command_id)

        tasks = [asyncio.create_task(complete()), asyncio.create_task(cancel())]
        try:
            await asyncio.wait_for(intent_locked.wait(), 5)
            backend = await pid
            async with asyncio.timeout(5), database.sessions() as session:
                while not await session.scalar(  # noqa: ASYNC110 - Verify the handler is waiting on the parent row.
                    text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"),
                    {"pid": backend},
                ):
                    await asyncio.sleep(0.01)
            finish.set()
            outcomes = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
            assert outcomes == [None, None], [str(outcome) for outcome in outcomes]
        finally:
            finish.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_outbound_cancellation_and_rejection_handling_share_family_lock_order(work_message, monkeypatch):
    """AO cancellation enqueue already owns the original family before outbound append."""
    async with journal_database() as database:
        store = database.store
        now = datetime.now(UTC) + timedelta(seconds=1)
        table = store.schema.table
        cancellation = cancellation_message(work_message, now)
        async with database.sessions.begin() as session:
            entry = await store.append_outbound(session, work_message)
            await session.execute(
                update(table)
                .where(table.c.entry_id == entry.entry_id)
                .values(
                    delivery_state="rejected",
                    handling_state="pending",
                    handling_due_at=now,
                )
            )
        async with database.sessions.begin() as session:
            claim = (await JournalClaims(store).claim_due_handling(session, uuid4(), now, 1))[0]
        worker = JournalHandlingWorker(store, database.sessions, CommandRegistry(), local_id="ao")
        family_locked = asyncio.Event()
        handling_entered = asyncio.Event()
        finish_enqueue = asyncio.Event()
        handling_pid = asyncio.get_running_loop().create_future()
        lock_family = store.lock_command_family

        async def observe_family_attempt(*args: object) -> None:
            handling_entered.set()
            await lock_family(*args)

        monkeypatch.setattr(store, "lock_command_family", observe_family_attempt)

        async def enqueue_cancellation() -> None:
            async with database.sessions.begin() as session:
                await lock_family(session, work_message.scope, work_message.command_id)
                family_locked.set()
                await finish_enqueue.wait()
                await store.append_outbound(session, cancellation)

        async def handle_rejection() -> None:
            await family_locked.wait()
            async with database.sessions.begin() as session:
                handling_pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                assert await worker._locked(session, claim) is not None

        tasks = [asyncio.create_task(enqueue_cancellation()), asyncio.create_task(handle_rejection())]
        try:
            await asyncio.wait_for(handling_entered.wait(), 5)
            backend = await handling_pid
            async with asyncio.timeout(5), database.sessions() as session:
                while not await session.scalar(  # noqa: ASYNC110 - Check the rejection's real parent-row wait.
                    text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"),
                    {"pid": backend},
                ):
                    await asyncio.sleep(0.01)
            finish_enqueue.set()
            outcomes = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
            assert outcomes == [None, None], [str(outcome) for outcome in outcomes]
            async with database.sessions() as session:
                saved = await store.get(session, cancellation.scope, cancellation.command_id, "outbound")
                assert saved.target_command_id == work_message.command_id
        finally:
            finish_enqueue.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("operation", ["append", "handling"])
async def test_outbound_responsibility_never_waits_for_inbound_admission_lock(work_message, operation):
    async with journal_database() as database:
        store = database.store
        now = datetime.now(UTC) + timedelta(seconds=1)
        table = store.schema.table
        async with database.sessions.begin() as session:
            entry = await store.append_outbound(session, work_message)
            await session.execute(
                update(table)
                .where(table.c.entry_id == entry.entry_id)
                .values(
                    delivery_state="rejected",
                    handling_state="pending",
                    handling_due_at=now,
                )
            )
        async with database.sessions.begin() as session:
            claim = (await JournalClaims(store).claim_due_handling(session, uuid4(), now, 1))[0]
        worker = JournalHandlingWorker(store, database.sessions, CommandRegistry(), local_id="ao")
        async with database.sessions.begin() as admission:
            await store.intents.lock(admission, work_message)
            async with database.sessions.begin() as sender:
                await sender.execute(text("SET LOCAL lock_timeout = '250ms'"))
                if operation == "append":
                    saved = await store.append_outbound(sender, cancellation_message(work_message, now))
                    assert saved.target_command_id == work_message.command_id
                else:
                    saved = await worker._locked(sender, claim)
                    assert saved.entry_id == entry.entry_id
