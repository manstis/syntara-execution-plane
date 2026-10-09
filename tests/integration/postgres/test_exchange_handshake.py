"""Receipt and control acceptance share a durable, loop-free transaction."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from execution_plane_client.handlers import CommandRegistry, HandlingResult
from execution_plane_client.security import AuthenticatedIdentity

from .exchange_helpers import journal_database


def receipt_handler(store, *, local_id="ep", hook=None):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.ingress import CommandReceiptHandler

    class Handler:
        async def handle(self, message, context, session) -> HandlingResult:
            return HandlingResult()

    registry = CommandRegistry()
    catalog = CommandCatalog()
    registry.register(catalog.definition("work.execute", 1), Handler())
    registry.register(catalog.definition("work.execute.completed", 1), Handler())
    return CommandReceiptHandler(
        store,
        catalog,
        registry,
        local_id=local_id,
        local_role="controller" if local_id == "ao" else "executor",
        receipt_hook=hook,
    )


def identity_for(message):
    return AuthenticatedIdentity(
        message.source_id,
        message.scope,
        frozenset({message.command.command_type}),
        "executor" if message.source_id == "ep" else "controller",
    )


async def test_completion_before_acceptance_updates_original_outcome(work_message):
    from execution_plane_client.completion import append_completion
    from execution_plane_client.protocol import CommandStatus

    async with journal_database() as ao, journal_database() as ep:
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
        async with ep.sessions.begin() as session:
            await receipt_handler(ep.store).receive(work_message, identity_for(work_message), session)
            completion = await append_completion(
                ep.store,
                session,
                work_message,
                HandlingResult(status=CommandStatus.SUCCEEDED, result={"value": 42}, completion_required=True),
            )
        async with ao.sessions.begin() as session:
            await receipt_handler(ao.store, local_id="ao").receive(completion, identity_for(completion), session)
        async with ao.sessions() as session:
            parent = await ao.store.get(session, work_message.scope, work_message.command_id, "outbound")
        assert parent.command_status == "succeeded"
        assert parent.delivery_state == "queued"


async def test_completion_must_match_the_original_peer_and_parent(work_message):
    from execution_plane_client.completion import append_completion
    from execution_plane_client.errors import UnauthorizedPeer
    from execution_plane_client.protocol import CommandStatus

    async with journal_database() as ao, journal_database() as ep:
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
        async with ep.sessions.begin() as session:
            await receipt_handler(ep.store).receive(work_message, identity_for(work_message), session)
            completion = await append_completion(
                ep.store,
                session,
                work_message,
                HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True),
            )
        forged = completion.model_copy(update={"source_id": "other"})
        identity = AuthenticatedIdentity("other", forged.scope, frozenset({"work.execute.completed"}), "executor")
        with pytest.raises(UnauthorizedPeer):
            async with ao.sessions.begin() as session:
                await receipt_handler(ao.store, local_id="ao").receive(forged, identity, session)
        assert len(await ao.read_rows()) == 1


async def test_receipt_hook_failure_rolls_back_inbound_and_acceptance(work_message):
    async with journal_database() as db:

        async def fail(message, identity, session) -> None:
            error = "domain failure"
            raise RuntimeError(error)

        receiver = receipt_handler(db.store, hook=fail)
        with pytest.raises(RuntimeError, match="domain failure"):
            async with db.sessions.begin() as session:
                await receiver.receive(work_message, identity_for(work_message), session)
        assert await db.read_rows() == []


async def test_duplicate_receipt_reuses_acceptance_without_repeating_hook(work_message):
    async with journal_database() as db:
        called = []

        async def hook(message, identity, session) -> None:
            called.append(message.command_id)

        receiver = receipt_handler(db.store, hook=hook)
        async with db.sessions.begin() as session:
            first = await receiver.receive(work_message, identity_for(work_message), session)
        async with db.sessions.begin() as session:
            second = await receiver.receive(work_message, identity_for(work_message), session)
        rows = await db.read_rows()
        assert first.duplicate is False
        assert second.duplicate is True
        assert called == [work_message.command_id]
        assert len(rows) == 2
        assert sum(row["command_type"] == "command.accepted" for row in rows) == 1


async def test_acceptance_acknowledges_exact_message_without_ack_of_ack(work_message):
    async with journal_database() as ao, journal_database() as ep:
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
        async with ep.sessions.begin() as session:
            await receipt_handler(ep.store).receive(work_message, identity_for(work_message), session)
            table = ep.store.schema.table
            entry_id = await session.scalar(select(table.c.entry_id).where(table.c.command_type == "command.accepted"))
            acceptance = await ep.store.load_message(session, entry_id)
        async with ao.sessions.begin() as session:
            await receipt_handler(ao.store, local_id="ao").receive(acceptance, identity_for(acceptance), session)
        rows = await ao.read_rows()
        assert len(rows) == 2
        assert next(row for row in rows if row["direction"] == "outbound")["delivery_state"] == "acknowledged"
        assert next(row for row in rows if row["direction"] == "inbound")["handling_state"] == "handled"


async def test_lost_http_response_retries_same_durable_receipt(work_message):
    from datetime import timedelta

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.errors import RetryableDelivery
    from execution_plane_client.protocol import CapabilitiesRead
    from execution_plane_client.transport import TransportReceipt
    from execution_plane_client.workers.delivery import JournalDeliveryWorker

    async with journal_database() as ao, journal_database() as ep:
        calls = []
        now = datetime.now(UTC) + timedelta(seconds=1)

        class Handler:
            async def handle(self, message, context, session) -> HandlingResult:
                return HandlingResult()

        class Network:
            async def get_capabilities(self, peer_id, scope=None) -> CapabilitiesRead:
                return CapabilitiesRead(
                    commands={"work.execute": [1]}, receipt_handlers=["work.execute"], completion_handlers=[]
                )

            async def send(self, message) -> TransportReceipt:
                async with ep.sessions.begin() as session:
                    await receipt_handler(ep.store).receive(message, identity_for(message), session)
                calls.append(message.message_id)
                if len(calls) == 1:
                    raise RetryableDelivery
                return TransportReceipt(received=True, message_id=message.message_id)

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute.completed", 1), Handler())
        worker = JournalDeliveryWorker(ao.store, ao.sessions, Network(), registry, clock=lambda: now)
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
        await worker.run_once()
        now += timedelta(seconds=60)
        await worker.run_once()
        assert calls == [work_message.message_id, work_message.message_id]
        assert len(await ep.read_rows()) == 2
        async with ao.sessions() as session:
            row = await ao.store.get(session, work_message.scope, work_message.command_id, "outbound")
        assert row.delivery_state == "sent"
        assert row.delivery_attempts == 2


async def test_unsupported_peer_revision_leaves_local_failure_responsibility(work_message):
    from datetime import timedelta

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.protocol import CapabilitiesRead
    from execution_plane_client.workers.delivery import JournalDeliveryWorker

    async with journal_database() as ao:

        class Handler:
            async def handle(self, message, context, session) -> HandlingResult:
                return HandlingResult()

        class Network:
            async def get_capabilities(self, peer_id, scope=None) -> CapabilitiesRead:
                return CapabilitiesRead(commands={}, receipt_handlers=[], completion_handlers=[])

            async def send(self, message) -> None:
                pytest.fail("Unsupported command must not be sent")

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute.completed", 1), Handler())
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
        worker = JournalDeliveryWorker(
            ao.store, ao.sessions, Network(), registry, clock=lambda: datetime.now(UTC) + timedelta(seconds=1)
        )
        await worker.run_once()
        row = (await ao.read_rows())[0]
        assert row["delivery_state"] == "rejected"
        assert row["handling_state"] == "pending"
        assert row["delivery_error_code"] == "unsupported_revision"
