"""HTTP receipts, duplicate commands and completion callbacks share the Journal."""

from datetime import UTC, datetime

import pytest

from execution_plane_client.handlers import CommandRegistry, HandlingResult
from execution_plane_client.security import AuthenticatedIdentity

from .exchange_helpers import journal_database


def receipt_handler(store, *, local_id="ep", hook=None):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_transport_journal.ingress import CommandReceiptHandler

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


async def test_completion_before_http_receipt_updates_original_outcome(work_message):
    from execution_plane_client.protocol import CommandStatus
    from execution_plane_transport_journal.completion import append_completion

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
    from execution_plane_client.errors import UnauthorizedPeer
    from execution_plane_client.protocol import CommandStatus
    from execution_plane_transport_journal.completion import append_completion

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


async def test_receipt_hook_failure_rolls_back_inbound_record(work_message):
    async with journal_database() as db:

        async def fail(message, identity, session) -> None:
            error = "domain failure"
            raise RuntimeError(error)

        receiver = receipt_handler(db.store, hook=fail)
        with pytest.raises(RuntimeError, match="domain failure"):
            async with db.sessions.begin() as session:
                await receiver.receive(work_message, identity_for(work_message), session)
        assert await db.read_rows() == []


async def test_duplicate_http_receipt_does_not_repeat_hook_or_create_ack_command(work_message):
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
        assert len(rows) == 1
        assert rows[0]["command_type"] == "work.execute"


async def test_lost_http_response_retries_same_durable_receipt(work_message):
    from datetime import timedelta

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.errors import RetryableDelivery
    from execution_plane_client.protocol import CapabilitiesRead
    from execution_plane_client.transport import DispatchReceipt, DispatchState
    from execution_plane_transport_journal.workers.delivery import JournalDeliveryWorker

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

            async def send(self, message) -> DispatchReceipt:
                async with ep.sessions.begin() as session:
                    await receipt_handler(ep.store).receive(message, identity_for(message), session)
                calls.append(message.message_id)
                if len(calls) == 1:
                    raise RetryableDelivery
                return DispatchReceipt(message_id=message.message_id, state=DispatchState.DELIVERED)

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute.completed", 1), Handler())
        worker = JournalDeliveryWorker(ao.store, ao.sessions, Network(), registry, clock=lambda: now)
        async with ao.sessions.begin() as session:
            await ao.store.append_outbound(session, work_message)
        await worker.run_once()
        now += timedelta(seconds=60)
        await worker.run_once()
        assert calls == [work_message.message_id, work_message.message_id]
        assert len(await ep.read_rows()) == 1
        async with ao.sessions() as session:
            row = await ao.store.get(session, work_message.scope, work_message.command_id, "outbound")
        assert row.delivery_state == "sent"
        assert row.delivery_attempts == 2


async def test_unsupported_peer_revision_leaves_local_failure_responsibility(work_message):
    from datetime import timedelta

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.protocol import CapabilitiesRead
    from execution_plane_transport_journal.workers.delivery import JournalDeliveryWorker

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
