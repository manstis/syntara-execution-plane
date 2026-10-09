"""Committed journal work wakes both participants without a polling tick."""

# ruff: noqa: ASYNC110 - Bounded probes assert durable state changed by separate workers.

import asyncio
from uuid import uuid4

import pytest

from execution_plane_client.configuration import ExchangeOptions
from execution_plane_client.journal.claims import JournalClaim
from execution_plane_client.transport import TransportReceipt

from .exchange_helpers import peer_pair


class ListenerObserver:
    """Expose actual listener establishment to tests without sleeping for startup."""

    def __init__(self) -> None:
        """Record listener establishment without changing production behaviour."""
        self.ready = asyncio.Queue()

    def record(self, event, fields):
        if event == "journal.listener.ready":
            self.ready.put_nowait(None)


def another_command(message):
    """Create an independent logical command without changing the test workload."""
    command_id = uuid4()
    return message.model_copy(
        update={
            "message_id": uuid4(),
            "command_id": command_id,
            "command": message.command.model_copy(update={"command_id": command_id}),
        }
    )


async def test_idle_bilateral_exchange_does_not_wait_for_poll_interval(work_message):
    async with peer_pair(options=ExchangeOptions(poll_seconds=60, reconciliation_seconds=60), realtime=True) as pair:
        tasks = [asyncio.create_task(peer.service.run()) for peer in (pair.ao, pair.ep)]
        try:
            # Let the initial empty scans finish; only a later commit can wake delivery.
            await asyncio.sleep(0.2)
            async with pair.ao.sessions.begin() as session:
                await pair.ao.service.client.submit(work_message, session)
            async with asyncio.timeout(3):
                while not pair.results:
                    await asyncio.sleep(0.01)
                while await pair.ao.delivery_state(work_message.command_id) != "acknowledged":
                    await asyncio.sleep(0.01)
                while await pair.ep.delivery_state(pair.results[0]) != "acknowledged":
                    await asyncio.sleep(0.01)
            assert pair.effects == 1
            assert len(pair.results) == 1
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_commit_during_empty_scan_is_not_lost(work_message, monkeypatch):
    async with peer_pair(options=ExchangeOptions(poll_seconds=60), realtime=True) as pair:
        empty_scan, release_scan = asyncio.Event(), asyncio.Event()
        worker = pair.ao.service.delivery
        claim = worker._claim

        async def pause_first_empty_scan(limit) -> list[JournalClaim]:
            claims = await claim(limit)
            if not claims and not empty_scan.is_set():
                empty_scan.set()
                await release_scan.wait()
            return claims

        monkeypatch.setattr(worker, "_claim", pause_first_empty_scan)
        observer = ListenerObserver()
        pair.ao.service.observer = observer
        tasks = [asyncio.create_task(peer.service.run()) for peer in (pair.ao, pair.ep)]
        try:
            await asyncio.wait_for(observer.ready.get(), 2)
            await asyncio.wait_for(empty_scan.wait(), 2)
            worker.wakeup.clear()
            async with pair.ao.sessions.begin() as session:
                await pair.ao.service.client.submit(work_message, session)
            await asyncio.wait_for(worker.wakeup.wait(), 2)
            release_scan.set()
            async with asyncio.timeout(3):
                while not pair.results:
                    await asyncio.sleep(0.01)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_notification_commit_rollback_and_savepoint_boundaries(work_message):
    from execution_plane_client.journal.notifications import JournalNotifications

    from .exchange_helpers import journal_database

    async with journal_database() as db:
        delivery, handling, observer = asyncio.Event(), asyncio.Event(), ListenerObserver()
        listener = JournalNotifications(db.store.schema, db.sessions, delivery, handling, observer=observer)
        task = asyncio.create_task(listener.run())
        try:
            await asyncio.wait_for(observer.ready.get(), 2)
            delivery.clear()
            handling.clear()
            async with db.sessions() as session:
                await db.store.append_outbound(session, work_message)
                await asyncio.sleep(0.05)
                assert not delivery.is_set()
                await session.rollback()
            async with db.sessions.begin() as session:
                nested = await session.begin_nested()
                await db.store.append_outbound(session, work_message)
                await nested.rollback()
            await asyncio.sleep(0.05)
            assert not delivery.is_set()
            assert await db.read_rows() == []
            async with db.sessions.begin() as session:
                await db.store.append_outbound(session, work_message)
            await asyncio.wait_for(delivery.wait(), 2)
            assert not handling.is_set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_listener_reconnect_rescans_then_receives_new_commits(work_message):
    from sqlalchemy import text

    from execution_plane_client.journal.notifications import JournalNotifications

    from .exchange_helpers import journal_database

    async with journal_database() as db:
        delivery, handling, observer = asyncio.Event(), asyncio.Event(), ListenerObserver()
        listener = JournalNotifications(
            db.store.schema,
            db.sessions,
            delivery,
            handling,
            observer=observer,
            health_seconds=0.1,
            reconnect_seconds=0.01,
        )
        task = asyncio.create_task(listener.run())
        try:
            await asyncio.wait_for(observer.ready.get(), 2)
            owned_session = next(session for session in db.live_sessions if session.owner_task is task)
            connection = await owned_session.connection()
            raw = await connection.get_raw_connection()
            pid = raw.driver_connection.get_server_pid()
            delivery.clear()
            handling.clear()
            async with db.sessions.begin() as session:
                await session.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            await asyncio.wait_for(observer.ready.get(), 2)
            assert delivery.is_set()
            assert handling.is_set()
            delivery.clear()
            async with db.sessions.begin() as session:
                await db.store.append_outbound(session, work_message)
            await asyncio.wait_for(delivery.wait(), 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_observation_deadline_does_not_wait_for_fallback_poll(work_message):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.handlers import CommandRegistry, HandlingResult
    from execution_plane_client.protocol import CommandStatus
    from execution_plane_client.workers.handling import JournalHandlingWorker

    from .exchange_helpers import journal_database

    async with journal_database() as db:
        observed, mutations = asyncio.Event(), []

        class Handler:
            async def handle(self, message, context, session) -> HandlingResult:
                mutations.append(message.command_id)
                return HandlingResult(status=CommandStatus.RUNNING, observe_after_seconds=0.05)

            async def observe(self, message, context, session) -> HandlingResult:
                observed.set()
                return HandlingResult(status=CommandStatus.SUCCEEDED)

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute", 1), Handler())
        async with db.sessions.begin() as session:
            await db.store.append_or_confirm_inbound(session, work_message)
        worker = JournalHandlingWorker(db.store, db.sessions, registry, local_id="ep", poll_seconds=60)
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(observed.wait(), 2)
            assert mutations == [work_message.command_id]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_fallback_scan_recovers_without_any_listener(work_message, monkeypatch):
    async with peer_pair(options=ExchangeOptions(poll_seconds=0.05), realtime=True) as pair:
        sent = asyncio.Event()
        send = pair.ao.service.transport.send

        async def record_send(message) -> TransportReceipt:
            receipt = await send(message)
            sent.set()
            return receipt

        monkeypatch.setattr(pair.ao.service.transport, "send", record_send)
        # Run delivery alone: no listener can relay PostgreSQL notifications.
        task = asyncio.create_task(pair.ao.service.delivery.run())
        try:
            await asyncio.sleep(0.1)
            async with pair.ao.sessions.begin() as session:
                await pair.ao.service.client.submit(work_message, session)
            await asyncio.wait_for(sent.wait(), 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_slow_handler_does_not_hold_up_free_handling_capacity(work_message):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.handlers import CommandRegistry, HandlingResult
    from execution_plane_client.workers.handling import JournalHandlingWorker

    from .exchange_helpers import journal_database

    async with journal_database() as db:
        second, third = another_command(work_message), another_command(work_message)
        blocked, third_handled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class Handler:
            async def handle(self, message, context, session) -> HandlingResult:
                if message.command_id == work_message.command_id:
                    blocked.set()
                    await release.wait()
                if message.command_id == third.command_id:
                    third_handled.set()
                return HandlingResult()

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute", 1), Handler())
        async with db.sessions.begin() as session:
            for message in (work_message, second, third):
                await db.store.append_or_confirm_inbound(session, message)
        worker = JournalHandlingWorker(db.store, db.sessions, registry, local_id="ep", poll_seconds=60, batch_size=2)
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(blocked.wait(), 2)
            await asyncio.wait_for(third_handled.wait(), 2)
            assert not release.is_set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_competing_delivery_workers_share_one_committed_claim(work_message, monkeypatch):
    from execution_plane_client.workers.delivery import JournalDeliveryWorker

    async with peer_pair(options=ExchangeOptions(poll_seconds=0.05), realtime=True) as pair:
        sending, release, sent = asyncio.Event(), asyncio.Event(), []
        send = pair.ao.service.transport.send

        async def record_send(message) -> TransportReceipt:
            sent.append(message.message_id)
            sending.set()
            await release.wait()
            return await send(message)

        monkeypatch.setattr(pair.ao.service.transport, "send", record_send)
        other = JournalDeliveryWorker(
            pair.ao.store,
            pair.ao.sessions,
            pair.ao.service.transport,
            pair.ao.service.delivery._registry,
            poll_seconds=0.05,
        )
        async with pair.ao.sessions.begin() as session:
            await pair.ao.service.client.submit(work_message, session)
        tasks = [asyncio.create_task(worker.run()) for worker in (pair.ao.service.delivery, other)]
        try:
            await asyncio.wait_for(sending.wait(), 2)
            await asyncio.sleep(0.1)
            assert sent == [work_message.message_id]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_slow_send_does_not_hold_up_free_delivery_capacity(work_message, monkeypatch):
    async with peer_pair(options=ExchangeOptions(poll_seconds=60, batch_size=2), realtime=True) as pair:
        second, third = another_command(work_message), another_command(work_message)
        blocked, third_sent, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        send = pair.ao.service.transport.send

        async def delayed_send(message) -> TransportReceipt:
            if message.command_id == work_message.command_id:
                blocked.set()
                await release.wait()
            receipt = await send(message)
            if message.command_id == third.command_id:
                third_sent.set()
            return receipt

        monkeypatch.setattr(pair.ao.service.transport, "send", delayed_send)
        async with pair.ao.sessions.begin() as session:
            for message in (work_message, second, third):
                await pair.ao.service.client.submit(message, session)
        task = asyncio.create_task(pair.ao.service.delivery.run())
        try:
            await asyncio.wait_for(blocked.wait(), 2)
            await asyncio.wait_for(third_sent.wait(), 2)
            assert not release.is_set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_delivery_retry_deadline_is_independent_of_fallback_poll(work_message, monkeypatch):
    from execution_plane_client.errors import RetryableDelivery

    options = ExchangeOptions(poll_seconds=60, retry_base_seconds=0.05)
    async with peer_pair(options=options, realtime=True) as pair:
        attempts, sent = [], asyncio.Event()
        send = pair.ao.service.transport.send

        async def uncertain_once(message) -> TransportReceipt:
            attempts.append(message.message_id)
            if len(attempts) == 1:
                raise RetryableDelivery
            receipt = await send(message)
            sent.set()
            return receipt

        monkeypatch.setattr(pair.ao.service.transport, "send", uncertain_once)
        async with pair.ao.sessions.begin() as session:
            await pair.ao.service.client.submit(work_message, session)
        task = asyncio.create_task(pair.ao.service.delivery.run())
        try:
            await asyncio.wait_for(sent.wait(), 2)
            assert attempts == [work_message.message_id, work_message.message_id]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("rollback", [False, True])
async def test_uncommitted_command_is_never_sent(work_message, rollback):
    async with peer_pair(options=ExchangeOptions(poll_seconds=60), realtime=True) as pair:
        tasks = [asyncio.create_task(peer.service.run()) for peer in (pair.ao, pair.ep)]
        try:
            await asyncio.sleep(0.2)
            async with pair.ao.sessions() as session:
                await pair.ao.service.client.submit(work_message, session)
                await asyncio.sleep(0.1)
                assert await pair.ep.store.get(session, work_message.scope, work_message.command_id, "inbound") is None
                if rollback:
                    await session.rollback()
                else:
                    await session.commit()
            if rollback:
                await asyncio.sleep(0.1)
                assert pair.effects == 0
                async with pair.ao.sessions() as read_session:
                    assert (
                        await pair.ao.store.get(read_session, work_message.scope, work_message.command_id, "outbound")
                        is None
                    )
            else:
                async with asyncio.timeout(3):
                    while not pair.results:
                        await asyncio.sleep(0.01)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
