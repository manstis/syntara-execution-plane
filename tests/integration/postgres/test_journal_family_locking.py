"""Family handling and duplicate receipt must share a noncyclic lock order."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import text

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.handlers import CommandRegistry
from execution_plane_client.ingress import CommandReceiptHandler, acceptance_for
from execution_plane_client.security import AuthenticatedIdentity

from .exchange_helpers import journal_database


async def test_duplicate_acceptance_and_family_handling_do_not_deadlock(work_message, monkeypatch):  # noqa: PLR0915 - Schedule and verify two real concurrent transactions.
    async with journal_database() as database:
        journal = database.store
        receipt = CommandReceiptHandler(
            journal, CommandCatalog(), CommandRegistry(), local_id="ao", local_role="controller"
        )
        acceptance = acceptance_for(work_message, datetime.now(UTC))
        identity = AuthenticatedIdentity("ep", work_message.scope, frozenset({"command.accepted"}), "executor")
        # UUID order differs from parent-first order, making the old cycle reproducible.
        with monkeypatch.context() as patch:
            patch.setattr("execution_plane_client.journal.store.uuid4", lambda: UUID(int=20))
            async with database.sessions.begin() as session:
                await journal.append_outbound(session, work_message)
            patch.setattr("execution_plane_client.journal.store.uuid4", lambda: UUID(int=10))
            async with database.sessions.begin() as session:
                await receipt.receive(acceptance, identity, session)

        parent_locked = asyncio.Event()
        release_receipt = asyncio.Event()
        acknowledge = journal.acknowledge

        async def pause_after_parent_lock(*args: object) -> object:
            result = await acknowledge(*args)
            parent_locked.set()
            await release_receipt.wait()
            return result

        monkeypatch.setattr(journal, "acknowledge", pause_after_parent_lock)
        family_pid = asyncio.get_running_loop().create_future()

        async def duplicate_receipt() -> None:
            async with database.sessions.begin() as session:
                await receipt.receive(acceptance, identity, session)

        async def handle_family() -> None:
            async with database.sessions.begin() as session:
                family_pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                await journal.lock_command_family(session, work_message.scope, work_message.command_id)

        receipt_task = asyncio.create_task(duplicate_receipt())
        family_task = None
        try:
            await asyncio.wait_for(parent_locked.wait(), timeout=5)
            family_task = asyncio.create_task(handle_family())
            pid = await family_pid
            # Observe the real database wait, instead of relying on task timing.
            async with asyncio.timeout(5), database.sessions() as observer:
                while not await observer.scalar(  # noqa: ASYNC110 - PostgreSQL lock waits have no asyncio event.
                    text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": pid}
                ):
                    await asyncio.sleep(0.01)
            release_receipt.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(receipt_task, family_task, return_exceptions=True), timeout=5
            )
            assert outcomes == [None, None], [str(outcome) for outcome in outcomes]
            async with database.sessions() as session:
                original = await journal.get(session, work_message.scope, work_message.command_id, "outbound")
                received = await journal.get(session, work_message.scope, acceptance.command_id, "inbound")
                assert original.delivery_state == "acknowledged"
                assert received.handling_state == "handled"
        finally:
            release_receipt.set()
            tasks = [receipt_task, *([family_task] if family_task is not None else [])]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
