"""Commands remain executable and observable with only the shared journal."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import MetaData, Table, update

from execution_plane.models.work_item import WorkItemStatus
from execution_plane_transport_journal.queries import JournalSessionCommandQueryProvider

from .command_helpers import resource_case, work_case


async def remove_command_tables(case):
    """Remove obsolete tables from this test's disposable schema only."""
    async with case.db.sessions.kw["bind"].begin() as connection:
        for name in ("command_records", "resource_revisions", "command_cancellation_intents"):
            table = Table(name, MetaData(), schema="execution_plane")
            await connection.run_sync(lambda conn, table=table: table.drop(conn, checkfirst=True))


async def test_resource_intent_and_status_need_only_the_journal():
    async with resource_case() as case:
        await remove_command_tables(case)
        key = uuid4()
        message = case.message(key=key)
        await case.run(message)
        async with case.db.sessions() as session:
            read = await JournalSessionCommandQueryProvider(case.db.journal).get(
                session, message.scope, message.command_id
            )
        assert read.status.value == "succeeded"
        assert read.resource.resource_id == (await case.clusters())[0].id
        obsolete = case.message(key=key, revision=1)
        await case.run(case.message(key=key, revision=2, name="new"))
        await case.run(obsolete)
        async with case.db.sessions() as session:
            read = await JournalSessionCommandQueryProvider(case.db.journal).get(
                session, obsolete.scope, obsolete.command_id
            )
        assert read.status.value == "superseded"


async def test_work_completion_needs_only_its_original_journal_entry(work_message):
    async with work_case(work_message) as case:
        await remove_command_tables(case)
        await case.store.set_result(case.work_id, {"value": 42}, WorkItemStatus.COMPLETED)
        async with case.db.sessions() as session:
            read = await JournalSessionCommandQueryProvider(case.db.journal).get(
                session, work_message.scope, work_message.command_id
            )
        assert read.status.value == "succeeded"
        assert read.resource.resource_id == case.work_id
        assert await case.counts() == 1


async def test_shared_worker_observes_completed_drain():
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore

    async with resource_case() as case:
        key = uuid4()
        created = await case.run(case.message(key=key))
        deletion = case.message("cluster.delete", key=key, revision=2)
        await case.run(deletion)
        async with case.db.sessions.begin() as session:
            for target in await case.targets(created.resource_id):
                await ExecutionTargetStore.from_transaction(session).finalize_delete(target.id)
            await ClusterStore.from_transaction(session).finalize_delete(created.resource_id)
            journal = case.db.journal.schema.table
            await session.execute(
                update(journal)
                .where(journal.c.command_id == deletion.command_id)
                .values(handling_due_at=datetime.now(UTC) - timedelta(seconds=1))
            )
        await case.worker.run_once()
        async with case.db.sessions() as session:
            read = await JournalSessionCommandQueryProvider(case.db.journal).get(
                session, deletion.scope, deletion.command_id
            )
        assert read.status.value == "succeeded"
