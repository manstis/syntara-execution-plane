"""Work outcomes remain durable with no legacy delivery or binding tables."""

import pytest
from sqlalchemy import MetaData, Table, func, select

from execution_plane.models.work_item import WorkItemStatus

from .command_helpers import work_case


async def test_work_outcomes_do_not_require_legacy_transport_tables(work_message):
    async with work_case(work_message) as case:
        async with case.db.sessions.kw["bind"].begin() as connection:
            for name in ("completion_events", "cluster_bindings"):
                table = Table(name, MetaData(), schema="execution_plane")
                await connection.run_sync(lambda conn, table=table: table.drop(conn, checkfirst=True))
        await case.store.mark_reconciliation_required(case.work_id, "lost provider response")
        await case.store.mark_reconciliation_required(case.work_id, "lost provider response")
        await case.store.set_result(case.work_id, {"value": 42}, WorkItemStatus.COMPLETED)
        await case.store.set_result(case.work_id, {"value": 42}, WorkItemStatus.COMPLETED)
        assert (await case.reload()).result == {"value": 42}
        async with case.db.sessions() as session:
            journal = case.db.journal.schema.table
            assert (
                await session.scalar(
                    select(func.count()).select_from(journal).where(journal.c.command_type == "work.execute.completed")
                )
                == 2
            )
            children = await session.scalars(
                select(journal.c.entry_id)
                .where(journal.c.command_type == "work.execute.completed")
                .order_by(journal.c.parent_sequence)
            )
            messages = [await case.db.journal.load_message(session, entry_id) for entry_id in children]
        assert [message.command.payload["status"] for message in messages] == ["outcome_unknown", "succeeded"]


@pytest.mark.integration
async def test_latest_migration_removes_obsolete_transport_tables():
    import os

    from sqlalchemy import inspect
    from sqlalchemy.ext.asyncio import create_async_engine

    url = os.environ.get("EP_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("EP_MIGRATION_DATABASE_URL must identify a disposable migrated database")
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names(schema="execution_plane"))
        assert "exchange_journal" in tables
        assert "completion_events" not in tables
        assert "cluster_bindings" not in tables
    finally:
        await engine.dispose()


async def test_cleanup_keeps_journal_owned_outcomes_and_rejects_unjournaled_ones(work_message):  # noqa: PLR0915 - Exercise one historical cutover transaction.
    import importlib
    import os
    from datetime import UTC, datetime
    from uuid import uuid4

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from execution_plane.models.work_item import WorkItem
    from execution_plane_client.completion import append_completion
    from execution_plane_client.crypto import AESGCMCipher
    from execution_plane_client.handlers import HandlingResult
    from execution_plane_client.journal.schema import JournalSchema
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import CommandStatus, ResourceReference

    url = os.environ.get("EP_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("EP_MIGRATION_DATABASE_URL must identify a disposable migrated database")
    migration = importlib.import_module(
        "execution_plane.migrations.versions.412de0fed627_retire_legacy_exchange_tables"
    )
    cutover = importlib.import_module(
        "execution_plane.migrations.versions.e0d8772ed6bb_make_command_journal_authoritative"
    )
    now = datetime.now(UTC)
    item = WorkItem(
        client_id=work_message.scope.client_id,
        project_id=work_message.scope.project_id,
        request_id=str(work_message.command_id),
        request_hash="test",
        work_correlation_id=uuid4(),
        status=WorkItemStatus.COMPLETED,
        payload={},
        result={"value": 42},
        created_at=now,
        completed_at=now,
    )

    def migrate(connection, direction, revision=migration) -> None:
        with Operations.context(MigrationContext.configure(connection)):
            getattr(revision, direction)()

    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.run_sync(lambda conn: migrate(conn, "downgrade", cutover))
                await connection.run_sync(lambda conn: migrate(conn, "downgrade"))
                metadata = MetaData()
                legacy_table = await connection.run_sync(
                    lambda conn: Table("exchange_journal", metadata, schema="execution_plane", autoload_with=conn)
                )
                journal = ExchangeJournalStore(
                    JournalSchema(metadata, legacy_table, 2),
                    AESGCMCipher(active_key_id="test", keys={"test": b"t" * 32}),
                )
                async with AsyncSession(bind=connection, expire_on_commit=False) as session:
                    session.add(item)
                    await session.flush()
                    await journal.append_or_confirm_inbound(session, work_message)
                    # The historical cleanup legitimately depended on this now-retired
                    # table. Seed its historical schema without reviving the ORM model.
                    await session.execute(
                        text("""
                        INSERT INTO execution_plane.command_records
                        (client_id, command_id, project_id, all_projects, source_id, command_type,
                         status, resource_type, resource_id, child_sequence, created_at, updated_at)
                        VALUES (:client, :command, :project, false, 'ao', 'work.execute',
                                'succeeded', 'work_item', :work, 0, :now, :now)
                        """),
                        {
                            "client": item.client_id,
                            "command": work_message.command_id,
                            "project": item.project_id,
                            "work": item.id,
                            "now": now,
                        },
                    )
                    await session.execute(
                        text("""
                        INSERT INTO execution_plane.completion_events
                        (id, work_item_id, client_id, project_id, request_id, state_revision, status, result,
                         created_at, next_attempt_at)
                        VALUES (:id, :work, :client, :project, :request, 1, 'completed',
                                CAST(:result AS jsonb), :now, :now)
                    """),
                        {
                            "id": uuid4(),
                            "work": item.id,
                            "client": item.client_id,
                            "project": item.project_id,
                            "request": item.request_id,
                            "result": '{"value": 42}',
                            "now": now,
                        },
                    )
                    with pytest.raises(RuntimeError, match="1 unjournaled outcomes"):
                        await connection.run_sync(lambda conn: migrate(conn, "upgrade"))
                    child = await append_completion(
                        journal,
                        session,
                        work_message,
                        HandlingResult(
                            status=CommandStatus.SUCCEEDED,
                            resource=ResourceReference(kind="work_item", resource_id=item.id),
                            result={"value": 42},
                        ),
                        occurred_at=now,
                    )
                    await connection.run_sync(lambda conn: migrate(conn, "upgrade"))
                    assert (await session.get(WorkItem, item.id)).result == {"value": 42}
                    entry = await journal.get(session, work_message.scope, child.command_id, "outbound")
                    assert await journal.load_message(session, entry.entry_id) == child
            finally:
                if transaction.is_active:
                    await transaction.rollback()
    finally:
        await engine.dispose()
