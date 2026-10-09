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
async def test_latest_migration_removes_obsolete_transport_tables(migrated_database: str):
    from sqlalchemy import inspect
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(migrated_database)
    try:
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names(schema="execution_plane"))
        assert "exchange_journal" in tables
        assert "completion_events" not in tables
        assert "cluster_bindings" not in tables
    finally:
        await engine.dispose()
